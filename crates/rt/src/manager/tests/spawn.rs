//! Tests related to interaction among workflows.

use futures::future;
use wasmtime::ExternRef;

use super::*;
use crate::{
    data::{tests::complete_task_with_error, SpawnFunctions},
    receipt::{ChannelEventKind, ResourceEvent, ResourceEventKind, ResourceId},
    storage::{LocalTransaction, Readonly},
    utils::{copy_string_from_wasm, decode_string},
};
use tardigrade::{abi::TryFromWasm, interface::ChannelKind};

const CHILD_ID: WorkflowId = 1;
const ERROR_PTR: u32 = 1_024;

async fn peek_channel(
    transaction: &Readonly<LocalTransaction<'_>>,
    channel_id: ChannelId,
) -> Vec<Vec<u8>> {
    let channel = transaction.channel(channel_id).await.unwrap();
    let len = channel.received_messages;
    let messages = (0..len).map(|idx| transaction.channel_message(channel_id, idx));
    future::try_join_all(messages).await.unwrap()
}

fn configure_handle(
    mut ctx: StoreContextMut<'_, WorkflowData>,
    handles: Option<ExternRef>,
    channel_kind: ChannelKind,
    name: &str,
) -> Result<(), Trap> {
    let (name_ptr, name_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(name.as_bytes())?;
    SpawnFunctions::set_channel_handle(
        ctx,
        handles,
        channel_kind.into_abi_in_wasm(),
        name_ptr,
        name_len,
        0,
    )
}

fn configure_handles(
    mut ctx: StoreContextMut<'_, WorkflowData>,
    handles: Option<ExternRef>,
) -> Result<(), Trap> {
    configure_handle(
        ctx.as_context_mut(),
        handles.clone(),
        ChannelKind::Inbound,
        "orders",
    )?;
    configure_handle(
        ctx.as_context_mut(),
        handles.clone(),
        ChannelKind::Outbound,
        "events",
    )?;
    configure_handle(
        ctx.as_context_mut(),
        handles,
        ChannelKind::Outbound,
        "traces",
    )?;
    Ok(())
}

fn spawn_child(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let (id_ptr, id_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(DEFINITION_ID.as_bytes())?;
    let (args_ptr, args_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"child_input")?;
    let handles = SpawnFunctions::create_channel_handles();
    configure_handles(ctx.as_context_mut(), handles.clone())?;
    let stub = SpawnFunctions::spawn(
        ctx.as_context_mut(),
        id_ptr,
        id_len,
        args_ptr,
        args_len,
        handles,
    )?;
    assert!(stub.is_some());

    let workflow =
        SpawnFunctions::poll_workflow_init(ctx.as_context_mut(), stub, POLL_CX, ERROR_PTR)?;
    assert!(workflow.is_none());
    Ok(Poll::Pending)
}

fn poll_child_traces(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let workflow = Some(WorkflowData::child_ref(CHILD_ID));

    // Block on the inbound channel from the child.
    let (traces_ptr, traces_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"traces")?;
    let child_traces = WorkflowFunctions::get_receiver(
        ctx.as_context_mut(),
        workflow,
        traces_ptr,
        traces_len,
        ERROR_PTR,
    )?;
    assert!(child_traces.is_some());

    let poll_result =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), child_traces, POLL_CX)?;
    assert_eq!(poll_result, -1); // Poll::Pending

    Ok(Poll::Pending)
}

#[async_std::test]
async fn spawning_child_workflow() {
    let receive_child_trace: MockPollFn = |mut ctx| {
        let child_traces = Some(WorkflowData::inbound_channel_ref(Some(CHILD_ID), "traces"));
        let poll_result =
            WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), child_traces, POLL_CX)?;
        assert_ne!(poll_result, -1); // Poll::Ready(Some(_))

        Ok(Poll::Pending)
    };

    let poll_fns = Answers::from_values([spawn_child, poll_child_traces, receive_child_trace]);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    tick_workflow(&manager, workflow_id).await.unwrap();
    workflow.update().await.unwrap();

    let persisted = workflow.persisted();
    let wakeup_causes: Vec<_> = persisted.pending_wakeup_causes().collect();
    assert_matches!(
        wakeup_causes.as_slice(),
        [WakeUpCause::InitWorkflow { stub_id: 0 }]
    );
    let mut children: Vec<_> = persisted.child_workflows().collect();
    assert_eq!(children.len(), 1);
    let (child_id, child_state) = children.pop().unwrap();
    assert_eq!(child_id, CHILD_ID);
    let traces_id = child_state.inbound_channel("traces").unwrap().id();

    tick_workflow(&manager, workflow_id).await.unwrap();
    workflow.update().await.unwrap();
    let persisted = workflow.persisted();
    let wakeup_causes: Vec<_> = persisted.pending_wakeup_causes().collect();
    assert!(wakeup_causes.is_empty(), "{wakeup_causes:?}");

    {
        let transaction = manager.storage.readonly_transaction().await;
        assert_eq!(transaction.count_workflows().await, 2);
        assert!(transaction.workflow(child_id).await.is_some());

        let traces = transaction.channel(traces_id).await.unwrap();
        assert_eq!(traces.receiver_workflow_id, Some(workflow_id));
        assert_eq!(traces.sender_workflow_ids, HashSet::from_iter([child_id]));
        assert!(!traces.has_external_sender);

        let pending_workflow = transaction.find_pending_workflow().await.unwrap();
        assert_eq!(pending_workflow.id, child_id);
    }

    // Emulate the child workflow putting a message to the `traces` channel.
    manager
        .send_message(traces_id, b"trace".to_vec())
        .await
        .unwrap();
    assert_eq!(
        find_consumable_channel(&manager).await,
        Some((traces_id, 0, workflow_id))
    );

    let receipt = feed_message(&manager, traces_id, workflow_id)
        .await
        .unwrap();
    let events = extract_channel_events(&receipt, Some(child_id), "traces");
    assert_matches!(
        events.as_slice(),
        [ChannelEventKind::InboundChannelPolled {
            result: Poll::Ready(Some(5)),
        }]
    );

    // Check channel handles for child workflow
    let mut handle = manager.workflow(child_id).await.unwrap().handle();
    let mut child_orders = handle.remove(InboundChannel("orders")).unwrap();
    let child_orders_id = child_orders.channel_id();
    child_orders.send(b"test".to_vec()).await.unwrap();
    child_orders.close().await; // no-op: a channel sender is not owned by the host

    let child_events = handle.remove(OutboundChannel("events")).unwrap();
    let child_events_id = child_events.channel_id();
    assert!(!child_events.can_manipulate());
    child_events.close().await; // no-op: the channel receiver is not owned by the host

    let transaction = manager.storage.readonly_transaction().await;
    let child_orders = transaction.channel(child_orders_id).await.unwrap();
    assert!(!child_orders.is_closed);
    let child_events = transaction.channel(child_events_id).await.unwrap();
    assert!(!child_events.is_closed);
}

#[async_std::test]
async fn sending_message_to_child() {
    let send_message_to_child: MockPollFn = |mut ctx| {
        let child_orders = Some(WorkflowData::outbound_channel_ref(Some(CHILD_ID), "orders"));
        let poll_result = WorkflowFunctions::poll_ready_for_sender(
            ctx.as_context_mut(),
            child_orders.clone(),
            POLL_CX,
        )?;
        assert_eq!(poll_result, 0); // Poll::Ready(Ok(())

        let (message_ptr, message_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"child_order")?;
        let send_result = WorkflowFunctions::start_send(
            ctx.as_context_mut(),
            child_orders.clone(),
            message_ptr,
            message_len,
        )?;
        assert_eq!(send_result, 0); // Ok(())

        let poll_result =
            WorkflowFunctions::poll_flush_for_sender(ctx.as_context_mut(), child_orders, POLL_CX)?;
        assert_eq!(poll_result, -1); // Poll::Pending

        Ok(Poll::Pending)
    };
    let init_child: MockPollFn = |mut ctx| {
        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        let poll_result =
            WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
        assert_eq!(poll_result, -1); // Poll::Pending
        Ok(Poll::Pending)
    };
    let poll_orders_in_child: MockPollFn = |mut ctx| {
        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        let poll_result =
            WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
        assert_ne!(poll_result, -1); // Poll::Ready(Some(_))
        Ok(Poll::Pending)
    };

    let poll_fns = Answers::from_values([
        spawn_child,
        send_message_to_child,
        init_child,
        poll_orders_in_child,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let workflow_id = create_test_workflow(&manager).await.id();
    tick_workflow(&manager, workflow_id).await.unwrap(); // initializes workflow
    tick_workflow(&manager, workflow_id).await.unwrap(); // notifies about child initialization

    let child = manager.workflow(CHILD_ID).await.unwrap();
    let child_channel_ids = child.persisted().channel_ids();
    let child_orders_id = child_channel_ids.inbound["orders"];
    {
        let transaction = manager.storage.readonly_transaction().await;
        let child_orders = peek_channel(&transaction, child_orders_id).await;
        assert_eq!(child_orders.len(), 1);
        assert_eq!(child_orders[0], b"child_order");
    }

    tick_workflow(&manager, CHILD_ID).await.unwrap();
    assert_eq!(
        find_consumable_channel(&manager).await,
        Some((child_orders_id, 0, CHILD_ID))
    );
    feed_message(&manager, child_orders_id, CHILD_ID)
        .await
        .unwrap();
}

fn test_child_channels_after_closure(
    mut ctx: StoreContextMut<WorkflowData>,
) -> Result<Poll<()>, Trap> {
    let child_orders = Some(WorkflowData::outbound_channel_ref(Some(CHILD_ID), "orders"));
    let poll_result =
        WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), child_orders, POLL_CX)?;
    assert_eq!(poll_result, 2); // Err(Closed)

    let child_traces = Some(WorkflowData::inbound_channel_ref(Some(CHILD_ID), "traces"));
    let poll_result =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), child_traces, POLL_CX)?;
    assert_eq!(poll_result, -2); // Poll::Ready(None)

    Ok(Poll::Pending)
}

async fn test_child_workflow_channel_management(complete_child: bool) {
    let poll_child_completion: MockPollFn = |mut ctx| {
        let _ = poll_child_traces(ctx.as_context_mut())?;
        let child = Some(WorkflowData::child_ref(CHILD_ID));
        let poll_result =
            SpawnFunctions::poll_workflow_completion(ctx.as_context_mut(), child, POLL_CX)?;
        assert_eq!(poll_result, -1); // Poll::Pending

        Ok(Poll::Pending)
    };
    let poll_child_workflow: MockPollFn = if complete_child {
        |_| Ok(Poll::Ready(()))
    } else {
        |mut ctx| {
            let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
            WorkflowFunctions::drop_ref(ctx.as_context_mut(), orders)?;
            let traces = Some(WorkflowData::outbound_channel_ref(None, "traces"));
            WorkflowFunctions::drop_ref(ctx.as_context_mut(), traces)?;

            Ok(Poll::Pending)
        }
    };
    let test_child_resources: MockPollFn = if complete_child {
        |mut ctx| {
            let _ = test_child_channels_after_closure(ctx.as_context_mut())?;
            let child = Some(WorkflowData::child_ref(CHILD_ID));
            let poll_result =
                SpawnFunctions::poll_workflow_completion(ctx.as_context_mut(), child, POLL_CX)?;
            assert_eq!(poll_result, -2); // Poll::Ready(Ok(()))

            Ok(Poll::Pending)
        }
    } else {
        test_child_channels_after_closure
    };

    let poll_fns = Answers::from_values([
        spawn_child,
        poll_child_completion,
        poll_child_workflow,
        test_child_resources,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    tick_workflow(&manager, workflow_id).await.unwrap(); // initializes workflow
    tick_workflow(&manager, workflow_id).await.unwrap(); // notifies about child initialization

    workflow.update().await.unwrap();
    let mut children: Vec<_> = workflow.persisted().child_workflows().collect();
    assert_eq!(children.len(), 1);
    let (child_id, child_state) = children.pop().unwrap();
    assert_eq!(child_id, CHILD_ID);
    let orders_id = child_state.outbound_channel("orders").unwrap().id();
    let traces_id = child_state.inbound_channel("traces").unwrap().id();

    tick_workflow(&manager, child_id).await.unwrap();
    let channel_info = manager.channel(orders_id).await.unwrap();
    assert!(channel_info.is_closed);
    let channel_info = manager.channel(traces_id).await.unwrap();
    assert!(channel_info.is_closed);

    workflow.update().await.unwrap();
    let child_state = workflow.persisted().child_workflow(child_id).unwrap();
    if complete_child {
        assert_matches!(child_state.result(), Poll::Ready(Ok(())));
    } else {
        assert_matches!(child_state.result(), Poll::Pending);
    }

    let events: Vec<_> = workflow.persisted().pending_wakeup_causes().collect();
    assert_matches!(
        events[0],
        WakeUpCause::ChannelClosed { workflow_id: Some(CHILD_ID), channel_name }
            if channel_name == "traces"
    );
    if complete_child {
        assert_eq!(events.len(), 2);
        assert_matches!(events[1], WakeUpCause::CompletedWorkflow(CHILD_ID));
    } else {
        assert_eq!(events.len(), 1);
    }

    let receipt = tick_workflow(&manager, workflow_id).await.unwrap();
    if complete_child {
        let child_completed = receipt.events().any(|event| {
            matches!(
                event.as_resource_event(),
                Some(ResourceEvent {
                    resource_id: ResourceId::Workflow(CHILD_ID),
                    kind: ResourceEventKind::Polled(Poll::Ready(())),
                })
            )
        });
        assert!(child_completed);
    }
}

#[async_std::test]
async fn child_workflow_completion() {
    test_child_workflow_channel_management(true).await;
}

#[async_std::test]
async fn child_workflow_channel_closure() {
    test_child_workflow_channel_management(false).await;
}

fn spawn_child_with_copied_outbound_channel(
    mut ctx: StoreContextMut<'_, WorkflowData>,
    copy_traces: bool,
) -> Result<Poll<()>, Trap> {
    let (id_ptr, id_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(DEFINITION_ID.as_bytes())?;
    let (args_ptr, args_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"child_input")?;

    let handles = SpawnFunctions::create_channel_handles();
    configure_handles(ctx.as_context_mut(), handles.clone())?;
    let events = Some(WorkflowData::outbound_channel_ref(None, "events"));
    let (name_ptr, name_len) = WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"events")?;
    SpawnFunctions::copy_sender_handle(
        ctx.as_context_mut(),
        handles.clone(),
        name_ptr,
        name_len,
        events.clone(),
    )?;
    if copy_traces {
        let (name_ptr, name_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"traces")?;
        SpawnFunctions::copy_sender_handle(
            ctx.as_context_mut(),
            handles.clone(),
            name_ptr,
            name_len,
            events,
        )?;
    }

    let stub = SpawnFunctions::spawn(
        ctx.as_context_mut(),
        id_ptr,
        id_len,
        args_ptr,
        args_len,
        handles,
    )?;
    assert!(stub.is_some());
    SpawnFunctions::poll_workflow_init(ctx.as_context_mut(), stub, POLL_CX, ERROR_PTR)?;

    Ok(Poll::Pending)
}

fn poll_child_completion_with_copied_channel(
    mut ctx: StoreContextMut<'_, WorkflowData>,
) -> Result<Poll<()>, Trap> {
    let workflow = Some(WorkflowData::child_ref(CHILD_ID));
    let (name_ptr, name_len) = WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"events")?;
    let child_events = WorkflowFunctions::get_receiver(
        ctx.as_context_mut(),
        workflow.clone(),
        name_ptr,
        name_len,
        ERROR_PTR,
    )?;
    assert!(child_events.is_none()); // The handle to child events is not captured

    // Block on child completion
    SpawnFunctions::poll_workflow_completion(ctx.as_context_mut(), workflow, POLL_CX)?;
    Ok(Poll::Pending)
}

#[async_std::test]
async fn spawning_child_with_copied_outbound_channel() {
    let init: MockPollFn = |ctx| spawn_child_with_copied_outbound_channel(ctx, false);
    let write_event_and_complete_child: MockPollFn = |mut ctx| {
        let events = Some(WorkflowData::outbound_channel_ref(None, "events"));
        let (message_ptr, message_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"child_event")?;
        WorkflowFunctions::start_send(ctx.as_context_mut(), events, message_ptr, message_len)?;
        Ok(Poll::Ready(()))
    };

    let poll_fns = Answers::from_values([
        init,
        poll_child_completion_with_copied_channel,
        write_event_and_complete_child,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let events_id = workflow.ids().channel_ids.outbound["events"];
    tick_workflow(&manager, workflow_id).await.unwrap(); // initializes workflow
    tick_workflow(&manager, workflow_id).await.unwrap(); // notifies about child initialization

    let events_channel = manager.channel(events_id).await.unwrap();
    assert_eq!(
        events_channel.sender_workflow_ids,
        HashSet::from_iter([workflow_id, CHILD_ID])
    );
    let child = manager.workflow(CHILD_ID).await.unwrap();
    let child_events_id = child
        .persisted()
        .outbound_channels()
        .find_map(|(_, name, state)| {
            if name == "events" {
                Some(state.id())
            } else {
                None
            }
        });
    assert_eq!(child_events_id, Some(events_id));

    tick_workflow(&manager, CHILD_ID).await.unwrap();
    let events_channel = manager.channel(events_id).await.unwrap();
    assert_eq!(
        events_channel.sender_workflow_ids,
        HashSet::from_iter([workflow_id])
    );
    assert_eq!(events_channel.receiver_workflow_id, None);

    let transaction = manager.storage.readonly_transaction().await;
    let events = peek_channel(&transaction, events_id).await;
    assert_eq!(events, [b"child_event"]);
}

async fn test_child_with_copied_closed_outbound_channel(close_before_spawn: bool) {
    let init: MockPollFn = |ctx| spawn_child_with_copied_outbound_channel(ctx, false);
    let test_writing_event_in_child: MockPollFn = |mut ctx| {
        let events = Some(WorkflowData::outbound_channel_ref(None, "events"));
        let (message_ptr, message_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"child_event")?;
        let send_result =
            WorkflowFunctions::start_send(ctx.as_context_mut(), events, message_ptr, message_len)?;
        assert_eq!(send_result, 2); // Err(SendError::Closed)

        Ok(Poll::Pending)
    };

    let poll_fns = Answers::from_values([
        init,
        poll_child_completion_with_copied_channel,
        test_writing_event_in_child,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let events_id = workflow.ids().channel_ids.outbound["events"];

    if close_before_spawn {
        manager.close_host_receiver(events_id).await;
    }
    tick_workflow(&manager, workflow_id).await.unwrap(); // initializes workflow
    tick_workflow(&manager, workflow_id).await.unwrap(); // notifies about child initialization

    if !close_before_spawn {
        manager.close_host_receiver(events_id).await;
    }
    tick_workflow(&manager, CHILD_ID).await.unwrap();
}

#[async_std::test]
async fn spawning_child_with_copied_closed_outbound_channel() {
    test_child_with_copied_closed_outbound_channel(true).await;
}

#[async_std::test]
async fn spawning_child_with_copied_then_closed_outbound_channel() {
    test_child_with_copied_closed_outbound_channel(false).await;
}

async fn test_child_with_aliased_outbound_channel(complete_child: bool) {
    let init: MockPollFn = |ctx| spawn_child_with_copied_outbound_channel(ctx, true);
    let write_event_and_drop_events: MockPollFn = |mut ctx| {
        let events = Some(WorkflowData::outbound_channel_ref(None, "events"));
        let (message_ptr, message_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"child_event")?;
        WorkflowFunctions::start_send(
            ctx.as_context_mut(),
            events.clone(),
            message_ptr,
            message_len,
        )?;
        WorkflowFunctions::drop_ref(ctx.as_context_mut(), events)?;

        let traces = Some(WorkflowData::outbound_channel_ref(None, "traces"));
        let (message_ptr, message_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"child_trace")?;
        WorkflowFunctions::start_send(
            ctx.as_context_mut(),
            traces.clone(),
            message_ptr,
            message_len,
        )?;
        let poll_result =
            WorkflowFunctions::poll_flush_for_sender(ctx.as_context_mut(), traces, POLL_CX)?;
        assert_eq!(poll_result, -1); // Poll::Pending
        Ok(Poll::Pending)
    };
    let complete_child_or_drop_traces: MockPollFn = if complete_child {
        |_| Ok(Poll::Ready(()))
    } else {
        |ctx| {
            let traces = Some(WorkflowData::outbound_channel_ref(None, "traces"));
            WorkflowFunctions::drop_ref(ctx, traces)?;
            Ok(Poll::Pending)
        }
    };

    let poll_fns = Answers::from_values([
        init,
        poll_child_completion_with_copied_channel,
        write_event_and_drop_events,
        complete_child_or_drop_traces,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let events_id = workflow.ids().channel_ids.outbound["events"];
    tick_workflow(&manager, workflow_id).await.unwrap(); // initializes workflow
    tick_workflow(&manager, workflow_id).await.unwrap(); // notifies about child initialization

    let events_channel = manager.channel(events_id).await.unwrap();
    assert_eq!(
        events_channel.sender_workflow_ids,
        HashSet::from_iter([workflow_id, CHILD_ID])
    );
    let child = manager.workflow(CHILD_ID).await.unwrap();
    let child_channel_ids = &child.ids().channel_ids;
    assert_eq!(child_channel_ids.outbound["events"], events_id);
    assert_eq!(child_channel_ids.outbound["traces"], events_id);

    tick_workflow(&manager, CHILD_ID).await.unwrap();
    let events_channel = manager.channel(events_id).await.unwrap();
    assert_eq!(
        events_channel.sender_workflow_ids,
        HashSet::from_iter([workflow_id, CHILD_ID])
    );
    let transaction = manager.storage.readonly_transaction().await;
    let outbound_messages: Vec<_> = peek_channel(&transaction, events_id).await;
    assert_eq!(outbound_messages, [b"child_event" as &[u8], b"child_trace"]);
    drop(transaction);

    tick_workflow(&manager, CHILD_ID).await.unwrap();
    let events_channel = manager.channel(events_id).await.unwrap();
    assert_eq!(
        events_channel.sender_workflow_ids,
        HashSet::from_iter([workflow_id])
    );
}

#[async_std::test]
async fn spawning_child_with_aliased_outbound_channel() {
    test_child_with_aliased_outbound_channel(false).await;
}

#[async_std::test]
async fn spawning_and_completing_child_with_aliased_outbound_channel() {
    test_child_with_aliased_outbound_channel(true).await;
}

fn poll_child_completion(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let workflow = Some(WorkflowData::child_ref(CHILD_ID));
    let poll_result =
        SpawnFunctions::poll_workflow_completion(ctx.as_context_mut(), workflow, POLL_CX)?;
    assert_eq!(poll_result, -1); // Poll::Pending

    Ok(Poll::Pending)
}

#[async_std::test]
async fn completing_child_with_error() {
    let check_child_completion: MockPollFn = |mut ctx| {
        let child = Some(WorkflowData::child_ref(CHILD_ID));
        let poll_result =
            SpawnFunctions::poll_workflow_completion(ctx.as_context_mut(), child.clone(), POLL_CX)?;
        assert_eq!(poll_result, -2); // Poll::Ready(Ok(()))

        let err = SpawnFunctions::completion_error(ctx.as_context_mut(), child)?;
        assert_ne!(err, 0);
        let (err_ptr, err_len) = decode_string(err);
        let memory = ctx.data().exports().memory;
        let err_json = copy_string_from_wasm(&ctx, &memory, err_ptr, err_len)?;
        assert!(err_json.starts_with('{') && err_json.ends_with('}'));

        Ok(Poll::Pending)
    };

    let poll_fns = Answers::from_values([
        spawn_child,
        poll_child_completion,
        complete_task_with_error,
        check_child_completion,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let workflow_id = create_test_workflow(&manager).await.id();
    tick_workflow(&manager, workflow_id).await.unwrap(); // initializes workflow
    tick_workflow(&manager, workflow_id).await.unwrap(); // notifies about child initialization
    tick_workflow(&manager, CHILD_ID).await.unwrap();

    let receipt = tick_workflow(&manager, workflow_id).await.unwrap();
    let is_child_polled = receipt.events().any(|event| {
        event.as_resource_event().map_or(false, |event| {
            matches!(event.resource_id, ResourceId::Workflow(CHILD_ID))
                && matches!(event.kind, ResourceEventKind::Polled(Poll::Ready(())))
        })
    });
    assert!(is_child_polled, "{receipt:?}");
}

fn complete_task_with_panic(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let (message_ptr, message_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"panic message")?;
    let (filename_ptr, filename_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"/build/src/test.rs")?;

    let events = Some(WorkflowData::outbound_channel_ref(None, "events"));
    let send_result =
        WorkflowFunctions::start_send(ctx.as_context_mut(), events, message_ptr, message_len)?;
    assert_eq!(send_result, 0); // Ok(())

    WorkflowFunctions::report_panic(
        ctx,
        message_ptr,
        message_len,
        filename_ptr,
        filename_len,
        42, // line
        1,  // column
    )?;
    Err(Trap::new("oops"))
}

fn check_aborted_child_completion(
    mut ctx: StoreContextMut<'_, WorkflowData>,
) -> Result<Poll<()>, Trap> {
    let child = Some(WorkflowData::child_ref(CHILD_ID));
    let poll_result =
        SpawnFunctions::poll_workflow_completion(ctx.as_context_mut(), child, POLL_CX)?;
    assert_eq!(poll_result, -3); // Poll::Ready(Err(Aborted))

    let child_events = Some(WorkflowData::inbound_channel_ref(Some(CHILD_ID), "events"));
    let poll_result =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), child_events, POLL_CX)?;
    assert_eq!(poll_result, -2); // Poll::Ready(None)

    Ok(Poll::Pending)
}

#[async_std::test]
async fn completing_child_with_panic() {
    let poll_fns = Answers::from_values([
        spawn_child,
        poll_child_completion,
        complete_task_with_panic,
        check_aborted_child_completion,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let workflow_id = create_test_workflow(&manager).await.id();

    let tick_result = manager.tick().await.unwrap();
    assert_eq!(tick_result.workflow_id(), workflow_id);
    tick_result.into_inner().unwrap();
    let child = manager.workflow(CHILD_ID).await.unwrap();
    let child_events_id = child.ids().channel_ids.outbound["events"];

    // Both workflow are ready to be polled at this point; control which one is polled first.
    tick_workflow(&manager, workflow_id).await.unwrap();

    let tick_result = manager.tick().await.unwrap();
    assert_eq!(tick_result.workflow_id(), CHILD_ID);
    let err = tick_result.abort_workflow().await.into_inner().unwrap_err();
    assert_eq!(
        err.panic_info().unwrap().message.as_deref(),
        Some("panic message")
    );
    assert_child_abort(&manager, workflow_id, child_events_id).await;
}

async fn assert_child_abort(
    manager: &WorkflowManager<(), LocalStorage>,
    workflow_id: WorkflowId,
    child_events_id: ChannelId,
) {
    // Check that the event emitted by the panicking workflow was not committed.
    let child_events = manager.channel(child_events_id).await.unwrap();
    assert_eq!(child_events.received_messages, 0);
    assert!(child_events.is_closed);

    let receipt = tick_workflow(manager, workflow_id).await.unwrap();
    let is_child_polled = receipt.events().any(|event| {
        event.as_resource_event().map_or(false, |event| {
            matches!(event.resource_id, ResourceId::Workflow(CHILD_ID))
                && matches!(event.kind, ResourceEventKind::Polled(Poll::Ready(())))
        })
    });
    assert!(is_child_polled, "{receipt:?}");
}

async fn test_aborting_child(initialize_child: bool) {
    let poll_fns: Vec<MockPollFn> = if initialize_child {
        vec![
            spawn_child,
            poll_child_completion,
            |_| Ok(Poll::Pending),
            check_aborted_child_completion,
        ]
    } else {
        vec![
            spawn_child,
            poll_child_completion,
            check_aborted_child_completion,
        ]
    };

    let poll_fns = Answers::from_values(poll_fns);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let workflow_id = create_test_workflow(&manager).await.id();
    tick_workflow(&manager, workflow_id).await.unwrap();
    tick_workflow(&manager, workflow_id).await.unwrap();

    let child = manager.workflow(CHILD_ID).await.unwrap();
    let child_events_id = child.ids().channel_ids.outbound["events"];
    if initialize_child {
        tick_workflow(&manager, CHILD_ID).await.unwrap();
    }
    manager.abort_workflow(CHILD_ID).await;

    assert_child_abort(&manager, workflow_id, child_events_id).await;
}

#[async_std::test]
async fn aborting_child_before_initialization() {
    test_aborting_child(false).await;
}

#[async_std::test]
async fn aborting_child_after_initialization() {
    test_aborting_child(true).await;
}

#[async_std::test]
async fn aborting_parent() {
    let check_child_channels: MockPollFn = |mut ctx| {
        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        let poll_result =
            WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
        assert_eq!(poll_result, -2); // Poll::Ready(None)

        let events = Some(WorkflowData::outbound_channel_ref(None, "events"));
        let (message_ptr, message_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"child event")?;
        let send_result =
            WorkflowFunctions::start_send(ctx.as_context_mut(), events, message_ptr, message_len)?;
        assert_eq!(send_result, 2); // Err(SendError::Closed)

        Ok(Poll::Ready(()))
    };

    let poll_fns = Answers::from_values([spawn_child, poll_child_completion, check_child_channels]);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let workflow_id = create_test_workflow(&manager).await.id();

    tick_workflow(&manager, workflow_id).await.unwrap();
    tick_workflow(&manager, workflow_id).await.unwrap();
    let child = manager.workflow(CHILD_ID).await.unwrap();
    let child_events_id = child.ids().channel_ids.outbound["events"];
    let child_orders_id = child.ids().channel_ids.inbound["orders"];
    manager.abort_workflow(workflow_id).await;
    let child_events = manager.channel(child_events_id).await.unwrap();
    assert!(child_events.is_closed);
    let child_orders = manager.channel(child_orders_id).await.unwrap();
    assert!(child_orders.is_closed);

    tick_workflow(&manager, CHILD_ID).await.unwrap();
    assert!(manager.workflow(CHILD_ID).await.is_none());
}
