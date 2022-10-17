//! Tests related to interaction among workflows.

use wasmtime::ExternRef;

use super::*;
use crate::{
    data::{tests::complete_task_with_error, SpawnFunctions},
    receipt::{ResourceEvent, ResourceEventKind, ResourceId},
    utils::{copy_string_from_wasm, decode_string},
};
use tardigrade_shared::{abi::TryFromWasm, interface::ChannelKind};

const CHILD_ID: WorkflowId = 1;

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
    let (id_ptr, id_len) = WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"test:latest")?;
    let (args_ptr, args_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"child_input")?;
    let handles = SpawnFunctions::create_channel_handles();
    configure_handles(ctx.as_context_mut(), handles.clone())?;
    let workflow = SpawnFunctions::spawn(
        ctx.as_context_mut(),
        id_ptr,
        id_len,
        args_ptr,
        args_len,
        handles,
        ERROR_PTR,
    )?;
    assert!(workflow.is_some());

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

#[test]
fn spawning_child_workflow() {
    let poll_child_channel: MockPollFn = |mut ctx| {
        let child_traces = Some(WorkflowData::inbound_channel_ref(Some(CHILD_ID), "traces"));
        let poll_result =
            WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), child_traces, POLL_CX)?;
        assert_ne!(poll_result, -1); // Poll::Ready(Some(_))

        Ok(Poll::Pending)
    };

    let poll_fns = Answers::from_values([spawn_child, poll_child_channel]);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow_id = create_test_workflow(&manager).id();
    manager.tick_workflow(workflow_id).unwrap();

    let persisted = manager.workflow(workflow_id).unwrap().persisted();
    let mut children: Vec<_> = persisted.child_workflows().collect();
    assert_eq!(children.len(), 1);
    let (child_id, child_state) = children.pop().unwrap();
    assert_eq!(child_id, CHILD_ID);
    let traces_id = child_state.inbound_channel("traces").unwrap().id();
    let pending_events: Vec<_> = persisted.pending_events().collect();
    assert!(pending_events.is_empty(), "{:?}", pending_events);

    {
        let state = manager.lock();
        assert_eq!(state.workflows.len(), 2);
        assert!(state.workflows.contains_key(&child_id));
        assert_eq!(
            state.channels[&traces_id].receiver_workflow_id,
            Some(workflow_id)
        );
        assert_eq!(
            state.channels[&traces_id].sender_workflow_ids,
            HashSet::from_iter([child_id])
        );
        assert!(!state.channels[&traces_id].has_external_sender);
        assert_eq!(state.find_workflow_with_pending_tasks(), Some(child_id));
    }

    // Emulate the child workflow putting a message to the `traces` channel.
    manager.send_message(traces_id, b"trace".to_vec()).unwrap();
    assert_eq!(
        manager.lock().find_consumable_channel(),
        Some((traces_id, workflow_id))
    );

    let receipt = manager
        .feed_message_to_workflow(traces_id, workflow_id)
        .unwrap();
    let events = extract_channel_events(&receipt, Some(child_id), "traces");
    assert_matches!(
        events.as_slice(),
        [ChannelEventKind::InboundChannelPolled {
            result: Poll::Ready(Some(5)),
        }]
    );

    // Check channel handles for child workflow
    let mut handle = manager.workflow(child_id).unwrap().handle();
    let mut child_orders = handle.remove(InboundChannel("orders")).unwrap();
    let child_orders_id = child_orders.channel_id();
    child_orders.send(b"test".to_vec()).unwrap();
    child_orders.close(); // no-op: a channel sender is not owned by the host

    let mut child_events = handle.remove(OutboundChannel("events")).unwrap();
    let child_events_id = child_events.channel_id();
    assert!(!child_events.can_receive_messages());
    assert!(child_events.take_messages().is_none());
    child_events.close(); // no-op: the channel receiver is not owned by the host

    {
        let state = manager.lock();
        assert!(!state.channels[&child_orders_id].is_closed);
        assert!(!state.channels[&child_events_id].is_closed);
    }
}

#[test]
fn sending_message_to_child() {
    let spawn_child_and_send_message: MockPollFn = |mut ctx| {
        let _ = spawn_child(ctx.as_context_mut())?;
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
        spawn_child_and_send_message,
        init_child,
        poll_orders_in_child,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow_id = create_test_workflow(&manager).id();
    manager.tick_workflow(workflow_id).unwrap();

    let state = manager.lock();
    let child_channel_ids = state.channel_ids(CHILD_ID).unwrap();
    let child_orders_id = child_channel_ids.inbound["orders"];
    let child_orders = &state.channels[&child_orders_id].messages;
    assert_eq!(child_orders.len(), 1);
    assert_eq!(child_orders[0].as_ref(), b"child_order");
    drop(state);

    manager.tick_workflow(CHILD_ID).unwrap();
    assert_eq!(
        manager.lock().find_consumable_channel(),
        Some((child_orders_id, CHILD_ID))
    );
    manager
        .feed_message_to_workflow(child_orders_id, CHILD_ID)
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

fn spawn_and_poll_child(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let _ = spawn_child(ctx.as_context_mut())?;
    let child = Some(WorkflowData::child_ref(CHILD_ID));
    let poll_result =
        SpawnFunctions::poll_workflow_completion(ctx.as_context_mut(), child, POLL_CX)?;
    assert_eq!(poll_result, -1); // Poll::Pending

    Ok(Poll::Pending)
}

fn test_child_workflow_channel_management(complete_child: bool) {
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
        spawn_and_poll_child,
        poll_child_workflow,
        test_child_resources,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow_id = create_test_workflow(&manager).id();
    manager.tick_workflow(workflow_id).unwrap();

    let persisted = manager.workflow(workflow_id).unwrap().persisted();
    let mut children: Vec<_> = persisted.child_workflows().collect();
    assert_eq!(children.len(), 1);
    let (child_id, child_state) = children.pop().unwrap();
    assert_eq!(child_id, CHILD_ID);
    let orders_id = child_state.outbound_channel("orders").unwrap().id();
    let traces_id = child_state.inbound_channel("traces").unwrap().id();

    manager.tick_workflow(child_id).unwrap();
    let channel_info = manager.channel(orders_id).unwrap();
    assert!(channel_info.is_closed);
    let channel_info = manager.channel(traces_id).unwrap();
    assert!(channel_info.is_closed);

    let persisted = manager.workflow(workflow_id).unwrap().persisted();
    let child_state = persisted.child_workflow(child_id).unwrap();
    if complete_child {
        assert_matches!(child_state.result(), Poll::Ready(Ok(())));
    } else {
        assert_matches!(child_state.result(), Poll::Pending);
    }

    let events: Vec<_> = persisted.pending_events().collect();
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

    let receipt = manager.tick_workflow(workflow_id).unwrap();
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

#[test]
fn child_workflow_completion() {
    test_child_workflow_channel_management(true);
}

#[test]
fn child_workflow_channel_closure() {
    test_child_workflow_channel_management(false);
}

fn spawn_child_with_copied_outbound_channel(
    mut ctx: StoreContextMut<'_, WorkflowData>,
    copy_traces: bool,
) -> Result<Poll<()>, Trap> {
    let (id_ptr, id_len) = WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"test:latest")?;
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

    let workflow = SpawnFunctions::spawn(
        ctx.as_context_mut(),
        id_ptr,
        id_len,
        args_ptr,
        args_len,
        handles,
        ERROR_PTR,
    )?;
    assert!(workflow.is_some());

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

#[test]
fn spawning_child_with_copied_outbound_channel() {
    let init: MockPollFn = |ctx| spawn_child_with_copied_outbound_channel(ctx, false);
    let write_event_and_complete_child: MockPollFn = |mut ctx| {
        let events = Some(WorkflowData::outbound_channel_ref(None, "events"));
        let (message_ptr, message_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"child_event")?;
        WorkflowFunctions::start_send(ctx.as_context_mut(), events, message_ptr, message_len)?;
        Ok(Poll::Ready(()))
    };

    let poll_fns = Answers::from_values([init, write_event_and_complete_child]);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow = create_test_workflow(&manager);
    let workflow_id = workflow.id();
    let events_id = workflow.ids().channel_ids.outbound["events"];
    manager.tick_workflow(workflow_id).unwrap();

    let state = manager.lock();
    assert_eq!(
        state.channels[&events_id].sender_workflow_ids,
        HashSet::from_iter([workflow_id, CHILD_ID])
    );
    let child = &state.workflows[&CHILD_ID].workflow;
    let child_events_id = child.outbound_channels().find_map(|(_, name, state)| {
        if name == "events" {
            Some(state.id())
        } else {
            None
        }
    });
    assert_eq!(child_events_id, Some(events_id));
    drop(state);

    manager.tick_workflow(CHILD_ID).unwrap();
    let state = manager.lock();
    let channel_state = &state.channels[&events_id];
    assert_eq!(
        channel_state.sender_workflow_ids,
        HashSet::from_iter([workflow_id])
    );
    assert_eq!(channel_state.receiver_workflow_id, None);
    assert_eq!(channel_state.messages.len(), 1);
    assert_eq!(channel_state.messages[0].as_ref(), b"child_event");
}

fn test_child_with_copied_closed_outbound_channel(close_before_spawn: bool) {
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

    let poll_fns = Answers::from_values([init, test_writing_event_in_child]);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow = create_test_workflow(&manager);
    let workflow_id = workflow.id();
    let events_id = workflow.ids().channel_ids.outbound["events"];

    if close_before_spawn {
        manager.close_host_receiver(events_id);
    }
    manager.tick_workflow(workflow_id).unwrap();
    if !close_before_spawn {
        manager.close_host_receiver(events_id);
    }

    manager.tick_workflow(CHILD_ID).unwrap();
}

#[test]
fn spawning_child_with_copied_closed_outbound_channel() {
    test_child_with_copied_closed_outbound_channel(true);
}

#[test]
fn spawning_child_with_copied_then_closed_outbound_channel() {
    test_child_with_copied_closed_outbound_channel(false);
}

fn test_child_with_aliased_outbound_channel(complete_child: bool) {
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
        write_event_and_drop_events,
        complete_child_or_drop_traces,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow = create_test_workflow(&manager);
    let workflow_id = workflow.id();
    let events_id = workflow.ids().channel_ids.outbound["events"];
    manager.tick_workflow(workflow_id).unwrap();

    let state = manager.lock();
    assert_eq!(
        state.channels[&events_id].sender_workflow_ids,
        HashSet::from_iter([workflow_id, CHILD_ID])
    );
    let child_channel_ids = state.channel_ids(CHILD_ID).unwrap();
    assert_eq!(child_channel_ids.outbound["events"], events_id);
    assert_eq!(child_channel_ids.outbound["traces"], events_id);
    drop(state);

    manager.tick_workflow(CHILD_ID).unwrap();
    let state = manager.lock();
    let channel_state = &state.channels[&events_id];
    assert_eq!(
        channel_state.sender_workflow_ids,
        HashSet::from_iter([workflow_id, CHILD_ID])
    );
    let outbound_messages: Vec<_> = channel_state.messages.iter().map(Message::as_ref).collect();
    assert_eq!(outbound_messages, [b"child_event" as &[u8], b"child_trace"]);
    drop(state);

    manager.tick_workflow(CHILD_ID).unwrap();
    let state = manager.lock();
    let channel_state = &state.channels[&events_id];
    assert_eq!(
        channel_state.sender_workflow_ids,
        HashSet::from_iter([workflow_id])
    );
}

#[test]
fn spawning_child_with_aliased_outbound_channel() {
    test_child_with_aliased_outbound_channel(false);
}

#[test]
fn spawning_and_completing_child_with_aliased_outbound_channel() {
    test_child_with_aliased_outbound_channel(true);
}

#[test]
fn completing_child_with_error() {
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
        spawn_and_poll_child,
        complete_task_with_error,
        check_child_completion,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow_id = create_test_workflow(&manager).id();
    manager.tick_workflow(workflow_id).unwrap();
    manager.tick_workflow(CHILD_ID).unwrap();

    let receipt = manager.tick_workflow(workflow_id).unwrap();
    let is_child_polled = receipt.events().any(|event| {
        event.as_resource_event().map_or(false, |event| {
            matches!(event.resource_id, ResourceId::Workflow(CHILD_ID))
                && matches!(event.kind, ResourceEventKind::Polled(Poll::Ready(())))
        })
    });
    assert!(is_child_polled, "{:?}", receipt);
}

fn complete_task_with_panic(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let (message_ptr, message_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"panic message")?;
    let (filename_ptr, filename_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"/build/src/test.rs")?;
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

#[test]
fn completing_child_with_panic() {
    let check_child_completion: MockPollFn = |ctx| {
        let child = Some(WorkflowData::child_ref(CHILD_ID));
        let poll_result = SpawnFunctions::poll_workflow_completion(ctx, child, POLL_CX)?;
        assert_eq!(poll_result, -3); // Poll::Ready(Err(Aborted))

        Ok(Poll::Pending)
    };

    let poll_fns = Answers::from_values([
        spawn_and_poll_child,
        complete_task_with_panic,
        check_child_completion,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow_id = create_test_workflow(&manager).id();

    let tick_result = manager.tick().unwrap();
    assert_eq!(tick_result.workflow_id(), workflow_id);
    tick_result.into_inner().unwrap();
    let tick_result = manager.tick().unwrap();
    assert_eq!(tick_result.workflow_id(), CHILD_ID);
    let err = tick_result.abort_workflow().into_inner().unwrap_err();
    assert_eq!(
        err.panic_info().unwrap().message.as_deref(),
        Some("panic message")
    );

    let receipt = manager.tick_workflow(workflow_id).unwrap();
    let is_child_polled = receipt.events().any(|event| {
        event.as_resource_event().map_or(false, |event| {
            matches!(event.resource_id, ResourceId::Workflow(CHILD_ID))
                && matches!(event.kind, ResourceEventKind::Polled(Poll::Ready(())))
        })
    });
    assert!(is_child_polled, "{:?}", receipt);
}
