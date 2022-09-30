//! Tests related to interaction among workflows.

use wasmtime::ExternRef;

use super::*;
use crate::receipt::{ResourceEventKind, ResourceId};
use crate::{data::SpawnFunctions, receipt::ResourceEvent};
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
}

fn spawn_and_poll_child(mut ctx: StoreContextMut<WorkflowData>) -> Result<Poll<()>, Trap> {
    let _ = spawn_child(ctx.as_context_mut())?;

    let child = Some(WorkflowData::child_ref(CHILD_ID));
    let poll_result =
        SpawnFunctions::poll_workflow_completion(ctx.as_context_mut(), child, POLL_CX)?;
    assert_eq!(poll_result, -1); // Poll::Pending

    Ok(Poll::Pending)
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
        spawn_and_poll_child as MockPollFn,
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
    let channel_info = manager.channel_info(orders_id).unwrap();
    assert!(channel_info.is_closed);
    let channel_info = manager.channel_info(traces_id).unwrap();
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

// FIXME: test copying outbound channel (open, closed)
