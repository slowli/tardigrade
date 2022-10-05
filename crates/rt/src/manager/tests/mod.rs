//! Tests for `WorkflowManager`.

use assert_matches::assert_matches;
use mimicry::Answers;
use wasmtime::{AsContextMut, StoreContextMut, Trap};

use std::{collections::HashSet, task::Poll};

mod spawn;

use super::*;
use crate::{
    data::{WasmContextPtr, WorkflowData, WorkflowFunctions},
    module::{ExportsMock, MockPollFn},
    receipt::{ExecutedFunction, WakeUpCause},
    utils::WasmAllocator,
    WorkflowEngine, WorkflowModule,
};
use tardigrade::{
    interface::{InboundChannel, OutboundChannel},
    spawn::ManageWorkflowsExt,
};
use tardigrade_shared::abi::AllocateBytes;

const POLL_CX: WasmContextPtr = 123;
const ERROR_PTR: u32 = 1_024;

fn create_test_spawner() -> WorkflowSpawner<()> {
    let engine = WorkflowEngine::default();
    WorkflowModule::new(&engine, ExportsMock::MOCK_MODULE_BYTES)
        .unwrap()
        .for_untyped_workflow("TestWorkflow")
        .unwrap()
}

fn create_test_manager() -> WorkflowManager {
    WorkflowManager::builder()
        .with_spawner("test:latest", create_test_spawner())
        .build()
}

fn create_test_workflow(manager: &WorkflowManager) -> WorkflowHandle<'_, ()> {
    manager
        .new_workflow("test:latest", b"test_input".to_vec())
        .unwrap()
        .build()
        .unwrap()
}

fn initialize_task(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
    let poll_result =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
    assert_eq!(poll_result, -1); // Pending

    let traces = Some(WorkflowData::outbound_channel_ref(None, "traces"));
    let (trace_ptr, trace_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"trace #1")?;
    WorkflowFunctions::start_send(ctx.as_context_mut(), traces, trace_ptr, trace_len)?;
    // ignore send result

    Ok(Poll::Pending)
}

#[test]
fn instantiating_workflow() {
    let poll_fns = Answers::from_value(initialize_task as MockPollFn);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow = create_test_workflow(&manager);

    let state = manager.lock();
    assert_eq!(
        state.find_workflow_with_pending_tasks(),
        Some(workflow.id())
    );
    let persisted = &state.workflows[&workflow.id()];
    assert_eq!(persisted.definition_id, "test:latest");
    let orders_id = workflow.ids().channel_ids.inbound["orders"];
    assert_eq!(
        state.channels[&orders_id].receiver_workflow_id,
        Some(workflow.id())
    );
    assert_eq!(
        state.channels[&orders_id].sender_workflow_ids,
        HashSet::new()
    );
    assert!(state.channels[&orders_id].has_external_sender);

    let traces_id = workflow.ids().channel_ids.outbound["traces"];
    assert_eq!(state.channels[&traces_id].receiver_workflow_id, None);
    assert_eq!(
        state.channels[&traces_id].sender_workflow_ids,
        HashSet::from_iter([workflow.id()])
    );
    assert!(!state.channels[&traces_id].has_external_sender);
    drop(state);

    let ids = workflow.ids().clone();
    test_initializing_workflow(&mut manager, &ids);
}

fn test_initializing_workflow(manager: &mut WorkflowManager, handle: &WorkflowAndChannelIds) {
    let receipt = manager.tick_workflow(handle.workflow_id).unwrap();
    assert_eq!(receipt.executions().len(), 2);
    let main_execution = &receipt.executions()[0];
    assert_matches!(main_execution.function, ExecutedFunction::Entry { .. });

    let traces_id = handle.channel_ids.outbound["traces"];
    let state = manager.lock();
    assert_eq!(state.find_workflow_with_pending_tasks(), None);
    assert_eq!(state.find_consumable_channel(), None);
    let traces = &state.channels[&traces_id].messages;
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].as_ref(), b"trace #1");
}

#[test]
fn initializing_workflow_with_closed_channels() {
    let test_channels: MockPollFn = |mut ctx| {
        let traces = Some(WorkflowData::outbound_channel_ref(None, "traces"));
        let poll_result =
            WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), traces, POLL_CX)?;
        assert_eq!(poll_result, 2); // Err(Closed)

        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        let poll_result =
            WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
        assert_eq!(poll_result, -2); // Ready(None)

        Ok(Poll::Pending)
    };

    let _guard = ExportsMock::prepare(Answers::from_value(test_channels));
    let mut manager = create_test_manager();
    let builder = manager
        .new_workflow::<()>("test:latest", b"test_input".to_vec())
        .unwrap();
    builder.handle()[InboundChannel("orders")].close();
    builder.handle()[OutboundChannel("traces")].close();
    let workflow = builder.build().unwrap();
    let workflow_id = workflow.id();

    assert_eq!(workflow.ids().channel_ids.inbound["orders"], 0);
    assert_eq!(workflow.ids().channel_ids.outbound["traces"], 0);

    manager.tick_workflow(workflow_id).unwrap();
    let mut handle = manager.workflow(workflow_id).unwrap().handle();
    let channel_info = handle[InboundChannel("orders")].channel_info();
    assert!(channel_info.is_closed());
    assert_eq!(channel_info.received_messages(), 0);
    assert_eq!(channel_info.flushed_messages(), 0);
    let channel_info = handle[OutboundChannel("traces")].channel_info();
    assert!(channel_info.is_closed());

    let err = handle[InboundChannel("orders")]
        .send(b"test".to_vec())
        .unwrap_err();
    assert_matches!(err, SendError::Closed);
}

#[test]
fn closing_workflow_channels() {
    let block_on_flush: MockPollFn = |mut ctx| {
        let events = Some(WorkflowData::outbound_channel_ref(None, "events"));
        let (ptr, len) = WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"event #1")?;
        WorkflowFunctions::start_send(ctx.as_context_mut(), events.clone(), ptr, len)?;
        let poll_result =
            WorkflowFunctions::poll_flush_for_sender(ctx.as_context_mut(), events, POLL_CX)?;
        assert_eq!(poll_result, -1); // Pending
        Ok(Poll::Pending)
    };
    let drop_inbound_channel: MockPollFn = |mut ctx| {
        // Check that the outbound channel is closed. Note that flushing the channel
        // must succeed (messages are flushed before the closure).
        let events = Some(WorkflowData::outbound_channel_ref(None, "events"));
        let poll_result = WorkflowFunctions::poll_flush_for_sender(
            ctx.as_context_mut(),
            events.clone(),
            POLL_CX,
        )?;
        assert_eq!(poll_result, 0); // Ok(())

        let poll_result =
            WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), events, POLL_CX)?;
        assert_eq!(poll_result, 2); // Err(Closed)

        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        WorkflowFunctions::drop_ref(ctx.as_context_mut(), orders)?;
        Ok(Poll::Pending)
    };

    let poll_fns = Answers::from_values([block_on_flush, drop_inbound_channel]);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow = create_test_workflow(&manager);
    let workflow_id = workflow.id();
    let events_id = workflow.ids().channel_ids.outbound["events"];
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    manager.tick_workflow(workflow_id).unwrap(); // initializes the workflow
    manager.close_host_receiver(events_id);
    let channel_info = manager.channel_info(events_id).unwrap();
    assert!(channel_info.is_closed());
    {
        let persisted = manager.workflow(workflow_id).unwrap().persisted();
        let events: Vec<_> = persisted.pending_events().collect();
        assert_matches!(
            events.as_slice(),
            [WakeUpCause::Flush { workflow_id: None, channel_name, .. }]
                if channel_name == "events"
        );
    }

    manager.tick_workflow(workflow_id).unwrap();
    let channel_info = manager.channel_info(orders_id).unwrap();
    assert!(channel_info.is_closed());
}

fn extract_channel_events<'a>(
    receipt: &'a Receipt,
    child_id: Option<WorkflowId>,
    name: &str,
) -> Vec<&'a ChannelEventKind> {
    let channel_events = receipt.events().filter_map(|event| {
        if let Some(ChannelEvent {
            kind,
            channel_name,
            workflow_id,
        }) = event.as_channel_event()
        {
            if channel_name == name && *workflow_id == child_id {
                return Some(kind);
            }
        }
        None
    });
    channel_events.collect()
}

fn test_closing_inbound_channel_from_host_side(with_message: bool) {
    let poll_inbound_channel: MockPollFn = |mut ctx| {
        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        let poll_result = WorkflowFunctions::poll_next_for_receiver(
            ctx.as_context_mut(),
            orders.clone(),
            POLL_CX,
        )?;
        assert_ne!(poll_result, -1); // Poll::Ready(Some(_))

        let poll_result = WorkflowFunctions::poll_next_for_receiver(ctx, orders, POLL_CX)?;
        assert_eq!(poll_result, -1);
        // ^ Poll::Pending; we don't dispatch the closure signal immediately

        Ok(Poll::Pending)
    };
    let test_closed_channel: MockPollFn = |ctx| {
        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        let poll_result = WorkflowFunctions::poll_next_for_receiver(ctx, orders, POLL_CX)?;
        assert_eq!(poll_result, -2); // Poll::Ready(None)

        Ok(Poll::Pending)
    };

    let poll_fns: Vec<MockPollFn> = if with_message {
        vec![initialize_task, poll_inbound_channel, test_closed_channel]
    } else {
        vec![initialize_task, test_closed_channel]
    };
    let _guard = ExportsMock::prepare(Answers::from_values(poll_fns));
    let mut manager = create_test_manager();
    let workflow = create_test_workflow(&manager);
    let workflow_id = workflow.id();
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    manager.tick_workflow(workflow_id).unwrap(); // initializes the workflow
    if with_message {
        manager
            .send_message(orders_id, b"order #1".to_vec())
            .unwrap();
    }
    manager.close_host_sender(orders_id);

    if with_message {
        assert_eq!(
            manager.lock().find_consumable_channel(),
            Some((orders_id, workflow_id))
        );

        let receipt = manager
            .feed_message_to_workflow(orders_id, workflow_id)
            .unwrap();
        let order_events = extract_channel_events(&receipt, None, "orders");
        assert_matches!(
            order_events.as_slice(),
            [
                ChannelEventKind::InboundChannelPolled {
                    result: Poll::Ready(Some(8)),
                },
                ChannelEventKind::InboundChannelPolled {
                    result: Poll::Pending,
                },
            ]
        );
    } else {
        assert_eq!(manager.lock().find_consumable_channel(), None);
    }

    {
        let persisted = manager.workflow(workflow_id).unwrap().persisted();
        let pending_events = persisted.pending_events().collect::<Vec<_>>();
        assert_matches!(
            pending_events.as_slice(),
            [WakeUpCause::ChannelClosed { workflow_id: None, channel_name }]
                if channel_name == "orders"
        );
    }

    let receipt = manager.tick_workflow(workflow_id).unwrap();
    let order_events = extract_channel_events(&receipt, None, "orders");
    assert_matches!(
        order_events.as_slice(),
        [ChannelEventKind::InboundChannelPolled {
            result: Poll::Ready(None),
        }]
    );
}

#[test]
fn closing_inbound_channel_from_host_side() {
    test_closing_inbound_channel_from_host_side(false);
}

#[test]
fn closing_inbound_channel_with_message_from_host_side() {
    test_closing_inbound_channel_from_host_side(true);
}

#[test]
fn error_initializing_workflow() {
    let error_on_initialization: MockPollFn = |_| Err(Trap::new("oops"));
    let poll_fns = Answers::from_value(error_on_initialization);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow_id = create_test_workflow(&manager).id();

    let err = manager.tick_workflow(workflow_id).unwrap_err();
    let err = err.trap().display_reason().to_string();
    assert!(err.contains("oops"), "{}", err);

    let persisted = manager.workflow(workflow_id).unwrap().persisted();
    assert!(!persisted.is_initialized());
}

fn poll_receiver(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
    let poll_result =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
    assert_eq!(poll_result, -1); // Poll::Pending

    Ok(Poll::Pending)
}

fn consume_message(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
    let poll_result =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
    assert_ne!(poll_result, -1); // Poll::Ready

    Ok(Poll::Pending)
}

#[test]
fn sending_message_to_workflow() {
    let poll_fns = Answers::from_values([poll_receiver as MockPollFn, consume_message]);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow = create_test_workflow(&manager);
    let workflow_id = workflow.id();
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    manager.tick_workflow(workflow_id).unwrap();
    manager
        .send_message(orders_id, b"order #1".to_vec())
        .unwrap();

    {
        let state = manager.state.lock().unwrap();
        let orders = &state.channels[&orders_id].messages;
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].as_ref(), b"order #1");
        assert_eq!(
            state.find_consumable_channel(),
            Some((orders_id, workflow_id))
        );
    }

    let receipt = manager
        .feed_message_to_workflow(orders_id, workflow_id)
        .unwrap();
    assert_eq!(receipt.executions().len(), 2); // waker + task
    let waker_execution = &receipt.executions()[1];
    assert_matches!(
        waker_execution.function,
        ExecutedFunction::Task {
            wake_up_cause: WakeUpCause::InboundMessage { .. },
            ..
        }
    );

    let state = manager.state.lock().unwrap();
    let orders = &state.channels[&orders_id].messages;
    assert!(orders.is_empty());
}

#[test]
fn error_processing_inbound_message_in_workflow() {
    let error_after_consuming_message: MockPollFn = |mut ctx| {
        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        let poll_result =
            WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
        assert_ne!(poll_result, -1); // Ready(Some(_))

        Err(Trap::new("oops"))
    };

    let _guard = ExportsMock::prepare(Answers::from_values([
        initialize_task,
        error_after_consuming_message,
    ]));
    let mut manager = create_test_manager();
    let workflow = create_test_workflow(&manager);
    let workflow_id = workflow.id();
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    manager.tick_workflow(workflow_id).unwrap(); // initializes the workflow
    manager.send_message(orders_id, b"test".to_vec()).unwrap();
    let err = manager
        .feed_message_to_workflow(orders_id, workflow_id)
        .unwrap_err();
    let err = err.trap().display_reason().to_string();
    assert!(err.contains("oops"), "{}", err);

    let channel_info = manager.channel_info(orders_id).unwrap();
    assert!(!channel_info.is_closed());
    assert_eq!(channel_info.received_messages(), 1);
    assert_eq!(channel_info.flushed_messages(), 0);
}

#[test]
fn workflow_not_consuming_inbound_message() {
    let not_consume_message: MockPollFn = |_| Ok(Poll::Pending);
    let poll_fns = Answers::from_values([poll_receiver as MockPollFn, not_consume_message]);
    let _guard = ExportsMock::prepare(poll_fns);
    let mut manager = create_test_manager();
    let workflow = create_test_workflow(&manager);
    let workflow_id = workflow.id();
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    manager.tick_workflow(workflow_id).unwrap();
    manager
        .send_message(orders_id, b"order #1".to_vec())
        .unwrap();
    manager
        .feed_message_to_workflow(orders_id, workflow_id)
        .unwrap();

    let state = manager.state.lock().unwrap();
    let orders = &state.channels[&orders_id].messages;
    assert_eq!(orders.len(), 1);

    // Workflow wakers should be consumed to not trigger the infinite loop.
    let events: Vec<_> = state.workflows[&workflow_id]
        .workflow
        .pending_events()
        .collect();
    assert!(events.is_empty(), "{:?}", events);
}
