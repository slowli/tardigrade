//! Tests for `WorkflowManager`.

use assert_matches::assert_matches;
use mimicry::Answers;
use wasmtime::{AsContextMut, StoreContextMut, Trap};

use std::task::Poll;

use super::*;
use crate::{
    data::{WasmContextPtr, WorkflowData, WorkflowFunctions},
    module::{ExportsMock, MockPollFn},
    receipt::{ExecutedFunction, WakeUpCause},
    utils::WasmAllocator,
    WorkflowEngine, WorkflowModule,
};
use tardigrade::spawn::ManageWorkflowsExt;
use tardigrade_shared::abi::AllocateBytes;

const POLL_CX: WasmContextPtr = 123;

fn test_spawner() -> WorkflowSpawner<()> {
    let engine = WorkflowEngine::default();
    WorkflowModule::new(&engine, ExportsMock::MOCK_MODULE_BYTES)
        .unwrap()
        .for_untyped_workflow("TestWorkflow")
        .unwrap()
}

fn initialize_task(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let traces = Some(WorkflowData::outbound_channel_ref(None, "traces"));
    let (trace_ptr, trace_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"trace #1")?;
    WorkflowFunctions::start_send(ctx.as_context_mut(), traces, trace_ptr, trace_len)?;

    Ok(Poll::Pending)
}

#[test]
fn instantiating_workflow() {
    let poll_fns = Answers::from_value(initialize_task as MockPollFn);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = WorkflowManager::builder()
        .with_spawner("test:latest", test_spawner())
        .build();
    let definition = manager.definition("test:latest").unwrap();
    let handle = definition
        .new_workflow(b"test_input".to_vec())
        .build()
        .unwrap();

    let state = manager.state.lock().unwrap();
    let persisted = &state.workflows[&handle.id()];
    assert_eq!(persisted.definition_id, "test:latest");
    let orders_id = handle.ids().channel_ids.inbound["orders"];
    assert_eq!(
        state.channels[&orders_id].receiver_workflow_id,
        Some(handle.id())
    );
    let traces_id = handle.ids().channel_ids.outbound["traces"];
    assert_eq!(state.channels[&traces_id].receiver_workflow_id, None);
    drop(state); // in order for `test_initializing_workflow()` not to dead-lock

    test_initializing_workflow(&manager, handle.ids());
}

fn test_initializing_workflow(manager: &WorkflowManager, handle: &WorkflowAndChannelIds) {
    let receipt = manager.tick_workflow(handle.workflow_id).unwrap();
    assert_eq!(receipt.executions().len(), 2);
    let main_execution = &receipt.executions()[0];
    assert_matches!(main_execution.function, ExecutedFunction::Entry { .. });

    let traces_id = handle.channel_ids.outbound["traces"];
    let state = manager.state.lock().unwrap();
    let traces = &state.channels[&traces_id].messages;
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].as_ref(), b"trace #1");
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
    let manager = WorkflowManager::builder()
        .with_spawner("test:latest", test_spawner())
        .build();
    let definition = manager.definition("test:latest").unwrap();
    let handle = definition
        .new_workflow(b"test_input".to_vec())
        .build()
        .unwrap();

    manager.tick_workflow(handle.id()).unwrap();
    let orders_id = handle.ids().channel_ids.inbound["orders"];
    manager
        .send_message(orders_id, b"order #1".to_vec())
        .unwrap();

    {
        let state = manager.state.lock().unwrap();
        let orders = &state.channels[&orders_id].messages;
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].as_ref(), b"order #1");
    }

    let receipt = manager
        .feed_message_to_workflow(orders_id, handle.id())
        .unwrap()
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

// FIXME: test errors during initialization; when sending a message
