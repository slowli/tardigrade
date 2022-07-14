use assert_matches::assert_matches;
use mimicry::Answers;
use wasmtime::StoreContextMut;

use std::collections::HashSet;

use super::*;
use crate::{
    data::{WasmContextPtr, WorkflowFunctions},
    module::{ExportsMock, MockPollFn},
    receipt::{ChannelEvent, ChannelEventKind},
    utils::{copy_string_from_wasm, WasmAllocator},
    WorkflowEngine,
};
use tardigrade::workflow::InputsBuilder;
use tardigrade_shared::{abi::AllocateBytes, JoinError};

const POLL_CX: WasmContextPtr = 1234;

#[derive(Debug, Clone, Copy)]
enum StaticStr {
    Inputs,
    Orders,
    Traces,
    Events,
}

impl StaticStr {
    fn alloc(ctx: &mut StoreContextMut<'_, WorkflowData>) {
        for str in [Self::Inputs, Self::Orders, Self::Traces, Self::Events] {
            let allocated = WasmAllocator::new(ctx.as_context_mut())
                .copy_to_wasm(str.as_str().as_bytes())
                .unwrap();
            assert_eq!(allocated, str.ptr_and_len());
        }
    }

    const fn ptr_and_len(self) -> (u32, u32) {
        match self {
            Self::Inputs => (0, 6),
            Self::Orders => (6, 6),
            Self::Traces => (12, 6),
            Self::Events => (18, 6),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Inputs => "inputs",
            Self::Orders => "orders",
            Self::Traces => "traces",
            Self::Events => "events",
        }
    }
}

#[allow(clippy::cast_sign_loss)]
fn decode_message_poll(poll_res: i64) -> (u32, u32) {
    let ptr = ((poll_res as u64) >> 32) as u32;
    let len = (poll_res & 0x_ffff_ffff) as u32;
    (ptr, len)
}

#[allow(clippy::unnecessary_wraps)] // more convenient for use with mock `Answers`
fn initialize_task(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    StaticStr::alloc(&mut ctx);

    // Emulate basic task startup: getting inputs...
    let (ptr, len) = StaticStr::Inputs.ptr_and_len();
    WorkflowFunctions::get_data_input(ctx.as_context_mut(), ptr, len).unwrap();

    // ...then getting the inbound channel
    let (ptr, len) = StaticStr::Orders.ptr_and_len();
    WorkflowFunctions::get_receiver(ctx.as_context_mut(), ptr, len).unwrap();

    // ...then polling this channel
    let poll_res =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), ptr, len, POLL_CX).unwrap();
    assert_eq!(poll_res, -1); // Poll::Pending

    Ok(Poll::Pending)
}

fn emit_event_and_flush(ctx: &mut StoreContextMut<'_, WorkflowData>) {
    let (ptr, len) = StaticStr::Events.ptr_and_len();
    let poll_res =
        WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), ptr, len, POLL_CX).unwrap();
    assert_eq!(poll_res, 0); // Poll::Ready

    let (event_ptr, event_len) = WasmAllocator::new(ctx.as_context_mut())
        .copy_to_wasm(b"event #1")
        .unwrap();
    WorkflowFunctions::start_send(ctx.as_context_mut(), ptr, len, event_ptr, event_len).unwrap();

    let poll_res =
        WorkflowFunctions::poll_flush_for_sender(ctx.as_context_mut(), ptr, len, POLL_CX).unwrap();
    assert_eq!(poll_res, -1); // Poll::Pending
}

#[allow(clippy::unnecessary_wraps)] // more convenient for use with mock `Answers`
fn consume_message(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    // Poll the channel again, since we yielded on this previously
    let (ptr, len) = StaticStr::Orders.ptr_and_len();
    let poll_res =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), ptr, len, POLL_CX).unwrap();

    let (msg_ptr, msg_len) = decode_message_poll(poll_res);
    let memory = ctx.data().exports().memory;
    let message = copy_string_from_wasm(&ctx, &memory, msg_ptr, msg_len).unwrap();
    assert_eq!(message, "order #1");

    // Emit a trace (shouldn't block the task).
    let (ptr, len) = StaticStr::Traces.ptr_and_len();
    let poll_res =
        WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), ptr, len, POLL_CX).unwrap();
    assert_eq!(poll_res, 0); // Poll::Ready

    let (trace_ptr, trace_len) = WasmAllocator::new(ctx.as_context_mut())
        .copy_to_wasm(b"trace #1")
        .unwrap();
    WorkflowFunctions::start_send(ctx.as_context_mut(), ptr, len, trace_ptr, trace_len).unwrap();

    emit_event_and_flush(&mut ctx);
    // Check that sending events is no longer possible.
    let (ptr, len) = StaticStr::Events.ptr_and_len();
    let poll_res =
        WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), ptr, len, POLL_CX).unwrap();
    assert_eq!(poll_res, -1); // Poll::Pending

    // Check that sending traces is still possible
    let (ptr, len) = StaticStr::Traces.ptr_and_len();
    let poll_res =
        WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), ptr, len, POLL_CX).unwrap();
    assert_eq!(poll_res, 0); // Poll::Ready

    Ok(Poll::Pending)
}

#[test]
fn starting_workflow() {
    let poll_fns = Answers::from_value(initialize_task as MockPollFn);
    let exports_guard = ExportsMock::prepare(poll_fns);

    let engine = WorkflowEngine::default();
    let module = WorkflowModule::<()>::new(&engine, ExportsMock::MOCK_MODULE_BYTES).unwrap();
    let mut inputs = InputsBuilder::new(module.interface());
    inputs.insert("inputs", b"test_input".to_vec());
    let receipt = Workflow::new(&module, inputs.build()).unwrap();

    let exports_mock = exports_guard.into_inner();
    assert!(exports_mock.exports_created);

    assert_eq!(receipt.executions().len(), 1);
    let execution = &receipt.executions()[0];
    assert_matches!(
        &execution.function,
        ExecutedFunction::Task {
            task_id: 0,
            wake_up_cause: WakeUpCause::Spawned(spawn_fn),
            poll_result: Poll::Pending,
        } if matches!(spawn_fn.as_ref(), ExecutedFunction::Entry)
    );
    assert_eq!(execution.events.len(), 1);
    assert_matches!(
        &execution.events[0],
        Event::Channel(ChannelEvent {
            kind: ChannelEventKind::InboundChannelPolled { result: Poll::Pending },
            channel_name,
        }) if channel_name == "orders"
    );

    let workflow = receipt.into_inner();
    workflow.persist().unwrap();
}

#[test]
fn receiving_inbound_message() {
    let poll_fns = Answers::from_values([initialize_task, consume_message]);
    let exports_guard = ExportsMock::prepare(poll_fns);

    let engine = WorkflowEngine::default();
    let module = WorkflowModule::<()>::new(&engine, ExportsMock::MOCK_MODULE_BYTES).unwrap();
    let mut inputs = InputsBuilder::new(module.interface());
    inputs.insert("inputs", b"test_input".to_vec());
    let mut workflow = Workflow::new(&module, inputs.build()).unwrap().into_inner();

    workflow
        .push_inbound_message("orders", b"order #1".to_vec())
        .unwrap();
    let receipt = workflow.tick().unwrap();

    let exports = exports_guard.into_inner();
    assert_eq!(exports.next_waker, 3); // 1 waker for first execution and 2 for the second one
    assert_eq!(exports.consumed_wakers.len(), 1);
    assert!(exports.consumed_wakers.contains(&0));

    assert_inbound_message_receipt(&receipt);

    let (_, traces) = workflow.take_outbound_messages("traces");
    assert_eq!(traces.len(), 1);
    assert_eq!(&traces[0], b"trace #1");
    let (_, events) = workflow.take_outbound_messages("events");
    assert_eq!(events.len(), 1);
    assert_eq!(&events[0], b"event #1");

    let (waker_ids, causes): (HashSet<_>, Vec<_>) = workflow.store.data_mut().take_wakers().unzip();
    assert_eq!(waker_ids.len(), 2);
    assert_eq!(causes.len(), 2);
    for cause in &causes {
        assert_matches!(
            cause,
            WakeUpCause::Flush { channel_name, message_indexes }
                if channel_name == "events" && *message_indexes == (0..1)
        );
    }
}

fn assert_inbound_message_receipt(receipt: &Receipt) {
    assert_eq!(receipt.executions().len(), 2);
    assert_matches!(
        &receipt.executions()[0],
        Execution {
            function: ExecutedFunction::Waker {
                waker_id: 0,
                wake_up_cause: WakeUpCause::InboundMessage {
                    channel_name,
                    message_index: 0,
                }
            },
            events,
        } if channel_name == "orders" && events.is_empty()
    );
    let task_execution = &receipt.executions()[1];
    assert_matches!(
        task_execution.function,
        ExecutedFunction::Task { task_id: 0, .. }
    );

    let events = task_execution.events.iter().map(|evt| match evt {
        Event::Channel(evt) => evt,
        _ => panic!("unexpected event"),
    });
    let events: Vec<_> = events.collect();
    assert_matches!(
        &events[0..6],
        [
            ChannelEvent {
                kind: ChannelEventKind::InboundChannelPolled { result: Poll::Ready(Some(_)) },
                channel_name: orders,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelReady { result: Poll::Ready(()) },
                channel_name: traces,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundMessageSent { .. },
                channel_name: traces2,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelReady { result: Poll::Ready(()) },
                channel_name: events,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundMessageSent { .. },
                channel_name: events2,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelFlushed { result: Poll::Pending },
                channel_name: events3,
            },
        ] if orders == "orders" && traces == "traces" && traces2 == "traces"
            && events == "events" && events2 == "events" && events3 == "events"
    );
}

#[test]
fn trap_when_starting_workflow() {
    let trap: MockPollFn = |_| Err(Trap::new("boom"));
    let poll_fns = Answers::from_value(trap);
    let _guard = ExportsMock::prepare(poll_fns);

    let engine = WorkflowEngine::default();
    let module = WorkflowModule::<()>::new(&engine, ExportsMock::MOCK_MODULE_BYTES).unwrap();
    let mut inputs = InputsBuilder::new(module.interface());
    inputs.insert("inputs", b"test_input".to_vec());
    let err = Workflow::new(&module, inputs.build())
        .unwrap_err()
        .to_string();

    assert!(err.contains("failed polling main task"), "{}", err);
}

#[allow(clippy::unnecessary_wraps)] // more convenient for use with mock `Answers`
fn initialize_and_spawn_task(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let _ = initialize_task(ctx.as_context_mut())?;

    let (name_ptr, name_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"new task")?;
    WorkflowFunctions::spawn_task(ctx.as_context_mut(), name_ptr, name_len, 1)?;

    let poll_res = WorkflowFunctions::poll_task_completion(ctx.as_context_mut(), 1, POLL_CX)?;
    assert_eq!(poll_res, -1); // Poll::Pending
    Ok(Poll::Pending)
}

#[test]
fn spawning_and_cancelling_task() {
    let abort_task_and_complete: MockPollFn = |mut ctx| {
        WorkflowFunctions::schedule_task_abortion(ctx.as_context_mut(), 1)?;
        Ok(Poll::Ready(()))
    };
    let poll_fns = Answers::from_values([initialize_and_spawn_task, abort_task_and_complete]);
    let mock_guard = ExportsMock::prepare(poll_fns);

    let engine = WorkflowEngine::default();
    let module = WorkflowModule::<()>::new(&engine, ExportsMock::MOCK_MODULE_BYTES).unwrap();
    let mut inputs = InputsBuilder::new(module.interface());
    inputs.insert("inputs", b"test_input".to_vec());
    let receipt = Workflow::new(&module, inputs.build()).unwrap();

    assert_eq!(receipt.executions().len(), 2);
    let task_spawned = receipt.executions()[0].events.iter().any(|event| {
        matches!(
            event,
            Event::Resource(res) if res.resource_id == ResourceId::Task(1)
                && res.kind == ResourceEventKind::Created
        )
    });
    assert!(task_spawned, "{:?}", receipt.executions());
    assert_matches!(
        receipt.executions()[1].function,
        ExecutedFunction::Task {
            task_id: 1,
            wake_up_cause: WakeUpCause::Function(_),
            ..
        }
    );

    let mut workflow = receipt.into_inner();
    let new_task = workflow.task(1).unwrap();
    assert_eq!(new_task.name(), "new task");
    assert_eq!(new_task.spawned_by(), Some(0));
    assert_matches!(new_task.result(), Poll::Pending);

    // Push the message in order to tick the main task.
    workflow
        .push_inbound_message("orders", b"order #1".to_vec())
        .unwrap();
    let receipt = workflow.tick().unwrap();

    assert_matches!(
        receipt.executions()[1].function,
        ExecutedFunction::Task { task_id: 0, .. }
    );
    assert!(receipt.executions().iter().any(|execution| matches!(
        execution.function,
        ExecutedFunction::TaskDrop { task_id: 1 }
    )));
    assert!(receipt.executions().iter().any(|execution| matches!(
        execution.function,
        ExecutedFunction::TaskDrop { task_id: 0 }
    )));
    let task_aborted = receipt.executions()[1].events.iter().any(|event| {
        matches!(
            event,
            Event::Resource(res) if res.resource_id == ResourceId::Task(1)
                && res.kind == ResourceEventKind::Dropped
        )
    });
    assert!(task_aborted, "{:?}", receipt.executions());

    assert_matches!(
        workflow.task(1).unwrap().result(),
        Poll::Ready(Err(JoinError::Aborted))
    );
    assert!(workflow.is_finished());
    assert!(mock_guard.into_inner().dropped_tasks.contains(&1));
}

#[test]
fn rolling_back_task_spawning() {
    let spawn_task_and_trap: MockPollFn = |mut ctx| {
        let (name_ptr, name_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"new task")?;
        WorkflowFunctions::spawn_task(ctx.as_context_mut(), name_ptr, name_len, 1)?;
        Err(Trap::new("boom"))
    };
    let poll_fns = Answers::from_values([initialize_task, spawn_task_and_trap]);
    let _guard = ExportsMock::prepare(poll_fns);

    let engine = WorkflowEngine::default();
    let module = WorkflowModule::<()>::new(&engine, ExportsMock::MOCK_MODULE_BYTES).unwrap();
    let mut inputs = InputsBuilder::new(module.interface());
    inputs.insert("inputs", b"test_input".to_vec());
    let mut workflow = Workflow::new(&module, inputs.build()).unwrap().into_inner();

    // Push the message in order to tick the main task.
    workflow
        .push_inbound_message("orders", b"order #1".to_vec())
        .unwrap();
    let err = workflow.tick().unwrap_err();
    assert_matches!(
        err.receipt().executions(),
        [
            Execution {
                function: ExecutedFunction::Waker { .. },
                ..
            },
            Execution {
                function: ExecutedFunction::Task { task_id: 0, .. },
                ..
            },
        ]
    );
    let err_message = err.trap().to_string();
    assert!(err_message.contains("boom"), "{}", err_message);

    assert!(workflow.task(1).is_none());
}

#[test]
fn rolling_back_task_abort() {
    let abort_task_and_trap: MockPollFn = |mut ctx| {
        WorkflowFunctions::schedule_task_abortion(ctx.as_context_mut(), 1)?;
        Err(Trap::new("boom"))
    };
    let poll_fns = Answers::from_values([initialize_and_spawn_task, abort_task_and_trap]);
    let mock_guard = ExportsMock::prepare(poll_fns);

    let engine = WorkflowEngine::default();
    let module = WorkflowModule::<()>::new(&engine, ExportsMock::MOCK_MODULE_BYTES).unwrap();
    let mut inputs = InputsBuilder::new(module.interface());
    inputs.insert("inputs", b"test_input".to_vec());
    let mut workflow = Workflow::new(&module, inputs.build()).unwrap().into_inner();

    // Push the message in order to tick the main task.
    workflow
        .push_inbound_message("orders", b"order #1".to_vec())
        .unwrap();
    let err = workflow.tick().unwrap_err();
    assert_eq!(err.receipt().executions().len(), 2);
    let err_message = err.trap().to_string();
    assert!(err_message.contains("boom"), "{}", err_message);

    assert_matches!(workflow.task(1).unwrap().result(), Poll::Pending);
    assert!(mock_guard.into_inner().dropped_tasks.is_empty());
}

#[test]
fn rolling_back_emitting_messages_on_trap() {
    let emit_message_and_trap: MockPollFn = |mut ctx| {
        let (ptr, len) = StaticStr::Traces.ptr_and_len();
        let poll_res =
            WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), ptr, len, POLL_CX)
                .unwrap();
        assert_eq!(poll_res, 0); // Poll::Ready

        let (trace_ptr, trace_len) = WasmAllocator::new(ctx.as_context_mut())
            .copy_to_wasm(b"trace #1")
            .unwrap();
        WorkflowFunctions::start_send(ctx.as_context_mut(), ptr, len, trace_ptr, trace_len)
            .unwrap();

        Err(Trap::new("boom"))
    };
    let poll_fns = Answers::from_values([initialize_task, emit_message_and_trap]);
    let _guard = ExportsMock::prepare(poll_fns);

    let engine = WorkflowEngine::default();
    let module = WorkflowModule::<()>::new(&engine, ExportsMock::MOCK_MODULE_BYTES).unwrap();
    let mut inputs = InputsBuilder::new(module.interface());
    inputs.insert("inputs", b"test_input".to_vec());
    let mut workflow = Workflow::new(&module, inputs.build()).unwrap().into_inner();

    // Push the message in order to tick the main task.
    workflow
        .push_inbound_message("orders", b"order #1".to_vec())
        .unwrap();
    workflow.tick().unwrap_err();

    let (_, messages) = workflow.take_outbound_messages("traces");
    assert!(messages.is_empty());
}

#[test]
fn rolling_back_placing_waker_on_trap() {
    let flush_event_and_trap: MockPollFn = |mut ctx| {
        emit_event_and_flush(&mut ctx);
        Err(Trap::new("boom"))
    };
    let poll_fns = Answers::from_values([initialize_task, flush_event_and_trap]);
    let _guard = ExportsMock::prepare(poll_fns);

    let engine = WorkflowEngine::default();
    let module = WorkflowModule::<()>::new(&engine, ExportsMock::MOCK_MODULE_BYTES).unwrap();
    let mut inputs = InputsBuilder::new(module.interface());
    inputs.insert("inputs", b"test_input".to_vec());
    let mut workflow = Workflow::new(&module, inputs.build()).unwrap().into_inner();

    // Push the message in order to tick the main task.
    workflow
        .push_inbound_message("orders", b"order #1".to_vec())
        .unwrap();
    workflow.tick().unwrap_err();

    let wakers: Vec<_> = workflow.store.data().outbound_channel_wakers().collect();
    assert!(wakers.is_empty(), "{:?}", wakers);
}
