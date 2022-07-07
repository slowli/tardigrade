use assert_matches::assert_matches;
use mimicry::Answers;
use wasmtime::StoreContextMut;

use std::collections::HashSet;

use super::*;
use crate::{
    data::WorkflowFunctions,
    module::{ExportsMock, MockPollFn},
    receipt::{ChannelEvent, ChannelEventKind},
    utils::{copy_string_from_wasm, WasmAllocator},
    WorkflowEngine,
};
use tardigrade::workflow::InputsBuilder;
use tardigrade_shared::abi::AllocateBytes;

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
    let poll_cx = 123;
    let poll_res =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), ptr, len, poll_cx).unwrap();
    assert_eq!(poll_res, -1); // Poll::Pending

    Ok(Poll::Pending)
}

#[allow(clippy::unnecessary_wraps)] // more convenient for use with mock `Answers`
fn consume_message(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    // Poll the channel again, since we yielded on this previously
    let (ptr, len) = StaticStr::Orders.ptr_and_len();
    let poll_cx = 1234;
    let poll_res =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), ptr, len, poll_cx).unwrap();

    let (msg_ptr, msg_len) = decode_message_poll(poll_res);
    let memory = ctx.data().exports().memory;
    let message = copy_string_from_wasm(&ctx, &memory, msg_ptr, msg_len).unwrap();
    assert_eq!(message, "order #1");

    // Emit a trace (shouldn't block the task).
    let (ptr, len) = StaticStr::Traces.ptr_and_len();
    let poll_res =
        WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), ptr, len, poll_cx).unwrap();
    assert_eq!(poll_res, 0); // Poll::Ready

    let (trace_ptr, trace_len) = WasmAllocator::new(ctx.as_context_mut())
        .copy_to_wasm(b"trace #1")
        .unwrap();
    WorkflowFunctions::start_send(ctx.as_context_mut(), ptr, len, trace_ptr, trace_len).unwrap();

    // Emit an event and flush (should block).
    let (ptr, len) = StaticStr::Events.ptr_and_len();
    let poll_res =
        WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), ptr, len, poll_cx).unwrap();
    assert_eq!(poll_res, 0); // Poll::Ready

    let (event_ptr, event_len) = WasmAllocator::new(ctx.as_context_mut())
        .copy_to_wasm(b"event #1")
        .unwrap();
    WorkflowFunctions::start_send(ctx.as_context_mut(), ptr, len, event_ptr, event_len).unwrap();

    let poll_res =
        WorkflowFunctions::poll_flush_for_sender(ctx.as_context_mut(), ptr, len, poll_cx).unwrap();
    assert_eq!(poll_res, -1); // Poll::Pending

    // Check that sending events is no longer possible.
    let poll_res =
        WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), ptr, len, poll_cx).unwrap();
    assert_eq!(poll_res, -1); // Poll::Pending

    // Check that sending traces is still possible
    let (ptr, len) = StaticStr::Traces.ptr_and_len();
    let poll_res =
        WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), ptr, len, poll_cx).unwrap();
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
            kind: ChannelEventKind::InboundChannelPolled,
            channel_name,
            result: Poll::Pending
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
        &events[0..4],
        [
            ChannelEvent {
                kind: ChannelEventKind::InboundChannelPolled,
                channel_name: orders,
                result: Poll::Ready(()),
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelReady,
                channel_name: traces,
                result: Poll::Ready(()),
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelReady,
                channel_name: events,
                result: Poll::Ready(()),
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelFlushed,
                channel_name: events2,
                result: Poll::Pending,
            },
        ] if orders == "orders" && traces == "traces" && events == "events" && events2 == "events"
    );
}
