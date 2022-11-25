//! Mid-level tests for instantiating and managing workflows.

use anyhow::anyhow;
use assert_matches::assert_matches;
use futures::{future, stream, FutureExt};
use mimicry::Answers;
use wasmtime::StoreContextMut;

use std::{
    collections::{HashMap, HashSet},
    task::Poll,
};

mod spawn;

use super::*;
use crate::{
    mock_scheduler::MockScheduler,
    module::{ExportsMock, MockPollFn, WorkflowEngine, WorkflowModule},
    receipt::{
        ChannelEvent, ChannelEventKind, Event, ExecutedFunction, Execution, ExecutionError,
        Receipt, ResourceEventKind, ResourceId,
    },
    utils::{copy_string_from_wasm, decode_string, WasmAllocator},
    workflow::{PersistedWorkflow, Workflow},
};
use tardigrade::{
    abi::AllocateBytes, interface::Interface, spawn::ChannelsConfig, task::JoinError, ChannelId,
};

const POLL_CX: WasmContextPtr = 1_234;
const ERROR_PTR: u32 = 1_024; // enough to not intersect with "real" memory

fn answer_main_task(mut poll_fns: Answers<MockPollFn>) -> Answers<MockPollFn, TaskId> {
    Answers::from_fn(move |&task_id| {
        if task_id == 0 {
            poll_fns.next_for(())
        } else {
            (|_| Ok(Poll::Pending)) as MockPollFn
        }
    })
}

fn initialize_task(mut ctx: StoreContextMut<'_, WorkflowData>) -> anyhow::Result<Poll<()>> {
    // Emulate basic task startup: getting the inbound channel
    let (ptr, len) = WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"orders")?;
    let orders = WorkflowFunctions::get_receiver(ctx.as_context_mut(), None, ptr, len, ERROR_PTR)?;

    // ...then polling this channel
    let poll_res =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
    assert_eq!(poll_res, -1); // Poll::Pending

    Ok(Poll::Pending)
}

fn emit_event_and_flush(ctx: &mut StoreContextMut<'_, WorkflowData>) -> anyhow::Result<()> {
    let events = Some(ctx.data_mut().outbound_channel_ref(None, "events"));

    let poll_res =
        WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), events.clone(), POLL_CX)?;
    assert_eq!(poll_res, 0); // Poll::Ready

    let (event_ptr, event_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"event #1")?;
    WorkflowFunctions::start_send(ctx.as_context_mut(), events.clone(), event_ptr, event_len)?;

    let poll_res = WorkflowFunctions::poll_flush_for_sender(ctx.as_context_mut(), events, POLL_CX)?;
    assert_eq!(poll_res, -1); // Poll::Pending
    Ok(())
}

fn consume_message(mut ctx: StoreContextMut<'_, WorkflowData>) -> anyhow::Result<Poll<()>> {
    let orders = Some(ctx.data().inbound_channel_ref(None, "orders"));
    let events = Some(ctx.data_mut().outbound_channel_ref(None, "events"));
    let traces = Some(ctx.data_mut().outbound_channel_ref(None, "traces"));

    // Poll the channel again, since we yielded on this previously
    let poll_res =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;

    let (msg_ptr, msg_len) = decode_string(poll_res);
    let memory = ctx.data().exports().memory;
    let message = copy_string_from_wasm(&ctx, &memory, msg_ptr, msg_len)?;
    assert_eq!(message, "order #1");

    // Emit a trace (shouldn't block the task).
    let poll_res =
        WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), traces.clone(), POLL_CX)?;
    assert_eq!(poll_res, 0); // Poll::Ready

    let (trace_ptr, trace_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"trace #1")?;
    WorkflowFunctions::start_send(ctx.as_context_mut(), traces.clone(), trace_ptr, trace_len)?;

    emit_event_and_flush(&mut ctx)?;
    // Check that sending events is no longer possible.
    let poll_res = WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), events, POLL_CX)?;
    assert_eq!(poll_res, -1); // Poll::Pending

    // Check that sending traces is still possible
    let poll_res = WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), traces, POLL_CX)?;
    assert_eq!(poll_res, 0); // Poll::Ready

    Ok(Poll::Pending)
}

fn mock_channel_ids(interface: &Interface, next_channel_id: &mut ChannelId) -> ChannelIds {
    let config = ChannelsConfig::from_interface(interface);
    let new_channel_ids = stream::unfold(next_channel_id, |next_channel_id| {
        let id = *next_channel_id;
        *next_channel_id += 1;
        future::ready(Some((id, next_channel_id)))
    });
    ChannelIds::new(config, new_channel_ids)
        .now_or_never()
        .unwrap()
}

fn create_workflow(services: Services<'_>) -> (Receipt, Workflow<'_>) {
    let engine = WorkflowEngine::default();
    let spawner = WorkflowModule::new(&engine, ExportsMock::MOCK_MODULE_BYTES)
        .unwrap()
        .for_untyped_workflow("TestWorkflow")
        .unwrap();

    let channel_ids = mock_channel_ids(spawner.interface(), &mut 1);
    let mut workflow = spawner
        .spawn(b"test_input".to_vec(), channel_ids, services)
        .unwrap();
    (workflow.initialize().unwrap(), workflow)
}

fn restore_workflow(persisted: PersistedWorkflow, services: Services<'_>) -> Workflow<'_> {
    let engine = WorkflowEngine::default();
    let spawner = WorkflowModule::new(&engine, ExportsMock::MOCK_MODULE_BYTES)
        .unwrap()
        .for_untyped_workflow("TestWorkflow")
        .unwrap();
    persisted.restore(&spawner, services).unwrap()
}

#[test]
fn starting_workflow() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let exports_guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let (receipt, workflow) = poll_fn_sx.send(initialize_task).scope(|| {
        create_workflow(Services {
            clock: &clock,
            workflows: None,
            tracer: None,
        })
    });
    let exports_mock = exports_guard.into_inner();
    assert!(exports_mock.exports_created);

    assert_eq!(receipt.executions().len(), 2);
    let execution = &receipt.executions()[1];
    assert_matches!(
        &execution.function,
        ExecutedFunction::Task {
            task_id: 0,
            wake_up_cause: WakeUpCause::Spawned,
        }
    );
    assert_eq!(execution.events.len(), 1);

    let mapping = workflow.data().persisted.channels();
    let orders_id = mapping.receiver_id("orders").unwrap();
    assert_matches!(
        &execution.events[0],
        Event::Channel(ChannelEvent {
            kind: ChannelEventKind::InboundChannelPolled { result: Poll::Pending },
            channel_id
        }) if *channel_id == orders_id
    );

    workflow.persist();
}

fn push_message_and_tick(workflow: &mut Workflow<'_>) -> Result<Receipt, ExecutionError> {
    let mapping = workflow.data().persisted.channels();
    let orders_id = mapping.receiver_id("orders").unwrap();
    workflow
        .data_mut()
        .persisted
        .push_inbound_message(orders_id, b"order #1".to_vec())
        .unwrap();
    workflow.tick()
}

#[test]
fn receiving_inbound_message() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let exports_guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let (_, mut workflow) = poll_fn_sx.send(initialize_task).scope(|| {
        create_workflow(Services {
            clock: &clock,
            workflows: None,
            tracer: None,
        })
    });

    let receipt = poll_fn_sx
        .send(consume_message)
        .scope(|| push_message_and_tick(&mut workflow))
        .unwrap();

    let exports = exports_guard.into_inner();
    assert_eq!(exports.next_waker, 3); // 1 waker for first execution and 2 for the second one
    assert_eq!(exports.consumed_wakers.len(), 1);
    assert!(exports.consumed_wakers.contains(&0));

    assert_inbound_message_receipt(&workflow, &receipt);

    let messages = workflow.data_mut().drain_messages();
    let mapping = workflow.data().persisted.channels();
    let traces_id = mapping.sender_id("traces").unwrap();
    assert_eq!(messages[&traces_id].len(), 1);
    assert_eq!(messages[&traces_id][0].as_ref(), b"trace #1");
    let events_id = mapping.sender_id("events").unwrap();
    assert_eq!(messages[&events_id].len(), 1);
    assert_eq!(messages[&events_id][0].as_ref(), b"event #1");

    let (waker_ids, causes): (HashSet<_>, Vec<_>) = workflow.data_mut().take_wakers().unzip();
    assert_eq!(waker_ids.len(), 2);
    assert_eq!(causes.len(), 2);
    for cause in &causes {
        assert_matches!(
            cause,
            WakeUpCause::Flush { channel_id, message_indexes }
                if *channel_id == events_id && *message_indexes == (0..1)
        );
    }
}

fn assert_inbound_message_receipt(workflow: &Workflow<'_>, receipt: &Receipt) {
    let mapping = workflow.data().persisted.channels();
    let orders_id = mapping.receiver_id("orders").unwrap();
    let events_id = mapping.sender_id("events").unwrap();
    let traces_id = mapping.sender_id("traces").unwrap();

    assert_eq!(receipt.executions().len(), 2);
    assert_matches!(
        &receipt.executions()[0],
        Execution {
            function: ExecutedFunction::Waker {
                waker_id: 0,
                wake_up_cause: WakeUpCause::InboundMessage {
                    channel_id,
                    message_index: 0,
                }
            },
            events,
            ..
        } if *channel_id == orders_id && events.is_empty()
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
                channel_id: orders,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelReady {
                    result: Poll::Ready(Ok(())),
                },
                channel_id: traces,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundMessageSent { .. },
                channel_id: traces2,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelReady {
                    result: Poll::Ready(Ok(())),
                },
                channel_id: events,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundMessageSent { .. },
                channel_id: events2,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelFlushed { result: Poll::Pending },
                channel_id: events3,
            },
        ] if *orders == orders_id && *traces == traces_id && *traces2 == traces_id
            && *events == events_id && *events2 == events_id && *events3 == events_id
    );
}

#[test]
fn trap_when_starting_workflow() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);

    let engine = WorkflowEngine::default();
    let module = WorkflowModule::new(&engine, ExportsMock::MOCK_MODULE_BYTES).unwrap();
    let spawner = module.for_untyped_workflow("TestWorkflow").unwrap();
    let channel_ids = mock_channel_ids(spawner.interface(), &mut 1);
    let services = Services {
        clock: &MockScheduler::default(),
        workflows: None,
        tracer: None,
    };
    let mut workflow = spawner
        .spawn(b"test_input".to_vec(), channel_ids, services)
        .unwrap();
    let err = poll_fn_sx
        .send(|_| Err(anyhow!("boom")))
        .scope(|| workflow.initialize())
        .unwrap_err()
        .to_string();

    assert!(err.contains("failed while polling task 0"), "{}", err);
}

fn initialize_and_spawn_task(
    mut ctx: StoreContextMut<'_, WorkflowData>,
) -> anyhow::Result<Poll<()>> {
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
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let mock_guard = ExportsMock::prepare(answer_main_task(poll_fns));
    let clock = MockScheduler::default();
    let (receipt, mut workflow) = poll_fn_sx.send(initialize_and_spawn_task).scope(|| {
        create_workflow(Services {
            clock: &clock,
            workflows: None,
            tracer: None,
        })
    });

    assert_eq!(receipt.executions().len(), 3);
    let task_spawned = receipt.executions()[1].events.iter().any(|event| {
        matches!(
            event,
            Event::Resource(res) if res.resource_id == ResourceId::Task(1)
                && res.kind == ResourceEventKind::Created
        )
    });
    assert!(task_spawned, "{:?}", receipt.executions());
    assert_matches!(
        receipt.executions()[2].function,
        ExecutedFunction::Task {
            task_id: 1,
            wake_up_cause: WakeUpCause::Function { task_id: Some(0) },
            ..
        }
    );

    let new_task = workflow.data().persisted.task(1).unwrap();
    assert_eq!(new_task.name(), "new task");
    assert_eq!(new_task.spawned_by(), Some(0));
    assert_matches!(new_task.result(), Poll::Pending);

    let abort_task_and_complete: MockPollFn = |mut ctx| {
        WorkflowFunctions::schedule_task_abortion(ctx.as_context_mut(), 1)?;
        Ok(Poll::Ready(()))
    };
    let receipt = poll_fn_sx
        .send(abort_task_and_complete)
        .scope(|| push_message_and_tick(&mut workflow))
        .unwrap();

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
        workflow.data().persisted.task(1).unwrap().result(),
        Poll::Ready(Err(JoinError::Aborted))
    );
    assert!(workflow
        .data()
        .persisted
        .tasks()
        .all(|(_, state)| state.result().is_ready()));
    assert!(mock_guard.into_inner().dropped_tasks.contains(&1));
}

#[test]
fn workflow_terminates_after_main_task_completion() {
    let spawn_task_and_complete: MockPollFn = |ctx| {
        let _ = initialize_and_spawn_task(ctx)?;
        Ok(Poll::Ready(()))
    };
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let (receipt, workflow) = poll_fn_sx.send(spawn_task_and_complete).scope(|| {
        create_workflow(Services {
            clock: &clock,
            workflows: None,
            tracer: None,
        })
    });

    let mut executions = receipt.executions().iter().rev();
    let last_execution = executions
        .find(|execution| matches!(execution.function, ExecutedFunction::Task { .. }))
        .unwrap();
    assert_matches!(
        last_execution.function,
        ExecutedFunction::Task {
            task_id: 0,
            wake_up_cause: WakeUpCause::Spawned
        }
    );
    let task_result = last_execution.task_result.as_ref().unwrap();
    task_result.as_ref().unwrap();

    let workflow = workflow.persist();
    assert_matches!(workflow.result(), Poll::Ready(Ok(())));
}

#[test]
fn rolling_back_task_spawning() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let (_, mut workflow) = poll_fn_sx.send(initialize_task).scope(|| {
        create_workflow(Services {
            clock: &clock,
            workflows: None,
            tracer: None,
        })
    });

    let spawn_task_and_trap: MockPollFn = |mut ctx| {
        let (name_ptr, name_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"new task")?;
        WorkflowFunctions::spawn_task(ctx.as_context_mut(), name_ptr, name_len, 1)?;
        Err(anyhow!("boom"))
    };
    let err = poll_fn_sx
        .send(spawn_task_and_trap)
        .scope(|| push_message_and_tick(&mut workflow))
        .unwrap_err();

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
    assert!(workflow.data().persisted.task(1).is_none());
}

#[test]
fn rolling_back_task_abort() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let mock_guard = ExportsMock::prepare(answer_main_task(poll_fns));
    let clock = MockScheduler::default();

    let (_, mut workflow) = poll_fn_sx.send(initialize_and_spawn_task).scope(|| {
        create_workflow(Services {
            clock: &clock,
            workflows: None,
            tracer: None,
        })
    });

    // Push the message in order to tick the main task.
    let abort_task_and_trap: MockPollFn = |mut ctx| {
        WorkflowFunctions::schedule_task_abortion(ctx.as_context_mut(), 1)?;
        Err(anyhow!("boom"))
    };
    let err = poll_fn_sx
        .send(abort_task_and_trap)
        .scope(|| push_message_and_tick(&mut workflow))
        .unwrap_err();
    assert_eq!(err.receipt().executions().len(), 2);
    let err_message = err.trap().to_string();
    assert!(err_message.contains("boom"), "{}", err_message);

    assert_matches!(
        workflow.data().persisted.task(1).unwrap().result(),
        Poll::Pending
    );
    assert!(mock_guard.into_inner().dropped_tasks.is_empty());
}

#[test]
fn rolling_back_emitting_messages_on_trap() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let (_, mut workflow) = poll_fn_sx.send(initialize_task).scope(|| {
        create_workflow(Services {
            clock: &clock,
            workflows: None,
            tracer: None,
        })
    });

    let emit_message_and_trap: MockPollFn = |mut ctx| {
        let traces = Some(ctx.data_mut().outbound_channel_ref(None, "traces"));
        let poll_res = WorkflowFunctions::poll_ready_for_sender(
            ctx.as_context_mut(),
            traces.clone(),
            POLL_CX,
        )?;
        assert_eq!(poll_res, 0); // Poll::Ready

        let (trace_ptr, trace_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"trace #1")?;
        WorkflowFunctions::start_send(ctx.as_context_mut(), traces, trace_ptr, trace_len)?;

        Err(anyhow!("boom"))
    };
    poll_fn_sx
        .send(emit_message_and_trap)
        .scope(|| push_message_and_tick(&mut workflow))
        .unwrap_err();

    let messages = workflow.data_mut().drain_messages();
    assert!(messages.is_empty(), "{messages:?}");
}

#[test]
fn rolling_back_placing_waker_on_trap() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let (_, mut workflow) = poll_fn_sx.send(initialize_task).scope(|| {
        create_workflow(Services {
            clock: &clock,
            workflows: None,
            tracer: None,
        })
    });

    // Push the message in order to tick the main task.
    let flush_event_and_trap: MockPollFn = |mut ctx| {
        emit_event_and_flush(&mut ctx)?;
        Err(anyhow!("boom"))
    };
    poll_fn_sx
        .send(flush_event_and_trap)
        .scope(|| push_message_and_tick(&mut workflow))
        .unwrap_err();

    let outbound_channels = workflow.data().persisted.channels.outbound.values();
    let wakers: Vec<_> = outbound_channels
        .flat_map(|state| state.wakes_on_flush.iter().copied())
        .collect();
    assert!(wakers.is_empty(), "{:?}", wakers);
}

#[test]
fn timers_basics() {
    let create_timers: MockPollFn = |mut ctx| {
        let ts = WorkflowFunctions::current_timestamp(ctx.as_context_mut());
        let timer_id = WorkflowFunctions::create_timer(ctx.as_context_mut(), ts - 100)?;
        assert_eq!(timer_id, 0);
        // ^ implementation detail, but it's used in code later
        let result = WorkflowFunctions::poll_timer(ctx.as_context_mut(), timer_id, POLL_CX)?;
        assert_eq!(result, ts);

        let timer_id = WorkflowFunctions::create_timer(ctx.as_context_mut(), ts + 100)?;
        assert_eq!(timer_id, 1);
        let result = WorkflowFunctions::poll_timer(ctx.as_context_mut(), timer_id, POLL_CX)?;
        assert_eq!(result, -1); // Poll::Pending

        Ok(Poll::Pending)
    };
    let poll_timers_and_drop: MockPollFn = |mut ctx| {
        let result = WorkflowFunctions::poll_timer(ctx.as_context_mut(), 0, POLL_CX)?;
        assert_ne!(result, -1);
        WorkflowFunctions::drop_timer(ctx.as_context_mut(), 0)?;

        let result = WorkflowFunctions::poll_timer(ctx.as_context_mut(), 1, POLL_CX)?;
        let ts = WorkflowFunctions::current_timestamp(ctx.as_context_mut());
        assert_eq!(result, ts);
        WorkflowFunctions::drop_timer(ctx.as_context_mut(), 1)?;

        Ok(Poll::Pending)
    };

    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let scheduler = MockScheduler::default();
    let (_, workflow) = poll_fn_sx.send(create_timers).scope(|| {
        create_workflow(Services {
            clock: &scheduler,
            workflows: None,
            tracer: None,
        })
    });

    let timers: HashMap<_, _> = workflow.data().persisted.timers.iter().collect();
    assert_eq!(timers.len(), 2, "{:?}", timers);
    assert!(timers[&0].completed_at().is_some());
    assert!(timers[&1].completed_at().is_none());

    scheduler.set_now(scheduler.now() + chrono::Duration::seconds(1));
    let mut persisted = workflow.persist();
    persisted.set_current_time(scheduler.now());
    let mut workflow = restore_workflow(
        persisted,
        Services {
            clock: &scheduler,
            workflows: None,
            tracer: None,
        },
    );
    poll_fn_sx
        .send(poll_timers_and_drop)
        .scope(|| workflow.tick())
        .unwrap();

    let timers: HashMap<_, _> = workflow.data().persisted.timers.iter().collect();
    assert!(timers.is_empty(), "{:?}", timers); // timers are cleared on drop
}

#[test]
fn dropping_inbound_channel_in_workflow() {
    let drop_channel: MockPollFn = |mut ctx| {
        let _ = initialize_task(ctx.as_context_mut())?;
        let orders = ctx.data().inbound_channel_ref(None, "orders");
        WorkflowFunctions::drop_ref(ctx, Some(orders))?;
        Ok(Poll::Pending)
    };

    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    poll_fn_sx.send(drop_channel).scope(|| {
        create_workflow(Services {
            clock: &clock,
            workflows: None,
            tracer: None,
        })
    });

    let mock = guard.into_inner();
    assert_eq!(mock.consumed_wakers.len(), 1, "{:?}", mock.consumed_wakers);
}

fn report_task_error(
    mut ctx: StoreContextMut<'_, WorkflowData>,
    line: u32,
    column: u32,
    message: &str,
) -> anyhow::Result<()> {
    let (message_ptr, message_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(message.as_bytes())?;
    let (filename_ptr, filename_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"/build/src/test.rs")?;
    WorkflowFunctions::report_task_error(
        ctx,
        message_ptr,
        message_len,
        filename_ptr,
        filename_len,
        line,
        column,
    )
}

pub(crate) fn complete_task_with_error(
    ctx: StoreContextMut<'_, WorkflowData>,
) -> anyhow::Result<Poll<()>> {
    report_task_error(ctx, 42, 1, "error message")?;
    Ok(Poll::Ready(()))
}

#[test]
fn completing_main_task_with_error() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let (receipt, workflow) = poll_fn_sx.send(complete_task_with_error).scope(|| {
        create_workflow(Services {
            clock: &clock,
            workflows: None,
            tracer: None,
        })
    });

    let task_result = receipt
        .executions()
        .iter()
        .find_map(|execution| execution.task_result.as_ref());
    assert_matches!(
        task_result.unwrap(),
        Err(err) if err.location().filename == "/build/src/test.rs"
    );

    let tasks = &workflow.data().persisted.tasks;
    assert_eq!(tasks.len(), 1);
    assert_task_error(&tasks[&0], false);
}

fn assert_task_error(task_state: &TaskState, has_context: bool) {
    let err = match task_state.result() {
        Poll::Ready(Err(JoinError::Err(err))) => err,
        other => panic!("unexpected task result: {:?}", other),
    };

    assert_eq!(err.cause().to_string(), "error message");
    assert_eq!(err.location().filename, "/build/src/test.rs");
    assert_eq!(err.location().line, 42);
    assert_eq!(err.location().column, 1);

    if has_context {
        assert_eq!(err.contexts().len(), 1);
        let context = &err.contexts()[0];
        assert_eq!(context.message(), "context message");
        assert_eq!(context.location().filename, "/build/src/test.rs");
        assert_eq!(context.location().line, 10);
        assert_eq!(context.location().column, 4);
    }
}

#[test]
fn completing_main_task_with_compound_error() {
    let complete_task_with_compound_error: MockPollFn = |mut ctx| {
        report_task_error(ctx.as_context_mut(), 42, 1, "error message")?;
        report_task_error(ctx, 10, 4, "context message")?;
        Ok(Poll::Ready(()))
    };

    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let (_, workflow) = poll_fn_sx
        .send(complete_task_with_compound_error)
        .scope(|| {
            create_workflow(Services {
                clock: &clock,
                workflows: None,
                tracer: None,
            })
        });

    let tasks = &workflow.data().persisted.tasks;
    assert_eq!(tasks.len(), 1);
    assert_task_error(&tasks[&0], true);
}

#[test]
fn completing_subtask_with_error() {
    let check_subtask_completion: MockPollFn = |mut ctx| {
        let poll_res = WorkflowFunctions::poll_task_completion(ctx.as_context_mut(), 1, POLL_CX)?;
        assert_eq!(poll_res, -2); // Poll::Ready(Ok(()))
        Ok(Poll::Pending)
    };
    let mut main_task_polls = Answers::from_values([
        initialize_and_spawn_task as MockPollFn,
        check_subtask_completion,
    ]);
    let poll_fns = Answers::from_fn(move |&task_id| match task_id {
        0 => main_task_polls.next_for(()),
        1 => complete_task_with_error,
        _ => unreachable!(),
    });

    let _guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let (receipt, workflow) = create_workflow(Services {
        clock: &clock,
        workflows: None,
        tracer: None,
    });

    let mut executions_by_task = HashMap::<_, usize>::new();
    for execution in receipt.executions() {
        if let Some(task_id) = execution.function.task_id() {
            *executions_by_task.entry(task_id).or_default() += 1;
        }
    }
    assert_eq!(executions_by_task[&0], 2);
    assert_eq!(executions_by_task[&1], 1);

    let task_result = receipt
        .executions()
        .iter()
        .find_map(|execution| execution.task_result.as_ref());
    assert_matches!(
        task_result.unwrap(),
        Err(err) if err.location().filename == "/build/src/test.rs"
    );

    let tasks = &workflow.data().persisted.tasks;
    assert_eq!(tasks.len(), 2);
    assert_task_error(&tasks[&1], false);
    let main_task = &tasks[&0];
    assert_matches!(main_task.result(), Poll::Pending);
}
