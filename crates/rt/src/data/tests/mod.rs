//! Mid-level tests for instantiating and managing workflows.

use anyhow::anyhow;
use assert_matches::assert_matches;
use chrono::{DateTime, Utc};

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    task::Poll,
};

mod spawn;

use super::*;
use crate::{
    backends::MockScheduler,
    engine::{
        AsWorkflowData, DefineWorkflow, MockAnswers, MockDefinition, MockInstance, MockPollFn,
    },
    receipt::{
        ChannelEvent, ChannelEventKind, Event, ExecutedFunction, Execution, ExecutionError,
        Receipt, ResourceEventKind, ResourceId,
    },
    workflow::{PersistedWorkflow, Workflow},
};
use tardigrade::{
    abi::PollMessage, interface::Interface, task::JoinError, ChannelId, TimerDefinition,
};

fn extract_message(poll_res: PollMessage) -> Vec<u8> {
    match poll_res {
        Poll::Ready(Some(message)) => message,
        other => panic!("unexpected poll result: {other:?}"),
    }
}

fn extract_timestamp(poll_res: Poll<DateTime<Utc>>) -> DateTime<Utc> {
    match poll_res {
        Poll::Ready(ts) => ts,
        Poll::Pending => panic!("unexpected poll result: Pending"),
    }
}

fn answer_main_task(mut poll_fns: mimicry::Answers<MockPollFn>) -> MockAnswers {
    MockAnswers::from_fn(move |&task_id| {
        if task_id == 0 {
            poll_fns.next_for(())
        } else {
            (|_| Ok(Poll::Pending)) as MockPollFn
        }
    })
}

fn initialize_task(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    // Emulate basic task startup: getting the channel receiver
    let orders_id = ctx
        .data()
        .persisted
        .channels()
        .channel_id("orders")
        .unwrap();
    // ...then polling this channel
    let mut orders = ctx.data_mut().receiver(orders_id);
    let poll_res = orders.poll_next().into_inner(ctx)?;
    assert_matches!(poll_res, Poll::Pending);

    Ok(Poll::Pending)
}

fn emit_event_and_flush(ctx: &mut MockInstance) -> anyhow::Result<()> {
    let data = ctx.data_mut();
    let events_id = data.persisted.channels().channel_id("events").unwrap();
    let poll_res = data.sender(events_id).poll_ready().into_inner(ctx)?;
    assert_matches!(poll_res, Poll::Ready(Ok(_)));

    let data = ctx.data_mut();
    data.sender(events_id).start_send(b"event #1".to_vec())?;
    let poll_res = data.sender(events_id).poll_flush().into_inner(ctx)?;
    assert_matches!(poll_res, Poll::Pending);
    Ok(())
}

fn consume_message(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let channels = ctx.data().persisted.channels();
    let orders_id = channels.channel_id("orders").unwrap();
    let events_id = channels.channel_id("events").unwrap();
    let traces_id = channels.channel_id("traces").unwrap();

    // Poll the channel again, since we yielded on this previously
    let mut orders = ctx.data_mut().receiver(orders_id);
    let poll_result = orders.poll_next().into_inner(ctx)?;
    assert_eq!(extract_message(poll_result), b"order #1");

    // Emit a trace (shouldn't block the task).
    let mut traces = ctx.data_mut().sender(traces_id);
    let poll_res = traces.poll_ready().into_inner(ctx)?;
    assert_matches!(poll_res, Poll::Ready(Ok(_)));
    let mut traces = ctx.data_mut().sender(traces_id);
    traces.start_send(b"trace #1".to_vec())?;
    emit_event_and_flush(ctx)?;

    // Check that sending events is no longer possible.
    let mut events = ctx.data_mut().sender(events_id);
    let poll_res = events.poll_ready().into_inner(ctx)?;
    assert_matches!(poll_res, Poll::Pending);
    // Check that sending traces is still possible
    let mut traces = ctx.data_mut().sender(traces_id);
    let poll_res = traces.poll_ready().into_inner(ctx)?;
    assert_matches!(poll_res, Poll::Ready(Ok(_)));

    Ok(Poll::Pending)
}

fn mock_channel_ids(interface: &Interface, next_channel_id: &mut ChannelId) -> ChannelIds {
    let ids = interface.handles().map(|(path, spec)| {
        let channel_id = *next_channel_id;
        *next_channel_id += 1;
        let channel_id = spec
            .as_ref()
            .map_sender(|_| channel_id)
            .map_receiver(|_| channel_id);
        (path.to_owned(), channel_id)
    });
    ids.collect()
}

fn create_workflow_with_scheduler(
    poll_fns: MockAnswers,
    scheduler: MockScheduler,
) -> (Receipt, Workflow<MockInstance>) {
    let definition = MockDefinition::new(poll_fns);
    let channel_ids = mock_channel_ids(definition.interface(), &mut 1);
    let mut data = WorkflowData::new(definition.interface(), channel_ids);
    data.set_services(Services {
        clock: Arc::new(scheduler),
        stubs: None,
        tracer: None,
    });
    let args = vec![].into();
    let mut workflow = Workflow::new(&definition, data, Some(args)).unwrap();
    (workflow.initialize().unwrap(), workflow)
}

fn create_workflow(poll_fns: MockAnswers) -> (Receipt, Workflow<MockInstance>) {
    create_workflow_with_scheduler(poll_fns, MockScheduler::default())
}

fn restore_workflow(
    poll_fns: MockAnswers,
    persisted: PersistedWorkflow,
    scheduler: MockScheduler,
) -> Workflow<MockInstance> {
    let definition = MockDefinition::new(poll_fns);
    let services = Services {
        clock: Arc::new(scheduler),
        stubs: None,
        tracer: None,
    };
    persisted.restore(&definition, services).unwrap()
}

#[test]
fn starting_workflow() {
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let (receipt, mut workflow) = poll_fn_sx
        .send(initialize_task)
        .scope(|| create_workflow(poll_fns));

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
    let orders_id = mapping.channel_id("orders").unwrap();
    assert_matches!(
        &execution.events[0],
        Event::Channel(ChannelEvent {
            kind: ChannelEventKind::ReceiverPolled { result: Poll::Pending },
            channel_id
        }) if *channel_id == orders_id
    );

    workflow.persist();
}

fn push_message_and_tick(workflow: &mut Workflow<MockInstance>) -> Result<Receipt, ExecutionError> {
    let mapping = workflow.data().persisted.channels();
    let orders_id = mapping.channel_id("orders").unwrap();
    workflow
        .data_mut()
        .persisted
        .push_message_for_receiver(orders_id, b"order #1".to_vec())
        .unwrap();
    workflow.tick()
}

#[test]
fn receiving_inbound_message() {
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let (_, mut workflow) = poll_fn_sx
        .send(initialize_task)
        .scope(|| create_workflow(poll_fns));

    let receipt = poll_fn_sx
        .send(consume_message)
        .scope(|| push_message_and_tick(&mut workflow))
        .unwrap();
    assert_inbound_message_receipt(&workflow, &receipt);

    let messages = workflow.data_mut().drain_messages();
    let mapping = workflow.data().persisted.channels();
    let traces_id = mapping.channel_id("traces").unwrap();
    assert_eq!(messages[&traces_id].len(), 1);
    assert_eq!(messages[&traces_id][0].as_ref(), b"trace #1");
    let events_id = mapping.channel_id("events").unwrap();
    assert_eq!(messages[&events_id].len(), 1);
    assert_eq!(messages[&events_id][0].as_ref(), b"event #1");

    let wakers = workflow.data_mut().take_wakers();
    let (waker_ids, causes): (HashSet<_>, Vec<_>) = wakers
        .map(|(id, cause)| {
            let WakerOrTask::Waker(id) = id else { unreachable!() };
            (id, cause)
        })
        .unzip();
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

fn assert_inbound_message_receipt(workflow: &Workflow<MockInstance>, receipt: &Receipt) {
    let mapping = workflow.data().persisted.channels();
    let orders_id = mapping.channel_id("orders").unwrap();
    let events_id = mapping.channel_id("events").unwrap();
    let traces_id = mapping.channel_id("traces").unwrap();

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
                kind: ChannelEventKind::ReceiverPolled { result: Poll::Ready(Some(_)) },
                channel_id: orders,
            },
            ChannelEvent {
                kind: ChannelEventKind::SenderReady {
                    result: Poll::Ready(Ok(())),
                },
                channel_id: traces,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundMessageSent { .. },
                channel_id: traces2,
            },
            ChannelEvent {
                kind: ChannelEventKind::SenderReady {
                    result: Poll::Ready(Ok(())),
                },
                channel_id: events,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundMessageSent { .. },
                channel_id: events2,
            },
            ChannelEvent {
                kind: ChannelEventKind::SenderFlushed { result: Poll::Pending },
                channel_id: events3,
            },
        ] if *orders == orders_id && *traces == traces_id && *traces2 == traces_id
            && *events == events_id && *events2 == events_id && *events3 == events_id
    );
}

#[test]
fn trap_when_starting_workflow() {
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let definition = MockDefinition::new(poll_fns);
    let channel_ids = mock_channel_ids(definition.interface(), &mut 1);
    let mut data = WorkflowData::new(definition.interface(), channel_ids);
    data.set_services(Services {
        clock: Arc::new(MockScheduler::default()),
        stubs: None,
        tracer: None,
    });
    let args = vec![].into();
    let mut workflow = Workflow::new(&definition, data, Some(args)).unwrap();
    let err = poll_fn_sx
        .send(|_| Err(anyhow!("boom")))
        .scope(|| workflow.initialize())
        .unwrap_err()
        .to_string();

    assert!(err.contains("failed while polling task 0"), "{err}");
}

fn initialize_and_spawn_task(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let _ = initialize_task(ctx)?;
    ctx.data_mut().spawn_task(1, "new task".to_owned())?;
    let poll_res = ctx.data_mut().task(1).poll_completion().into_inner(ctx)?;
    assert_matches!(poll_res, Poll::Pending);

    Ok(Poll::Pending)
}

#[test]
fn spawning_and_cancelling_task() {
    let (poll_fns, mut poll_fn_sx) = mimicry::Answers::channel();
    let poll_fns = answer_main_task(poll_fns);
    let (receipt, mut workflow) = poll_fn_sx
        .send(initialize_and_spawn_task)
        .scope(|| create_workflow(poll_fns));

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

    let abort_task_and_complete: MockPollFn = |ctx| {
        ctx.data_mut().task(1).schedule_abortion();
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
}

#[test]
fn workflow_terminates_after_main_task_completion() {
    let spawn_task_and_complete: MockPollFn = |ctx| {
        let _ = initialize_and_spawn_task(ctx)?;
        Ok(Poll::Ready(()))
    };
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let (receipt, mut workflow) = poll_fn_sx
        .send(spawn_task_and_complete)
        .scope(|| create_workflow(poll_fns));

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
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let (_, mut workflow) = poll_fn_sx
        .send(initialize_task)
        .scope(|| create_workflow(poll_fns));

    let spawn_task_and_trap: MockPollFn = |ctx| {
        ctx.data_mut().spawn_task(1, "new task".to_owned())?;
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
    assert!(err_message.contains("boom"), "{err_message}");
    assert!(workflow.data().persisted.task(1).is_none());
}

#[test]
fn rolling_back_task_abort() {
    let (poll_fns, mut poll_fn_sx) = mimicry::Answers::channel();
    let poll_fns = answer_main_task(poll_fns);
    let (_, mut workflow) = poll_fn_sx
        .send(initialize_and_spawn_task)
        .scope(|| create_workflow(poll_fns));

    let abort_task_and_trap: MockPollFn = |ctx| {
        ctx.data_mut().task(1).schedule_abortion();
        Err(anyhow!("boom"))
    };
    // Push the message in order to tick the main task.
    let err = poll_fn_sx
        .send(abort_task_and_trap)
        .scope(|| push_message_and_tick(&mut workflow))
        .unwrap_err();
    assert_eq!(err.receipt().executions().len(), 2);
    let err_message = err.trap().to_string();
    assert!(err_message.contains("boom"), "{err_message}");

    assert_matches!(
        workflow.data().persisted.task(1).unwrap().result(),
        Poll::Pending
    );
}

#[test]
fn rolling_back_emitting_messages_on_trap() {
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let (_, mut workflow) = poll_fn_sx
        .send(initialize_task)
        .scope(|| create_workflow(poll_fns));

    let emit_message_and_trap: MockPollFn = |ctx| {
        let traces_id = ctx
            .data()
            .persisted
            .channels()
            .channel_id("traces")
            .unwrap();
        let mut traces = ctx.data_mut().sender(traces_id);
        let poll_res = traces.poll_ready().into_inner(ctx)?;
        assert_matches!(poll_res, Poll::Ready(Ok(_)));
        ctx.data_mut()
            .sender(traces_id)
            .start_send(b"trace #1".to_vec())?;

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
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let (_, mut workflow) = poll_fn_sx
        .send(initialize_task)
        .scope(|| create_workflow(poll_fns));

    // Push the message in order to tick the main task.
    let flush_event_and_trap: MockPollFn = |ctx| {
        emit_event_and_flush(ctx)?;
        Err(anyhow!("boom"))
    };
    poll_fn_sx
        .send(flush_event_and_trap)
        .scope(|| push_message_and_tick(&mut workflow))
        .unwrap_err();

    let senders = workflow.data().persisted.channels.senders.values();
    let wakers: Vec<_> = senders
        .flat_map(|state| state.wakes_on_flush.iter().copied())
        .collect();
    assert!(wakers.is_empty(), "{wakers:?}");
}

#[test]
fn timers_basics() {
    let create_timers: MockPollFn = |ctx| {
        let ts = ctx.data().current_timestamp();
        let timer_id = ctx.data_mut().create_timer(TimerDefinition {
            expires_at: ts - chrono::Duration::milliseconds(100),
        });
        assert_eq!(timer_id, 0);
        // ^ implementation detail, but it's used in code later
        let result = ctx.data_mut().timer(timer_id).poll().into_inner(ctx)?;
        assert_eq!(extract_timestamp(result), ts);

        let timer_id = ctx.data_mut().create_timer(TimerDefinition {
            expires_at: ts + chrono::Duration::milliseconds(100),
        });
        assert_eq!(timer_id, 1);
        let result = ctx.data_mut().timer(timer_id).poll().into_inner(ctx)?;
        assert!(result.is_pending());

        Ok(Poll::Pending)
    };
    let poll_timers_and_drop: MockPollFn = |ctx| {
        let result = ctx.data_mut().timer(0).poll().into_inner(ctx)?;
        assert!(result.is_ready());
        let _wakers = ctx.data_mut().timer(0).drop();

        let result = ctx.data_mut().timer(1).poll().into_inner(ctx)?;
        let ts = ctx.data().current_timestamp();
        assert_eq!(extract_timestamp(result), ts);
        let _wakers = ctx.data_mut().timer(1).drop();

        Ok(Poll::Pending)
    };

    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let scheduler = MockScheduler::default();
    let (_, mut workflow) = poll_fn_sx
        .send(create_timers)
        .scope(|| create_workflow_with_scheduler(poll_fns, scheduler.clone()));

    let timers: HashMap<_, _> = workflow.data().persisted.timers.iter().collect();
    assert_eq!(timers.len(), 2, "{timers:?}");
    assert!(timers[&0].completed_at().is_some());
    assert!(timers[&1].completed_at().is_none());

    scheduler.set_now(scheduler.now() + chrono::Duration::seconds(1));
    let mut persisted = workflow.persist();
    persisted.data_mut().set_current_time(scheduler.now());
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let mut workflow = restore_workflow(poll_fns, persisted, scheduler);
    poll_fn_sx
        .send(poll_timers_and_drop)
        .scope(|| workflow.tick())
        .unwrap();

    let timers: HashMap<_, _> = workflow.data().persisted.timers.iter().collect();
    assert!(timers.is_empty(), "{timers:?}"); // timers are cleared on drop
}

#[test]
fn dropping_receiver_in_workflow() {
    let drop_channel: MockPollFn = |ctx| {
        let _ = initialize_task(ctx)?;
        let channels = ctx.data().persisted.channels();
        let orders_id = channels.channel_id("orders").unwrap();
        let _wakers = ctx.data_mut().receiver(orders_id).drop();
        Ok(Poll::Pending)
    };

    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    poll_fn_sx
        .send(drop_channel)
        .scope(|| create_workflow(poll_fns));
}

fn report_task_error(data: &mut WorkflowData, line: u32, column: u32, message: &str) {
    data.report_error_or_panic(
        ReportedErrorKind::TaskError,
        Some(message.to_owned()),
        Some("/build/src/test.rs".to_owned()),
        line,
        column,
    );
}

#[allow(clippy::unnecessary_wraps)] // convenient to use as a mock poll fn
pub(crate) fn complete_task_with_error(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    report_task_error(ctx.data_mut(), 42, 1, "error message");
    Ok(Poll::Ready(()))
}

#[test]
fn completing_main_task_with_error() {
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let (receipt, workflow) = poll_fn_sx
        .send(complete_task_with_error)
        .scope(|| create_workflow(poll_fns));

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
        other => panic!("unexpected task result: {other:?}"),
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
    let complete_task_with_compound_error: MockPollFn = |ctx| {
        report_task_error(ctx.data_mut(), 42, 1, "error message");
        report_task_error(ctx.data_mut(), 10, 4, "context message");
        Ok(Poll::Ready(()))
    };

    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let (_, workflow) = poll_fn_sx
        .send(complete_task_with_compound_error)
        .scope(|| create_workflow(poll_fns));

    let tasks = &workflow.data().persisted.tasks;
    assert_eq!(tasks.len(), 1);
    assert_task_error(&tasks[&0], true);
}

#[test]
fn completing_subtask_with_error() {
    let check_subtask_completion: MockPollFn = |ctx| {
        let poll_res = ctx.data_mut().task(1).poll_completion().into_inner(ctx)?;
        assert_matches!(poll_res, Poll::Ready(Ok(_)));
        Ok(Poll::Pending)
    };
    let mut main_task_polls = mimicry::Answers::from_values([
        initialize_and_spawn_task as MockPollFn,
        check_subtask_completion,
    ]);
    let poll_fns = MockAnswers::from_fn(move |&task_id| match task_id {
        0 => main_task_polls.next_for(()),
        1 => complete_task_with_error,
        _ => unreachable!(),
    });
    let (receipt, workflow) = create_workflow(poll_fns);

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
