//! Tests a variation of workflow with subtasks.

use assert_matches::assert_matches;
use async_std::task;
use futures::{stream, SinkExt, StreamExt, TryStreamExt};

use std::{
    cmp,
    collections::{HashMap, HashSet},
    sync::Arc,
    task::Poll,
    time::Duration,
};

use tardigrade::{spawn::ManageWorkflowsExt, Json, TaskId};
use tardigrade_rt::{
    manager::{
        future::{AsyncEnv, MessageSender, Termination},
        WorkflowManager,
    },
    receipt::{
        Event, ExecutedFunction, Execution, Receipt, ResourceEvent, ResourceEventKind, ResourceId,
    },
    test::MockScheduler,
};
use tardigrade_test_basic::{
    tasks::{Args, PizzaDeliveryWithTasks},
    DomainEvent, PizzaKind, PizzaOrder,
};

use crate::{TestResult, MODULE};

const LARGE_DURATION: Duration = Duration::from_secs(86_400);

pub(crate) async fn send_orders(
    mut orders_sx: MessageSender<PizzaOrder, Json>,
    count: usize,
) -> TestResult {
    let orders = (0..count).map(|i| {
        Ok(PizzaOrder {
            kind: match i % 3 {
                0 => PizzaKind::Margherita,
                1 => PizzaKind::Pepperoni,
                2 => PizzaKind::FourCheese,
                _ => unreachable!(),
            },
            delivery_distance: 10,
        })
    });
    orders_sx.send_all(&mut stream::iter(orders)).await?;
    Ok(())
}

pub(crate) fn assert_event_completeness(events: &[DomainEvent], order_count: usize) {
    assert_eq!(events.len(), 2 * order_count, "{events:?}");

    for i in 1..=order_count {
        let order_events: Vec<_> = events.iter().filter(|event| event.index() == i).collect();
        assert_matches!(
            order_events.as_slice(),
            [DomainEvent::OrderTaken { .. }, DomainEvent::Baked { .. }]
        );
    }
}

pub(crate) fn assert_event_concurrency(events: &[DomainEvent], expected_concurrency: usize) {
    let mut current_concurrency = 0;
    let mut max_concurrency = 0;
    for event in events {
        match event {
            DomainEvent::OrderTaken { .. } => {
                current_concurrency += 1;
                max_concurrency = cmp::max(max_concurrency, current_concurrency);
            }
            DomainEvent::Baked { .. } => {
                current_concurrency -= 1;
            }
            _ => unreachable!(),
        }
    }

    assert!(max_concurrency <= expected_concurrency, "{events:?}");
}

fn assert_receipts(receipts: &[Receipt], order_count: usize) {
    let receipt_events = receipts.iter().flat_map(|receipt| receipt.events());
    let mut events_by_task = HashMap::<_, Vec<_>>::new();
    for event in receipt_events {
        if let Event::Resource(ResourceEvent {
            resource_id: ResourceId::Task(id),
            kind,
            ..
        }) = event
        {
            events_by_task.entry(*id).or_default().push(*kind);
        }
    }

    events_by_task.remove(&0); // main task has an incomplete set of events
    assert_eq!(events_by_task.len(), order_count);
    for task_events in events_by_task.values() {
        assert_matches!(
            task_events.as_slice(),
            [
                ResourceEventKind::Created,
                ResourceEventKind::Polled(Poll::Pending),
                ResourceEventKind::Dropped,
                ResourceEventKind::Polled(Poll::Ready(())),
            ]
        );
    }
}

async fn setup_workflow(
    args: Args,
    order_count: usize,
) -> TestResult<(Vec<DomainEvent>, Vec<Receipt>)> {
    let module = task::spawn_blocking(|| &*MODULE).await;
    let spawner = module.for_workflow::<PizzaDeliveryWithTasks>()?;
    let (scheduler, mut expirations) = MockScheduler::with_expirations();
    let scheduler = Arc::new(scheduler);
    let mut manager = WorkflowManager::builder()
        .with_spawner("pizza", spawner)
        .with_clock(Arc::clone(&scheduler))
        .build();

    let mut workflow = manager
        .new_workflow::<PizzaDeliveryWithTasks>("pizza", args)?
        .build()?;
    let handle = workflow.handle();
    let mut env = AsyncEnv::new(Arc::clone(&scheduler));
    let orders_sx = handle.orders.into_async(&mut env);
    let events_rx = handle.shared.events.into_async(&mut env);
    let receipts_rx = env
        .tick_results()
        .map(|result| result.into_inner().unwrap());
    let join_handle = task::spawn(async move { env.run(&mut manager).await });

    // We want to have precise control over time (in order to deterministically fail subtasks).
    task::spawn(async move {
        while let Some(expiration) = expirations.next().await {
            if expiration > scheduler.now() {
                scheduler.set_now(expiration);
            }
        }
    });
    send_orders(orders_sx, order_count).await?;
    assert_matches!(join_handle.await?, Termination::Finished);

    let events = events_rx.try_collect().await?;
    let receipts = receipts_rx.collect().await;
    Ok((events, receipts))
}

async fn test_workflow_with_tasks(args: Args, order_count: usize) -> TestResult {
    let (events, receipts) = setup_workflow(args, order_count).await?;
    assert_event_completeness(&events, order_count);
    assert_event_concurrency(&events, args.oven_count);
    assert_receipts(&receipts, order_count);
    Ok(())
}

#[async_std::test]
async fn task_basics() -> TestResult {
    let args = Args {
        oven_count: 1,
        fail_after: LARGE_DURATION,
        propagate_errors: false,
    };

    for order_count in 1..5 {
        println!("Testing with {order_count} order(s)");
        test_workflow_with_tasks(args, order_count).await?;
    }
    Ok(())
}

#[async_std::test]
async fn tasks_with_concurrency() -> TestResult {
    let args = Args {
        oven_count: 3,
        fail_after: LARGE_DURATION,
        propagate_errors: false,
    };

    for order_count in [2, 3, 5, 8, 13] {
        println!("Testing with {order_count} order(s)");
        test_workflow_with_tasks(args, order_count).await?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskFailureKind {
    /// Fails all baking subtasks.
    All,
    /// Fails baking margherita pizzas.
    Margherita,
}

impl TaskFailureKind {
    fn fail_after(self) -> Duration {
        match self {
            Self::All => PizzaKind::FourCheese.baking_time() - Duration::from_millis(5),
            Self::Margherita => PizzaKind::Margherita.baking_time() - Duration::from_millis(5),
        }
    }
}

fn assert_task_results(receipts: &[Receipt], expected_successful_tasks: &HashSet<TaskId>) {
    let executions = receipts.iter().flat_map(Receipt::executions);
    let results_by_task = executions.filter_map(|execution| {
        if let Execution {
            function: ExecutedFunction::Task { task_id, .. },
            task_result: Some(result),
            ..
        } = execution
        {
            Some((*task_id, result))
        } else {
            None
        }
    });
    let results_by_task: HashMap<_, _> = results_by_task.collect();
    for (task_id, &result) in &results_by_task {
        if expected_successful_tasks.contains(task_id) {
            assert!(result.is_ok(), "{result:?}");
        } else {
            let err = result.as_ref().unwrap_err();
            assert_eq!(err.cause().to_string(), "baking interrupted");
            assert!(err.location().filename.ends_with("tasks.rs"));

            if *task_id == 0 {
                assert_eq!(err.contexts().len(), 1);
                let context = &err.contexts()[0];
                assert_eq!(context.message(), "propagating task error");
                assert!(context.location().filename.ends_with("tasks.rs"));
            } else {
                assert!(err.contexts().is_empty());
            }
        }
    }
}

async fn test_failures_in_tasks(kind: TaskFailureKind, propagate_errors: bool) -> TestResult {
    let args = Args {
        oven_count: 2,
        fail_after: kind.fail_after(),
        propagate_errors,
    };
    let order_count = 3;
    let (events, receipts) = setup_workflow(args, order_count).await?;

    if propagate_errors {
        for i in 1..=order_count {
            let order_events: Vec<_> = events.iter().filter(|event| event.index() == i).collect();
            assert_matches!(
                order_events.as_slice(),
                [] | [DomainEvent::OrderTaken { .. }] // ^ The main task may quit before the order is taken in some subtasks
            );
        }
    } else {
        for i in 0..order_count {
            let order_events: Vec<_> = events
                .iter()
                .filter(|event| event.index() == i + 1)
                .collect();

            let should_fail = kind == TaskFailureKind::All || i % 3 == 0;
            if should_fail {
                assert_matches!(order_events.as_slice(), [DomainEvent::OrderTaken { .. }]);
            } else {
                assert_matches!(
                    order_events.as_slice(),
                    [DomainEvent::OrderTaken { .. }, DomainEvent::Baked { .. }]
                );
            }
        }
        assert_receipts(&receipts, order_count);
    }

    let mut expected_successful_tasks: HashSet<TaskId> = match kind {
        TaskFailureKind::All => HashSet::new(),
        TaskFailureKind::Margherita => {
            let all_events = receipts.iter().flat_map(Receipt::events);
            let subtask_ids = all_events.filter_map(|event| {
                if let Event::Resource(ResourceEvent {
                    resource_id: ResourceId::Task(id),
                    kind: ResourceEventKind::Created,
                    ..
                }) = event
                {
                    Some(*id)
                } else {
                    None
                }
            });
            let successful_task_ids = subtask_ids.enumerate().filter_map(|(idx, id)| {
                // Margherita orders have `idx % 3 == 0` as per `send_orders()`
                Some(id).filter(|_| idx % 3 != 0)
            });
            successful_task_ids.collect()
        }
    };
    if !propagate_errors {
        expected_successful_tasks.insert(0);
    }
    assert_task_results(&receipts, &expected_successful_tasks);
    Ok(())
}

#[async_std::test]
async fn failures_in_subtasks() -> TestResult {
    test_failures_in_tasks(TaskFailureKind::All, false).await
}

#[async_std::test]
async fn partial_failures_in_subtasks() -> TestResult {
    test_failures_in_tasks(TaskFailureKind::Margherita, false).await
}

#[async_std::test]
async fn propagated_failures_in_subtasks() -> TestResult {
    test_failures_in_tasks(TaskFailureKind::All, true).await
}
