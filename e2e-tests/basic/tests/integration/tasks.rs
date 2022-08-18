//! Tests a variation of workflow with externally executed tasks.

use assert_matches::assert_matches;
use async_std::task;
use futures::{channel::mpsc, future, stream, SinkExt, StreamExt, TryStreamExt};

use std::cmp;

use tardigrade::channel::WithId;
use tardigrade_rt::handle::future::{AsyncEnv, AsyncIoScheduler, Termination};
use tardigrade_test_basic::{
    tasks::{Args, PizzaDeliveryWithTasks},
    DomainEvent, PizzaKind, PizzaOrder,
};

use super::{TestResult, MODULE};

async fn test_external_tasks(
    oven_count: usize,
    order_count: usize,
    task_concurrency: Option<usize>,
) -> TestResult {
    let module = task::spawn_blocking(|| &*MODULE).await;
    let spawner = module.for_workflow::<PizzaDeliveryWithTasks>()?;

    let inputs = Args { oven_count };
    let workflow = spawner.spawn(inputs)?.init()?.into_inner();
    let mut env = AsyncEnv::new(workflow, AsyncIoScheduler);
    let mut handle = env.handle();
    let join_handle = task::spawn(async move { env.run().await });

    let responses = handle.baking_responses;
    let (executor_events_sx, executor_events_rx) = mpsc::unbounded();
    let tasks_stream = handle.baking_tasks.try_for_each_concurrent(
        task_concurrency,
        move |WithId { id, data: order }| {
            let mut responses = responses.clone();
            let executor_events_sx = executor_events_sx.clone();
            let index = id as usize;
            async move {
                executor_events_sx
                    .unbounded_send(DomainEvent::OrderTaken { index, order })
                    .unwrap();
                task::sleep(order.kind.baking_time()).await;
                responses.send(WithId { id, data: () }).await.ok();
                executor_events_sx
                    .unbounded_send(DomainEvent::Baked { index, order })
                    .unwrap();
                Ok(())
            }
        },
    );
    let tasks_handle = task::spawn(tasks_stream);

    let orders = (0..order_count).map(|i| {
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
    handle.orders.send_all(&mut stream::iter(orders)).await?;
    drop(handle.orders);

    let events: Vec<_> = handle.shared.events.try_collect().await?;
    assert_eq!(events.len(), 2 * order_count, "{:?}", events);
    for i in 1..=order_count {
        let order_events: Vec<_> = events.iter().filter(|event| event.index() == i).collect();
        assert_matches!(
            order_events.as_slice(),
            [DomainEvent::OrderTaken { .. }, DomainEvent::Baked { .. }]
        );
    }

    // Check that concurrency is properly controlled by the workflow.
    let executor_events: Vec<_> = executor_events_rx.collect().await;
    assert_eq!(
        executor_events.len(),
        2 * order_count,
        "{:?}",
        executor_events
    );
    let mut current_concurrency = 0;
    let mut max_concurrency = 0;
    for event in &executor_events {
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

    let expected_concurrency = cmp::min(oven_count, task_concurrency.unwrap_or(usize::MAX));
    assert!(
        max_concurrency <= expected_concurrency,
        "{:?}",
        executor_events
    );

    tasks_handle.await?;
    assert_matches!(join_handle.await?, Termination::Finished);
    Ok(())
}

#[async_std::test]
async fn external_task_basics() -> TestResult {
    test_external_tasks(1, 1, None).await
}

#[async_std::test]
async fn sequential_external_tasks() -> TestResult {
    test_external_tasks(1, 4, None).await
}

#[async_std::test]
async fn concurrent_external_tasks() -> TestResult {
    test_external_tasks(3, 10, None).await
}

#[async_std::test]
async fn tasks_with_concurrency_limited_by_executor() -> TestResult {
    test_external_tasks(3, 10, Some(1)).await
}

#[async_std::test]
async fn closing_task_responses_on_host() -> TestResult {
    const ORDER_COUNT: usize = 10;
    const SUCCESSFUL_TASK_COUNT: usize = 3;

    let module = task::spawn_blocking(|| &*MODULE).await;
    let spawner = module.for_workflow::<PizzaDeliveryWithTasks>()?;

    let inputs = Args { oven_count: 2 };
    let workflow = spawner.spawn(inputs)?.init()?.into_inner();
    let mut env = AsyncEnv::new(workflow, AsyncIoScheduler);
    let mut handle = env.handle();
    let join_handle = task::spawn(async move { env.run().await });

    let responses = handle.baking_responses;
    let tasks_stream = handle.baking_tasks.map(move |res| {
        let WithId { id, data: order } = res.unwrap();
        let mut responses = responses.clone();
        async move {
            task::sleep(order.kind.baking_time()).await;
            responses.send(WithId { id, data: () }).await.ok();
        }
    });
    // Complete first `SUCCESSFUL_TASK_COUNT` tasks, then drop the executor.
    let tasks_stream = tasks_stream
        .buffer_unordered(SUCCESSFUL_TASK_COUNT)
        .take(SUCCESSFUL_TASK_COUNT)
        .for_each(future::ready);
    let tasks_handle = task::spawn(tasks_stream);

    let orders = (0..ORDER_COUNT).map(|i| {
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
    handle.orders.send_all(&mut stream::iter(orders)).await?;
    drop(handle.orders);

    let events: Vec<_> = handle.shared.events.try_collect().await?;
    assert_eq!(
        events.len(),
        ORDER_COUNT + SUCCESSFUL_TASK_COUNT,
        "{:?}",
        events
    );
    let baked_count = events
        .iter()
        .filter(|event| matches!(event, DomainEvent::Baked { .. }))
        .count();
    assert_eq!(baked_count, SUCCESSFUL_TASK_COUNT, "{:?}", events);

    tasks_handle.await;
    assert_matches!(join_handle.await?, Termination::Finished);
    Ok(())
}
