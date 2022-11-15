//! Tests a variation of workflow with externally executed tasks.

use assert_matches::assert_matches;
use async_std::task;
use futures::{channel::mpsc, future, stream, SinkExt, StreamExt, TryStreamExt};

use std::cmp;

use tardigrade::{channel::WithId, spawn::ManageWorkflowsExt};
use tardigrade_rt::{
    driver::{Driver, Termination},
    AsyncIoScheduler,
};
use tardigrade_test_basic::{
    requests::{Args, PizzaDeliveryWithRequests},
    DomainEvent, PizzaKind, PizzaOrder,
};

const DEFINITION_ID: &str = "test::PizzaDeliveryWithRequests";

use crate::{
    create_manager,
    tasks::{assert_event_completeness, assert_event_concurrency, send_orders},
    TestResult,
};

async fn test_external_tasks(
    oven_count: usize,
    order_count: usize,
    task_concurrency: Option<usize>,
) -> TestResult {
    let mut manager = create_manager(AsyncIoScheduler).await?;

    let mut workflow = manager
        .new_workflow::<PizzaDeliveryWithRequests>(DEFINITION_ID, Args { oven_count })?
        .build()
        .await?;
    let handle = workflow.handle();
    let mut driver = Driver::new();
    let responses_sx = handle.baking_responses.into_sink(&mut driver);
    let baking_tasks_rx = handle.baking_tasks.into_stream(&mut driver);
    let orders_sx = handle.orders.into_sink(&mut driver);
    let events_rx = handle.shared.events.into_stream(&mut driver);
    let join_handle = task::spawn(async move { driver.drive(&mut manager).await });

    let (executor_events_sx, executor_events_rx) = mpsc::unbounded();
    let tasks_stream = baking_tasks_rx.try_for_each_concurrent(
        task_concurrency,
        move |WithId { id, data: order }| {
            let mut responses = responses_sx.clone();
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

    send_orders(orders_sx, order_count).await?;

    let events: Vec<_> = events_rx.try_collect().await?;
    assert_eq!(events.len(), 2 * order_count, "{:?}", events);
    assert_event_completeness(&events, order_count);

    // Check that concurrency is properly controlled by the workflow.
    let executor_events: Vec<_> = executor_events_rx.collect().await;
    let expected_concurrency = cmp::min(oven_count, task_concurrency.unwrap_or(usize::MAX));
    assert_event_concurrency(&executor_events, expected_concurrency);

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

    let mut manager = create_manager(AsyncIoScheduler).await?;
    let mut workflow = manager
        .new_workflow::<PizzaDeliveryWithRequests>(DEFINITION_ID, Args { oven_count: 2 })?
        .build()
        .await?;

    let mut driver = Driver::new();
    let handle = workflow.handle();
    let responses_sx = handle.baking_responses.into_sink(&mut driver);
    let baking_tasks_rx = handle.baking_tasks.into_stream(&mut driver);
    let mut orders_sx = handle.orders.into_sink(&mut driver);
    let events_rx = handle.shared.events.into_stream(&mut driver);
    let join_handle = task::spawn(async move { driver.drive(&mut manager).await });

    let tasks_stream = baking_tasks_rx.map(move |res| {
        let WithId { id, data: order } = res.unwrap();
        let mut responses_sx = responses_sx.clone();
        async move {
            task::sleep(order.kind.baking_time()).await;
            responses_sx.send(WithId { id, data: () }).await.ok();
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
    orders_sx.send_all(&mut stream::iter(orders)).await?;
    drop(orders_sx);

    let events: Vec<_> = events_rx.try_collect().await?;
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
