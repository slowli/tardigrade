//! Tests a variation of workflow with externally executed tasks.

use assert_matches::assert_matches;
use async_std::task;
use futures::{channel::mpsc, future, FutureExt, StreamExt, TryStreamExt};
use test_casing::{test_casing, Product};

use std::cmp;

use tardigrade::channel::{Request, Response};
use tardigrade_rt::{
    manager::{DriveConfig, Termination},
    AsyncIoScheduler,
};

use crate::{
    create_streaming_manager, spawn_workflow,
    tasks::{assert_event_completeness, assert_event_concurrency, send_orders},
    TestResult,
};
use tardigrade_pizza::{
    requests::{Args, PizzaDeliveryWithRequests},
    DomainEvent, PizzaKind, PizzaOrder,
};

const DEFINITION_ID: &str = "test::PizzaDeliveryWithRequests";

async fn test_external_tasks(
    oven_count: usize,
    order_count: usize,
    task_concurrency: Option<usize>,
) -> TestResult {
    let (manager, mut commits_rx) = create_streaming_manager(AsyncIoScheduler).await?;

    let args = Args { oven_count };
    let (_, handle) =
        spawn_workflow::<_, PizzaDeliveryWithRequests>(&manager, DEFINITION_ID, args).await?;
    let responses_sx = handle.baking.responses.into_owned();
    let responses_id = responses_sx.channel_id();
    let baking_tasks_rx = handle.baking.requests.into_owned().stream_messages(0..);
    let baking_tasks_rx = baking_tasks_rx.map(|message| message.decode());
    let orders_sx = handle.orders;
    let events_rx = handle.shared.events.stream_messages(0..);
    let events_rx = events_rx.map(|message| message.decode());

    let manager = manager.clone();
    let join_handle =
        task::spawn(async move { manager.drive(&mut commits_rx, DriveConfig::new()).await });

    let (executor_events_sx, executor_events_rx) = mpsc::unbounded();
    let tasks_stream = baking_tasks_rx.try_for_each_concurrent(task_concurrency, move |request| {
        let Request::New { id, data: order, response_channel_id } = request else {
            return future::ok(()).left_future();
        };
        assert_eq!(response_channel_id, responses_id);

        let responses = responses_sx.clone();
        let executor_events_sx = executor_events_sx.clone();
        let index = id as usize;
        async move {
            executor_events_sx
                .unbounded_send(DomainEvent::OrderTaken { index, order })
                .unwrap();
            task::sleep(order.kind.baking_time()).await;
            responses.send(Response { id, data: () }).await.ok();
            executor_events_sx
                .unbounded_send(DomainEvent::Baked { index, order })
                .unwrap();
            Ok(())
        }
        .right_future()
    });
    let tasks_handle = task::spawn(tasks_stream);

    send_orders(orders_sx, order_count).await?;
    let events: Vec<_> = events_rx.try_collect().await?;
    assert_eq!(events.len(), 2 * order_count, "{events:?}");
    assert_event_completeness(&events, order_count);

    // Check that concurrency is properly controlled by the workflow.
    let executor_events: Vec<_> = executor_events_rx.collect().await;
    let expected_concurrency = cmp::min(oven_count, task_concurrency.unwrap_or(usize::MAX));
    assert_event_concurrency(&executor_events, expected_concurrency);

    tasks_handle.await?;
    assert_matches!(join_handle.await, Termination::Finished);
    Ok(())
}

#[async_std::test]
async fn external_task_basics() -> TestResult {
    test_external_tasks(1, 1, None).await
}

const CONCURRENCY_CASES: [Option<usize>; 4] = [None, Some(1), Some(2), Some(3)];

#[test_casing(4, CONCURRENCY_CASES)]
#[async_std::test]
async fn sequential_external_tasks(task_concurrency: Option<usize>) -> TestResult {
    test_external_tasks(1, 4, task_concurrency).await
}

#[test_casing(12, Product((2..=4, CONCURRENCY_CASES)))]
#[async_std::test]
async fn concurrent_external_tasks(
    oven_count: usize,
    task_concurrency: Option<usize>,
) -> TestResult {
    test_external_tasks(oven_count, 10, task_concurrency).await
}

#[test_casing(4, [(5, 2), (5, 4), (10, 3), (10, 7)])]
#[async_std::test]
async fn closing_task_responses_on_host(
    order_count: usize,
    successful_task_count: usize,
) -> TestResult {
    let (manager, mut commits_rx) = create_streaming_manager(AsyncIoScheduler).await?;
    let args = Args { oven_count: 2 };
    let (_, handle) =
        spawn_workflow::<_, PizzaDeliveryWithRequests>(&manager, DEFINITION_ID, args).await?;

    let responses_sx = handle.baking.responses.into_owned();
    let baking_tasks = handle.baking.requests.into_owned();
    let orders_sx = handle.orders;
    let events_rx = handle.shared.events.stream_messages(0..);
    let events_rx = events_rx.map(|message| message.decode());

    let manager = manager.clone();
    let join_handle =
        task::spawn(async move { manager.drive(&mut commits_rx, DriveConfig::default()).await });

    // Complete first `successful_task_count` tasks, then close the responses channel.
    let tasks_handle = task::spawn(async move {
        let baking_tasks_rx = baking_tasks.stream_messages(0..);
        let responses_sx_to_close = responses_sx.clone();
        let tasks_stream = baking_tasks_rx.map(move |message| {
            let Request::New { id, data: order, .. } = message.decode().unwrap() else {
                return future::ready(()).left_future();
            };

            let responses_sx = responses_sx.clone();
            async move {
                task::sleep(order.kind.baking_time()).await;
                responses_sx.send(Response { id, data: () }).await.ok();
            }
            .right_future()
        });
        tasks_stream
            .buffer_unordered(successful_task_count)
            .take(successful_task_count)
            .for_each(future::ready)
            .await;
        responses_sx_to_close.close().await;
    });

    let orders = (0..order_count).map(|i| PizzaOrder {
        kind: match i % 3 {
            0 => PizzaKind::Margherita,
            1 => PizzaKind::Pepperoni,
            2 => PizzaKind::FourCheese,
            _ => unreachable!(),
        },
        delivery_distance: 10,
    });
    orders_sx.send_all(orders).await?;
    orders_sx.close().await;

    let events: Vec<_> = events_rx.try_collect().await?;
    assert_eq!(
        events.len(),
        order_count + successful_task_count,
        "{events:?}"
    );
    let baked_count = events
        .iter()
        .filter(|event| matches!(event, DomainEvent::Baked { .. }))
        .count();
    assert_eq!(baked_count, successful_task_count, "{events:?}");

    tasks_handle.await;
    assert_matches!(join_handle.await, Termination::Finished);
    Ok(())
}
