//! Tests for the `PizzaDeliveryWithSpawning` workflow.

use assert_matches::assert_matches;
use async_std::task;
use futures::{StreamExt, TryStreamExt};
use test_casing::test_casing;

use std::{collections::HashMap, task::Poll};

use tardigrade_rt::{
    receipt::{Event, Receipt, ResourceEvent, ResourceEventKind, ResourceId},
    runtime::{DriveConfig, Termination},
    test::MockScheduler,
    AsyncIoScheduler,
};

use crate::{
    create_streaming_manager, driver::spawn_traced_task, enable_tracing_assertions, spawn_workflow,
    tasks::send_orders, TestResult,
};
use tardigrade_pizza::{
    spawn::{Args, Baking, PizzaDeliveryWithSpawning},
    DomainEvent, PizzaKind, PizzaOrder,
};

const PARENT_DEF: &str = "test::PizzaDeliveryWithSpawning";

#[test_casing(4, [(2, 2), (1, 5), (2, 5), (5, 2)])]
#[async_std::test]
async fn spawning_child_workflows(oven_count: usize, order_count: usize) -> TestResult {
    let (_guard, tracing_storage) = enable_tracing_assertions();
    let (manager, mut commits_rx) = create_streaming_manager(AsyncIoScheduler).await?;

    let args = Args {
        oven_count,
        collect_metrics: true,
    };
    let (_, handle) =
        spawn_workflow::<_, PizzaDeliveryWithSpawning>(&manager, PARENT_DEF, args).await?;
    let orders_sx = handle.orders;
    let events_rx = handle.shared.events.stream_messages(0..);
    let events_rx = events_rx.map(|message| message.decode());

    let mut config = DriveConfig::new();
    let results = config.tick_results();
    let manager = manager.clone();
    let join_handle =
        spawn_traced_task(async move { manager.drive(&mut commits_rx, config).await });

    send_orders(orders_sx, order_count).await?;
    let termination = join_handle.await;
    assert_matches!(termination, Termination::Finished);

    // Check domain events produced by child workflows
    let events: Vec<_> = events_rx.try_collect().await?;
    assert_eq!(events.len(), order_count * 2, "{events:?}");
    for i in 1..=order_count {
        let order_events: Vec<_> = events.iter().filter(|event| event.index() == i).collect();
        assert_matches!(
            order_events.as_slice(),
            [DomainEvent::OrderTaken { .. }, DomainEvent::Baked { .. }]
        );
    }

    let results: Vec<_> = results.collect().await;
    let receipts: Vec<_> = results
        .into_iter()
        .map(|result| result.into_inner().unwrap())
        .collect();

    // Check metrics collected from child workflows.
    {
        let storage = tracing_storage.lock();
        let durations = storage.all_events().filter_map(|event| {
            if event.message() == Some("received child metrics") {
                event["duration"].as_uint()
            } else {
                None
            }
        });
        let durations: Vec<_> = durations.collect();
        assert_eq!(durations.len(), order_count);
        assert!(
            durations.iter().copied().all(|dur| dur < 1_000),
            "{durations:?}"
        );
    }

    assert_child_events(&receipts, order_count);
    Ok(())
}

fn assert_child_events(receipts: &[Receipt], order_count: usize) {
    const READY: Poll<()> = Poll::Ready(());

    let events_by_kind = receipts.iter().flat_map(Receipt::events).fold(
        HashMap::<_, usize>::new(),
        |mut acc, event| {
            if let Event::Resource(ResourceEvent {
                resource_id: ResourceId::Workflow(_),
                kind,
                ..
            }) = event
            {
                *acc.entry(*kind).or_default() += 1;
            }
            acc
        },
    );

    assert_eq!(events_by_kind[&ResourceEventKind::Created], order_count);
    assert_eq!(events_by_kind[&ResourceEventKind::Dropped], order_count);
    assert_eq!(
        events_by_kind[&ResourceEventKind::Polled(READY)],
        order_count
    );
}

#[async_std::test]
async fn accessing_handles_in_child_workflows() -> TestResult {
    let (scheduler, mut expirations) = MockScheduler::with_expirations();
    let (manager, mut commits_rx) = create_streaming_manager(scheduler.clone()).await?;

    let args = Args {
        oven_count: 2,
        collect_metrics: false,
    };
    let (_, handle) =
        spawn_workflow::<_, PizzaDeliveryWithSpawning>(&manager, PARENT_DEF, args).await?;

    let orders = [
        PizzaOrder {
            kind: PizzaKind::Pepperoni,
            delivery_distance: 10,
        },
        PizzaOrder {
            kind: PizzaKind::FourCheese,
            delivery_distance: 5,
        },
    ];
    handle.orders.send_all(orders).await?;
    let parent_events_id = handle.shared.events.channel_id();

    let mut child_ids = vec![];
    while let Ok(tick_result) = manager.tick().await {
        let receipt = tick_result.into_inner()?;
        let new_child_ids = receipt.events().filter_map(|event| {
            if let Event::Resource(ResourceEvent {
                resource_id: ResourceId::Workflow(child_id),
                kind: ResourceEventKind::Created,
                ..
            }) = event
            {
                Some(*child_id)
            } else {
                None
            }
        });
        child_ids.extend(new_child_ids);
    }
    // At this point, 2 child workflows should be spawned and initialized, and both blocked
    // on a timer.
    assert_eq!(child_ids.len(), 2);

    let mut child_events_rxs = vec![];
    for &child_id in &child_ids {
        let child = manager.storage().workflow(child_id).await.unwrap();
        let child_handle = child.downcast::<Baking>()?.handle().await;

        assert!(child_handle.events.can_manipulate());
        assert_eq!(child_handle.events.channel_id(), parent_events_id);
        let event = child_handle.events.receive_message(0).await?;
        assert_matches!(event.decode()?, DomainEvent::OrderTaken { .. });
        let child_events_rx = child_handle
            .events
            .stream_messages(2..)
            .map(|message| message.decode());
        child_events_rxs.push(child_events_rx);

        assert!(child_handle.duration.channel_info().is_closed);
        assert_eq!(child_handle.duration.channel_id(), 0);
    }

    // Complete baking tasks.
    task::spawn(async move {
        while let Some(expiration) = expirations.next().await {
            if expiration > scheduler.now() {
                scheduler.set_now(expiration);
            }
        }
    });
    handle.orders.close().await;

    let manager = manager.clone();
    task::spawn(async move { manager.drive(&mut commits_rx, DriveConfig::new()).await });
    let events: Vec<_> = child_events_rxs[1].by_ref().try_collect().await?;
    assert_matches!(
        events.as_slice(),
        [DomainEvent::Baked { .. }, DomainEvent::Baked { .. }]
    );

    Ok(())
}
