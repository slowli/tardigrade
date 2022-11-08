//! Tests for the `PizzaDeliveryWithSpawning` workflow.

use assert_matches::assert_matches;
use async_std::task;
use futures::{stream, SinkExt, StreamExt, TryStreamExt};

use std::{collections::HashMap, task::Poll};

use tardigrade::spawn::ManageWorkflowsExt;
use tardigrade_rt::{
    manager::{
        future::{AsyncEnv, AsyncIoScheduler, Termination},
        WorkflowManager,
    },
    receipt::{Event, Receipt, ResourceEvent, ResourceEventKind, ResourceId},
    storage::LocalStorage,
};
use tardigrade_test_basic::{
    spawn::{Baking, PizzaDeliveryWithSpawning},
    Args, DomainEvent, PizzaKind, PizzaOrder,
};

use super::{create_module, TestResult};

const PARENT_DEF: &str = "test::PizzaDeliveryWithSpawning";

#[async_std::test]
async fn spawning_child_workflows() -> TestResult {
    const READY: Poll<()> = Poll::Ready(());

    let module = create_module().await;
    let mut manager = WorkflowManager::builder(LocalStorage::default())
        .build()
        .await;
    manager.insert_module("test", module).await;

    let inputs = Args {
        oven_count: 2,
        deliverer_count: 1,
    };
    let mut workflow = manager
        .new_workflow::<PizzaDeliveryWithSpawning>(PARENT_DEF, inputs)?
        .build()
        .await?;

    let handle = workflow.handle();
    let mut env = AsyncEnv::new(AsyncIoScheduler);
    let results = env.tick_results();
    let mut orders_sx = handle.orders.into_async(&mut env);
    let events_rx = handle.shared.events.into_async(&mut env);
    let join_handle = task::spawn(async move { env.run(&mut manager).await });

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
    orders_sx
        .send_all(&mut stream::iter(orders).map(Ok))
        .await?;
    drop(orders_sx);

    let termination = join_handle.await?;
    assert_matches!(termination, Termination::Finished);

    // Check domain events produced by child workflows
    let events: Vec<_> = events_rx.try_collect().await?;
    assert_eq!(events.len(), orders.len() * 2);
    for i in 1..=orders.len() {
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

    // Check that child-related events are complete.
    let workflow_events_by_kind = receipts.iter().flat_map(Receipt::events).fold(
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
    dbg!(&workflow_events_by_kind);

    assert_eq!(workflow_events_by_kind[&ResourceEventKind::Created], 2);
    assert_eq!(workflow_events_by_kind[&ResourceEventKind::Dropped], 2);
    assert_eq!(
        workflow_events_by_kind[&ResourceEventKind::Polled(READY)],
        2
    );

    Ok(())
}

#[async_std::test]
async fn accessing_handles_in_child_workflows() -> TestResult {
    let module = create_module().await;
    let mut manager = WorkflowManager::builder(LocalStorage::default())
        .build()
        .await;
    manager.insert_module("test", module).await;

    let inputs = Args {
        oven_count: 2,
        deliverer_count: 1,
    };
    let mut workflow = manager
        .new_workflow::<PizzaDeliveryWithSpawning>(PARENT_DEF, inputs)?
        .build()
        .await?;
    let mut handle = workflow.handle();

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
    for order in orders {
        handle.orders.send(order).await?;
    }
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

    // At this point, 2 child workflows should be spawned and initialized.
    assert_eq!(child_ids.len(), 2);

    let mut env = AsyncEnv::new(AsyncIoScheduler);
    let mut child_events_rxs = vec![];
    for &child_id in &child_ids {
        let child = manager.workflow(child_id).await.unwrap();
        let child_handle = child.downcast::<Baking>()?.handle();
        assert!(child_handle.events.can_receive_messages());
        assert_eq!(child_handle.events.channel_id(), parent_events_id);
        child_events_rxs.push(child_handle.events.into_async(&mut env));
    }

    // The aliased channels should be immediately disconnected.
    assert!(child_events_rxs[0].try_next().await?.is_none());

    task::spawn(async move { env.run(&mut manager).await });
    // The pre-closed channels should be disconnected as well, but this happens
    // on first tick.
    let events: Vec<_> = child_events_rxs[1].by_ref().try_collect().await?;
    assert_eq!(events.len(), 4);

    Ok(())
}
