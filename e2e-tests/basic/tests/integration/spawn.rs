//! Tests for the `PizzaDeliveryWithSpawning` workflow.

use assert_matches::assert_matches;
use async_std::task;
use futures::{stream, SinkExt, StreamExt};

use std::{collections::HashMap, task::Poll};

use tardigrade::spawn::ManageWorkflowsExt;
use tardigrade_rt::handle::future::Termination;
use tardigrade_rt::{
    handle::{
        future::{AsyncEnv, AsyncIoScheduler},
        WorkflowHandle,
    },
    manager::WorkflowManager,
    receipt::{Event, Receipt, ResourceEvent, ResourceEventKind, ResourceId},
    WorkflowModule,
};
use tardigrade_test_basic::{
    spawn::{Baking, PizzaDeliveryWithSpawning},
    Args, PizzaKind, PizzaOrder,
};

use super::{TestResult, MODULE};

fn create_manager(module: &WorkflowModule) -> TestResult<WorkflowManager> {
    Ok(WorkflowManager::builder()
        .with_spawner("baking", module.for_workflow::<Baking>()?)
        .with_spawner("pizza", module.for_workflow::<PizzaDeliveryWithSpawning>()?)
        .build())
}

#[async_std::test]
async fn spawning_child_workflows() -> TestResult {
    const READY: Poll<()> = Poll::Ready(());

    let module = task::spawn_blocking(|| &*MODULE).await;
    let mut manager = create_manager(module)?;

    let inputs = Args {
        oven_count: 2,
        deliverer_count: 1,
    };
    let mut workflow: WorkflowHandle<PizzaDeliveryWithSpawning> =
        manager.new_workflow("pizza", inputs)?.build()?;

    let handle = workflow.handle();
    let mut env = AsyncEnv::new(AsyncIoScheduler);
    let results = env.execution_results();
    let mut orders_sx = handle.orders.into_async(&mut env);
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

    assert_eq!(workflow_events_by_kind[&ResourceEventKind::Created], 2);
    assert_eq!(workflow_events_by_kind[&ResourceEventKind::Dropped], 2);
    assert_eq!(
        workflow_events_by_kind[&ResourceEventKind::Polled(READY)],
        2
    );

    Ok(())
}
