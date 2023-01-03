//! Version of the `PizzaDelivery` workflow with each order implemented via a separate workflow.
//! Also, there is no delivery.

use async_trait::async_trait;
use futures::{SinkExt, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::{DomainEvent, PizzaDelivery, PizzaOrder, SharedHandle};
use tardigrade::{
    channel::Sender,
    spawn::{CreateWorkflow, Workflows},
    task::{TaskError, TaskResult},
    workflow::{
        DelegateHandle, GetInterface, HandleFormat, InEnv, SpawnWorkflow, Wasm, WithHandle,
        WorkflowEntry, WorkflowFn,
    },
    Json,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Args {
    pub oven_count: usize,
    pub collect_metrics: bool,
}

#[derive(Debug, GetInterface, WorkflowEntry)]
pub struct PizzaDeliveryWithSpawning(());

impl DelegateHandle for PizzaDeliveryWithSpawning {
    type Delegate = PizzaDelivery;
}

impl WorkflowFn for PizzaDeliveryWithSpawning {
    type Args = Args;
    type Codec = Json;
}

impl PizzaDelivery {
    async fn spawn_with_child_workflows(self, args: Args) -> TaskResult {
        let mut counter = 0;
        self.orders
            .map(Ok)
            .try_for_each_concurrent(args.oven_count, |order| {
                counter += 1;
                let events = self.shared.events.clone();
                Self::spawn_child(counter, order, args.collect_metrics, events)
            })
            .await
    }

    async fn spawn_child(
        counter: usize,
        order: PizzaOrder,
        collect_metrics: bool,
        events: Sender<DomainEvent, Json>,
    ) -> TaskResult {
        const DEFINITION_ID: &str = "test::Baking";

        let builder = Workflows.new_workflow::<Baking>(DEFINITION_ID).await?;
        let handles = builder.handles(|config| {
            config.events.copy_from(events);
            if !collect_metrics {
                config.duration.close();
            }
        });
        let (child_handles, mut self_handles) = handles.await;
        let child = builder.build((counter, order), child_handles).await?;
        child.await.map_err(TaskError::from)?;
        if let Some(metric) = self_handles.duration.next().await {
            tracing::info!(duration = metric.duration_millis, "received child metrics");
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl SpawnWorkflow for PizzaDeliveryWithSpawning {
    async fn spawn(args: Args, handle: PizzaDelivery) -> TaskResult {
        handle.spawn_with_child_workflows(args).await
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DurationMetric {
    index: usize,
    duration_millis: u64,
}

#[derive(GetInterface, WithHandle, WorkflowEntry)]
#[tardigrade(derive(Debug), interface = "src/tardigrade-baking.json")]
pub struct Baking<Fmt: HandleFormat = Wasm> {
    pub events: InEnv<Sender<DomainEvent, Json>, Fmt>,
    pub duration: InEnv<Sender<DurationMetric, Json>, Fmt>,
}

impl WorkflowFn for Baking {
    type Args = (usize, PizzaOrder);
    type Codec = Json;
}

#[async_trait(?Send)]
impl SpawnWorkflow for Baking {
    async fn spawn((idx, order): (usize, PizzaOrder), mut handle: Self) -> TaskResult {
        let start = tardigrade::now();
        let shared = SharedHandle {
            events: handle.events,
        };
        shared.bake(idx, order).await;

        let metric = DurationMetric {
            index: idx,
            duration_millis: (tardigrade::now() - start).num_milliseconds() as u64,
        };
        tracing::info!(?metric, "sent baking duration metric");
        handle.duration.send(metric).await.ok();
        Ok(())
    }
}
