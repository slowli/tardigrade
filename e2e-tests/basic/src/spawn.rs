//! Version of the `PizzaDelivery` workflow with each order implemented via a separate workflow.
//! Also, there is no delivery.

use async_trait::async_trait;
use futures::{SinkExt, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::{DomainEvent, PizzaDeliveryHandle, PizzaOrder, SharedHandle};
use tardigrade::{
    channel::Sender,
    spawn::{ManageWorkflowsExt, Workflows},
    task::{TaskError, TaskResult},
    workflow::{GetInterface, InEnv, SpawnWorkflow, TakeHandle, Wasm, WorkflowEnv, WorkflowFn},
    Json,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Args {
    pub oven_count: usize,
    pub collect_metrics: bool,
}

#[derive(Debug, GetInterface, TakeHandle)]
#[tardigrade(handle = "PizzaDeliveryHandle")]
pub struct PizzaDeliveryWithSpawning(());

impl WorkflowFn for PizzaDeliveryWithSpawning {
    type Args = Args;
    type Codec = Json;
}

impl PizzaDeliveryHandle {
    async fn spawn_with_child_workflows(self, args: Args) -> TaskResult {
        const DEFINITION_ID: &str = "test::Baking";

        let mut counter = 0;
        self.orders
            .map(Ok)
            .try_for_each_concurrent(args.oven_count, |order| {
                counter += 1;
                let events = self.shared.events.clone();
                async move {
                    let builder =
                        Workflows.new_workflow::<Baking>(DEFINITION_ID, (counter, order))?;
                    builder.handle().events.copy_from(events);
                    if !args.collect_metrics {
                        builder.handle().duration.close();
                    }

                    let mut handle = builder.build().await?;
                    handle.workflow.await.map_err(TaskError::from)?;
                    if let Some(metric) = handle.api.duration.next().await {
                        tracing::info!(duration = metric.duration_millis, "received child metrics");
                    }
                    Ok(())
                }
            })
            .await
    }
}

#[async_trait(?Send)]
impl SpawnWorkflow for PizzaDeliveryWithSpawning {
    async fn spawn(args: Args, handle: PizzaDeliveryHandle) -> TaskResult {
        handle.spawn_with_child_workflows(args).await
    }
}

tardigrade::workflow_entry!(PizzaDeliveryWithSpawning);

#[derive(Debug, Serialize, Deserialize)]
pub struct DurationMetric {
    index: usize,
    duration_millis: u64,
}

#[derive(TakeHandle)]
pub struct BakingHandle<Env: WorkflowEnv = Wasm> {
    pub events: InEnv<Sender<DomainEvent, Json>, Env>,
    pub duration: InEnv<Sender<DurationMetric, Json>, Env>,
}

#[derive(Debug, GetInterface, TakeHandle)]
#[tardigrade(handle = "BakingHandle", interface = "src/tardigrade-baking.json")]
pub struct Baking(());

impl WorkflowFn for Baking {
    type Args = (usize, PizzaOrder);
    type Codec = Json;
}

#[async_trait(?Send)]
impl SpawnWorkflow for Baking {
    async fn spawn((idx, order): (usize, PizzaOrder), mut handle: BakingHandle) -> TaskResult {
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

tardigrade::workflow_entry!(Baking);
