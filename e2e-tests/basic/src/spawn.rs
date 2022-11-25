//! Version of the `PizzaDelivery` workflow with each order implemented via a separate workflow.
//! Also, there is no delivery.

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};

use crate::{Args, PizzaDeliveryHandle, PizzaOrder, SharedHandle};
use tardigrade::{
    spawn::{ManageWorkflowsExt, Workflows},
    task::{TaskError, TaskResult},
    workflow::{GetInterface, SpawnWorkflow, TakeHandle, Wasm, WorkflowFn},
    Json,
};

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
                    // FIXME: add another sender to close?
                    let handle = builder.build().await?;
                    handle.workflow.await.map_err(TaskError::from)
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

#[derive(Debug, GetInterface, TakeHandle)]
#[tardigrade(handle = "SharedHandle", interface = "src/tardigrade-baking.json")]
pub struct Baking(());

impl WorkflowFn for Baking {
    type Args = (usize, PizzaOrder);
    type Codec = Json;
}

#[async_trait(?Send)]
impl SpawnWorkflow for Baking {
    async fn spawn((idx, order): (usize, PizzaOrder), handle: SharedHandle<Wasm>) -> TaskResult {
        handle.bake(idx, order).await;
        Ok(())
    }
}

tardigrade::workflow_entry!(Baking);
