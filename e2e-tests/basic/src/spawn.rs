//! Version of the `PizzaDelivery` workflow with each order implemented via a separate workflow.
//! Also, there is no delivery.

use futures::{FutureExt, StreamExt};

use crate::{Args, PizzaDelivery, PizzaOrder, Shared};
use tardigrade::{
    spawn::{ManageWorkflowsExt, WorkflowBuilder, Workflows},
    workflow::{GetInterface, Handle, SpawnWorkflow, TaskHandle, Wasm, WorkflowFn},
    Json,
};

#[derive(Debug, GetInterface)]
pub struct PizzaDeliveryWithSpawning(());

impl WorkflowFn for PizzaDeliveryWithSpawning {
    type Args = Args;
    type Codec = Json;
}

#[tardigrade::handle(for = "PizzaDeliveryWithSpawning")]
#[derive(Debug)]
pub struct WorkflowHandle<Env> {
    #[tardigrade(flatten)]
    pub inner: Handle<PizzaDelivery, Env>,
}

impl WorkflowHandle<Wasm> {
    async fn spawn(self, args: Args) {
        let mut counter = 0;
        self.inner
            .orders
            .for_each_concurrent(args.oven_count, |order| {
                counter += 1;
                let builder: WorkflowBuilder<_, Baking> =
                    Workflows.new_workflow("baking", (counter, order)).unwrap();
                // TODO: proxy events / traces.
                builder.handle().inner.events.close();
                builder.handle().inner.tracer.close();
                builder.build().unwrap().workflow.map(Result::unwrap)
            })
            .await;
    }
}

impl SpawnWorkflow for PizzaDeliveryWithSpawning {
    fn spawn(args: Args, handle: Self::Handle) -> TaskHandle {
        TaskHandle::new(handle.spawn(args))
    }
}

tardigrade::workflow_entry!(PizzaDeliveryWithSpawning);

#[derive(Debug, GetInterface)]
#[tardigrade(interface = "file:src/tardigrade-baking.json")]
pub struct Baking(());

impl WorkflowFn for Baking {
    type Args = (usize, PizzaOrder);
    type Codec = Json;
}

#[tardigrade::handle(for = "Baking")]
#[derive(Debug)]
pub struct BakingHandle<Env> {
    #[tardigrade(flatten)]
    inner: Handle<Shared, Env>,
}

impl SpawnWorkflow for Baking {
    fn spawn((idx, order): (usize, PizzaOrder), handle: Self::Handle) -> TaskHandle {
        TaskHandle::new(async move {
            handle.inner.bake(idx, order).await;
        })
    }
}

tardigrade::workflow_entry!(Baking);
