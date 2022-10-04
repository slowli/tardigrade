//! Version of the `PizzaDelivery` workflow with each order implemented via a separate workflow.
//! Also, there is no delivery.

use futures::{FutureExt, StreamExt};

use crate::{Args, PizzaDeliveryHandle, PizzaOrder, SharedHandle};
use tardigrade::{
    spawn::{ManageWorkflowsExt, WorkflowBuilder, Workflows},
    workflow::{GetInterface, SpawnWorkflow, TakeHandle, TaskHandle, Wasm, WorkflowFn},
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
    async fn spawn_with_child_workflows(self, args: Args) {
        let mut counter = 0;
        let events = self.shared.events;
        self.orders
            .for_each_concurrent(args.oven_count, |order| {
                counter += 1;
                let builder: WorkflowBuilder<_, Baking> =
                    Workflows.new_workflow("baking", (counter, order)).unwrap();
                builder.handle().events.copy_from(events.clone());
                builder.handle().tracer.close();
                builder.build().unwrap().workflow.map(Result::unwrap)
            })
            .await;
    }
}

impl SpawnWorkflow for PizzaDeliveryWithSpawning {
    fn spawn(args: Args, handle: PizzaDeliveryHandle) -> TaskHandle {
        TaskHandle::new(handle.spawn_with_child_workflows(args))
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

impl SpawnWorkflow for Baking {
    fn spawn((idx, order): (usize, PizzaOrder), handle: SharedHandle<Wasm>) -> TaskHandle {
        TaskHandle::new(async move {
            handle.bake(idx, order).await;
        })
    }
}

tardigrade::workflow_entry!(Baking);
