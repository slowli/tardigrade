//! Version of the `PizzaDelivery` workflow with timers replaced with external tasks.
//! Also, we don't do delivery.

use futures::{Future, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{DomainEvent, PizzaOrder, Shared, SharedHandle};
use tardigrade::{
    channel::{Receiver, Requests, Sender, WithId},
    workflow::{GetInterface, Handle, SpawnWorkflow, TaskHandle, Wasm, WorkflowFn},
    FutureExt as _, Json,
};

#[derive(Debug, GetInterface)]
#[tardigrade(interface = "file:src/tardigrade-tasks.json")]
pub struct PizzaDeliveryWithTasks(());

#[tardigrade::handle(for = "PizzaDeliveryWithTasks")]
#[derive(Debug)]
pub struct WorkflowHandle<Env> {
    pub orders: Handle<Receiver<PizzaOrder, Json>, Env>,
    #[tardigrade(flatten)]
    pub shared: Handle<Shared, Env>,
    pub baking_tasks: Handle<Sender<WithId<PizzaOrder>, Json>, Env>,
    pub baking_responses: Handle<Receiver<WithId<()>, Json>, Env>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Args {
    pub oven_count: usize,
}

#[test]
fn interface_agrees_between_declaration_and_handle() {
    PizzaDeliveryWithTasks::interface(); // Checks are performed internally
}

impl WorkflowFn for PizzaDeliveryWithTasks {
    type Args = Args;
    type Codec = Json;
}

impl SpawnWorkflow for PizzaDeliveryWithTasks {
    fn spawn(args: Self::Args, handle: Self::Handle) -> TaskHandle {
        TaskHandle::new(handle.spawn(args))
    }
}

tardigrade::workflow_entry!(PizzaDeliveryWithTasks);

impl WorkflowHandle<Wasm> {
    fn spawn(self, args: Args) -> impl Future<Output = ()> {
        let requests = Requests::builder(self.baking_tasks, self.baking_responses)
            .with_capacity(args.oven_count)
            .with_task_name("baking_requests")
            .build();
        let shared = self.shared;

        let mut counter = 0;
        async move {
            let order_processing = self.orders.for_each_concurrent(None, |order| {
                counter += 1;
                shared
                    .bake_with_requests(&requests, order, counter)
                    .trace(&shared.tracer, format!("baking order {}", counter))
            });
            order_processing.await
        }
    }
}

impl SharedHandle<Wasm> {
    async fn bake_with_requests(
        &self,
        requests: &Requests<PizzaOrder, ()>,
        order: PizzaOrder,
        index: usize,
    ) {
        let mut events = self.events.clone();
        events
            .send(DomainEvent::OrderTaken { index, order })
            .await
            .ok();
        if requests.request(order).await.is_err() {
            return; // The request loop was terminated; thus, the pizza will never be baked :(
        }
        events.send(DomainEvent::Baked { index, order }).await.ok();
    }
}
