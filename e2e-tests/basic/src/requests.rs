//! Version of the `PizzaDelivery` workflow with timers replaced with external tasks.
//! Also, we don't do delivery.

use async_trait::async_trait;
use futures::{Future, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{DomainEvent, PizzaOrder, SharedHandle};
use tardigrade::{
    channel::{Receiver, RequestHandles, Requests},
    task::TaskResult,
    workflow::{
        GetInterface, HandleFormat, InEnv, SpawnWorkflow, Wasm, WithHandle, WorkflowEntry,
        WorkflowFn,
    },
    Json,
};

#[derive(GetInterface, WithHandle, WorkflowEntry)]
#[tardigrade(derive(Debug), interface = "src/tardigrade-req.json")]
pub struct PizzaDeliveryWithRequests<Fmt: HandleFormat = Wasm> {
    pub orders: InEnv<Receiver<PizzaOrder, Json>, Fmt>,
    #[tardigrade(flatten)]
    pub shared: SharedHandle<Fmt>,
    pub baking: RequestHandles<PizzaOrder, (), Json, Fmt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Args {
    pub oven_count: usize,
}

#[test]
fn interface_agrees_between_declaration_and_handle() {
    PizzaDeliveryWithRequests::interface(); // Checks are performed internally
}

impl WorkflowFn for PizzaDeliveryWithRequests {
    type Args = Args;
    type Codec = Json;
}

#[async_trait(?Send)]
impl SpawnWorkflow for PizzaDeliveryWithRequests {
    async fn spawn(args: Self::Args, handle: Self) -> TaskResult {
        handle.spawn(args).await;
        Ok(())
    }
}

impl PizzaDeliveryWithRequests {
    fn spawn(self, args: Args) -> impl Future<Output = ()> {
        let (requests, requests_task) = self
            .baking
            .process_requests()
            .with_capacity(args.oven_count)
            .with_task_name("baking")
            .build();
        let shared = self.shared;

        let mut counter = 0;
        async move {
            let order_processing = self.orders.for_each_concurrent(None, |order| {
                counter += 1;
                shared.bake_with_requests(&requests, order, counter)
            });
            order_processing.await;

            // Ensure that background processing stops before terminating the workflow.
            // Otherwise, we might not get some responses after the `orders` channel
            // is closed.
            drop(requests);
            requests_task.await.ok();
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
