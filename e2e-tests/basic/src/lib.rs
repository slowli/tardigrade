use futures::StreamExt;
use serde::{Deserialize, Serialize};

use std::time::Duration;

use tardigrade::{
    channel::{Receiver, Sender},
    sleep,
    trace::Tracer,
    workflow::{GetInterface, Initialize, InputsBuilder, TakeHandle, ValidateInterface},
    Data, FutureExt as _, Json, SpawnWorkflow, TaskHandle, Wasm,
};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PizzaKind {
    Pepperoni,
    Margherita,
    FourCheese,
}

impl PizzaKind {
    fn baking_time(self) -> Duration {
        Duration::from_millis(match self {
            Self::Pepperoni => 50,
            Self::Margherita => 75,
            Self::FourCheese => 40,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PizzaOrder {
    pub kind: PizzaKind,
    pub delivery_distance: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum DomainEvent {
    OrderTaken { index: usize, order: PizzaOrder },
    Baked { index: usize, order: PizzaOrder },
    StartedDelivering { index: usize, order: PizzaOrder },
    Delivered { index: usize, order: PizzaOrder },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Inputs {
    pub oven_count: usize,
    pub deliverer_count: usize,
}

#[derive(Debug, TakeHandle, ValidateInterface)]
pub struct Shared {
    pub events: Sender<DomainEvent, Json>,
    #[tardigrade(rename = "traces")]
    pub tracer: Tracer<Json>,
}

#[derive(Debug, TakeHandle, Initialize, ValidateInterface, GetInterface)]
pub struct PizzaDelivery {
    pub inputs: Data<Inputs, Json>,
    pub orders: Receiver<PizzaOrder, Json>,
    #[tardigrade(flatten)]
    pub shared: Shared,
}

impl From<Inputs> for PizzaDeliveryInit<InputsBuilder> {
    fn from(inputs: Inputs) -> Self {
        Self { inputs }
    }
}

impl SpawnWorkflow for PizzaDelivery {
    fn spawn(handle: Self::Handle) -> TaskHandle {
        TaskHandle::new(handle.spawn())
    }
}

tardigrade::workflow_entry!(PizzaDelivery);

impl PizzaDeliveryHandle<Wasm> {
    async fn spawn(self) {
        let inputs = self.inputs.into_inner();
        let shared = self.shared;

        let mut counter = 0;
        let baked_pizzas = self
            .orders
            .map(|order| {
                counter += 1;
                shared.bake(counter, order).trace(
                    &shared.tracer,
                    format!("baking_process (order={})", counter),
                )
            })
            .buffer_unordered(inputs.oven_count);

        baked_pizzas
            .map(|(index, order)| shared.deliver(index, order))
            .buffer_unordered(inputs.deliverer_count)
            .for_each(|()| async { /* do nothing, just await */ })
            .await;
    }
}

impl SharedHandle<Wasm> {
    async fn bake(&self, index: usize, order: PizzaOrder) -> (usize, PizzaOrder) {
        let mut events = self.events.clone();
        events.send(DomainEvent::OrderTaken { index, order }).await;
        sleep(order.kind.baking_time())
            .trace(&self.tracer, "baking_timer")
            .await;
        events.send(DomainEvent::Baked { index, order }).await;
        (index, order)
    }

    async fn deliver(&self, index: usize, order: PizzaOrder) {
        let mut events = self.events.clone();
        events
            .send(DomainEvent::StartedDelivering { index, order })
            .await;
        let delay = Duration::from_millis(order.delivery_distance * 100);
        sleep(delay).trace(&self.tracer, "delivery_timer").await;
        events.send(DomainEvent::Delivered { index, order }).await;
    }
}
