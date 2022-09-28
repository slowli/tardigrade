//! Tardigrade workflow example implementing pizza shop business process
//! (baking and delivery).

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

use std::time::Duration;

use tardigrade::{
    channel::{Receiver, Sender},
    sleep,
    trace::Tracer,
    workflow::{GetInterface, Handle, SpawnWorkflow, TaskHandle, Wasm, WorkflowFn},
    FutureExt as _, Json,
};

pub mod spawn;
pub mod tasks;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PizzaKind {
    Pepperoni,
    Margherita,
    FourCheese,
}

impl PizzaKind {
    pub fn baking_time(self) -> Duration {
        Duration::from_millis(match self {
            Self::Pepperoni => 50,
            Self::Margherita => 75,
            Self::FourCheese => 40,
        })
    }
}

/// Orders sent to the workflow via an inbound channel (see [`PizzaDeliveryHandle`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PizzaOrder {
    pub kind: PizzaKind,
    pub delivery_distance: u64,
}

/// Domain events emitted by the workflow and sent via the corresponding outbound channel.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainEvent {
    OrderTaken { index: usize, order: PizzaOrder },
    Baked { index: usize, order: PizzaOrder },
    StartedDelivering { index: usize, order: PizzaOrder },
    Delivered { index: usize, order: PizzaOrder },
}

impl DomainEvent {
    pub fn index(&self) -> usize {
        match self {
            Self::OrderTaken { index, .. }
            | Self::Baked { index, .. }
            | Self::StartedDelivering { index, .. }
            | Self::Delivered { index, .. } => *index,
        }
    }
}

/// Arguments necessary for the workflow initialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Args {
    pub oven_count: usize,
    pub deliverer_count: usize,
}

/// Marker for the cloneable part of the workflow.
#[derive(Debug)]
pub struct Shared(());

/// Cloneable part of the workflow handle consisting of its outbound channels.
#[tardigrade::handle(for = "Shared")]
// ^ Proc macro that ties `SharedHandle` to the marker type.
#[derive(Debug, Clone)]
pub struct SharedHandle<Env> {
    // For the proc macro to work, fields need to be defined as `Handle<T, Env>`, where
    // `T` describes a workflow element (in this case, an outbound channel).
    pub events: Handle<Sender<DomainEvent, Json>, Env>,
    #[tardigrade(rename = "traces")]
    // ^ Similar to `serde`, elements can be renamed using a field attribute.
    pub tracer: Handle<Tracer<Json>, Env>,
}

/// Marker workflow type.
// `GetInterface` derive macro picks up the workflow interface definition at `tardigrade.json`
// and implements the corresponding trait based on it. It also exposes the interface definition
// in a custom WASM section, so that it is available to the workflow runtime.
#[derive(Debug, GetInterface)]
pub struct PizzaDelivery(());

/// Handle for the workflow.
#[tardigrade::handle(for = "PizzaDelivery")]
#[derive(Debug)]
pub struct PizzaDeliveryHandle<Env = Wasm> {
    pub orders: Handle<Receiver<PizzaOrder, Json>, Env>,
    #[tardigrade(flatten)]
    pub shared: Handle<Shared, Env>,
}

// Besides defining `PizzaDeliveryHandle` as a handle for `PizzaDelivery`,
// the `handle` proc macro also provides a `ValidateInterface` implementation.
// This allows to ensure (unfortunately, in runtime) that the handle corresponds
// to the interface declaration.
#[test]
fn interface_agrees_between_declaration_and_handle() {
    PizzaDelivery::interface(); // Checks are performed internally
}

/// Defines workflow interface.
impl WorkflowFn for PizzaDelivery {
    type Args = Args;
    type Codec = Json;
}

/// Defines how workflow instances are spawned.
impl SpawnWorkflow for PizzaDelivery {
    fn spawn(args: Args, handle: Self::Handle) -> TaskHandle {
        TaskHandle::new(handle.spawn(args))
    }
}

// Defines the entry point for the workflow.
tardigrade::workflow_entry!(PizzaDelivery);

impl PizzaDeliveryHandle<Wasm> {
    /// This is where the actual workflow logic is contained. We pass incoming orders
    /// through 2 unordered buffers with the capacities defined by the workflow arguments.
    async fn spawn(self, args: Args) {
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
                // ^ `trace` extension for `Future`s allows tracing progress
                // for the target. Internally, it passes updates to the host
                // via an outbound channel.
            })
            .buffer_unordered(args.oven_count);

        baked_pizzas
            .map(|(index, order)| shared.deliver(index, order))
            .buffer_unordered(args.deliverer_count)
            .for_each(|()| async { /* do nothing, just await */ })
            .await;
    }
}

impl SharedHandle<Wasm> {
    async fn bake(&self, index: usize, order: PizzaOrder) -> (usize, PizzaOrder) {
        let mut events = self.events.clone();
        events
            .send(DomainEvent::OrderTaken { index, order })
            .await
            .ok();
        sleep(order.kind.baking_time())
            .trace(&self.tracer, "baking_timer")
            .await;
        events.send(DomainEvent::Baked { index, order }).await.ok();
        (index, order)
    }

    async fn deliver(&self, index: usize, order: PizzaOrder) {
        let mut events = self.events.clone();
        events
            .send(DomainEvent::StartedDelivering { index, order })
            .await
            .ok();
        let delay = Duration::from_millis(order.delivery_distance * 10);
        sleep(delay).trace(&self.tracer, "delivery_timer").await;
        events
            .send(DomainEvent::Delivered { index, order })
            .await
            .ok();
    }
}
