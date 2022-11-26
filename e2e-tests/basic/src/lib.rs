//! Tardigrade workflow example implementing pizza shop business process
//! (baking and delivery).

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::Instrument;

use std::time::Duration;

use tardigrade::{
    channel::{Receiver, Sender},
    sleep,
    task::TaskResult,
    workflow::{GetInterface, Handle, SpawnWorkflow, TakeHandle, Wasm, WorkflowFn},
    Json,
};

pub mod requests;
pub mod spawn;
pub mod tasks;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

/// Orders sent to the workflow via a channel (see [`PizzaDeliveryHandle`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PizzaOrder {
    pub kind: PizzaKind,
    pub delivery_distance: u64,
}

/// Domain events emitted by the workflow and sent via the corresponding channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// Cloneable part of the workflow handle consisting of its channel senders.
#[tardigrade::handle]
// ^ Proc macro that derives some helper traits for the handle.
#[derive(Debug, Clone)]
pub struct SharedHandle<Env> {
    // For the proc macro to work, fields need to be defined as `Handle<T, Env>`, where
    // `T` describes a workflow element (in this case, a receiver).
    pub events: Handle<Sender<DomainEvent, Json>, Env>,
}

/// Handle for the workflow.
#[tardigrade::handle]
#[derive(Debug)]
pub struct PizzaDeliveryHandle<Env = Wasm> {
    pub orders: Handle<Receiver<PizzaOrder, Json>, Env>,
    #[tardigrade(flatten)]
    pub shared: Handle<SharedHandle<Wasm>, Env>,
}

/// Marker workflow type.
// `GetInterface` derive macro picks up the workflow interface definition at `tardigrade.json`
// and implements the corresponding trait based on it. It also exposes the interface definition
// in a custom WASM section, so that it is available to the workflow runtime.
#[derive(Debug, GetInterface, TakeHandle)]
#[tardigrade(handle = "PizzaDeliveryHandle")]
pub struct PizzaDelivery(());

// The `GetInterface` implementation ensures (unfortunately, in runtime) that
// the handle corresponds to the interface declaration.
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
#[async_trait(?Send)]
impl SpawnWorkflow for PizzaDelivery {
    async fn spawn(args: Args, handle: PizzaDeliveryHandle) -> TaskResult {
        handle.spawn(args).await;
        Ok(())
    }
}

// Defines the entry point for the workflow.
tardigrade::workflow_entry!(PizzaDelivery);

impl PizzaDeliveryHandle {
    /// This is where the actual workflow logic is contained. We pass incoming orders
    /// through 2 unordered buffers with the capacities defined by the workflow arguments.
    #[tracing::instrument(skip(self))]
    async fn spawn(self, args: Args) {
        let shared = self.shared;
        let mut counter = 0;
        let baked_pizzas = self
            .orders
            .map(|order| {
                counter += 1;
                shared.bake(counter, order)
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
    #[tracing::instrument(skip(self))]
    async fn bake(&self, index: usize, order: PizzaOrder) -> (usize, PizzaOrder) {
        let mut events = self.events.clone();
        let event = DomainEvent::OrderTaken { index, order };
        events.send(event).await.ok();
        tracing::info!(?event, "sent event");

        sleep(order.kind.baking_time())
            .instrument(tracing::info_span!("baking_timer", index, ?order.kind))
            .await;

        let event = DomainEvent::Baked { index, order };
        events.send(event).await.ok();
        tracing::info!(?event, "sent event");
        (index, order)
    }

    #[tracing::instrument(skip(self))]
    async fn deliver(&self, index: usize, order: PizzaOrder) {
        let mut events = self.events.clone();
        let event = DomainEvent::StartedDelivering { index, order };
        events.send(event).await.ok();
        tracing::info!(?event, "sent event");

        let delay = Duration::from_millis(order.delivery_distance * 10);
        let sleep_span = tracing::info_span!("delivery_timer", index, order.delivery_distance);
        sleep(delay).instrument(sleep_span).await;

        let event = DomainEvent::Delivered { index, order };
        events.send(event).await.ok();
        tracing::info!(?event, "sent event");
    }
}
