//! Tests that use the test wrapper from `tardigrade` library.
//!
//! The wrapper emulates the Tardigrade environment without requiring compilation to WASM.

use assert_matches::assert_matches;
use futures::{stream, FutureExt, SinkExt};
use tracing::{subscriber::DefaultGuard, Level, Subscriber};
use tracing_capture::{CaptureLayer, SharedStorage};
use tracing_subscriber::{
    filter::Targets, layer::SubscriberExt, registry::LookupSpan, FmtSubscriber,
};

use super::*;
use tardigrade::{
    test::{TestInstance, Timers},
    workflow::{InEnv, Inverse},
};

fn create_fmt_subscriber() -> impl Subscriber + for<'a> LookupSpan<'a> {
    const FILTER: &str = "tardigrade_pizza=debug,tardigrade=debug";

    FmtSubscriber::builder()
        .pretty()
        .with_test_writer()
        .with_env_filter(FILTER)
        .finish()
}

fn enable_tracing_assertions() -> (DefaultGuard, SharedStorage) {
    let storage = SharedStorage::default();
    let filter = Targets::new().with_target("tardigrade_pizza", Level::INFO);
    let layer = CaptureLayer::new(&storage).with_filter(filter);
    let subscriber = create_fmt_subscriber().with(layer);
    let guard = tracing::subscriber::set_default(subscriber);
    (guard, storage)
}

async fn test_workflow_basics(
    mut api: InEnv<PizzaDelivery, Inverse<Wasm>>,
    storage: SharedStorage,
) {
    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    api.orders.send(order).await.unwrap();

    let event = api.shared.events.next().await.unwrap();
    assert_eq!(event, DomainEvent::OrderTaken { index: 1, order });
    {
        let storage = storage.lock();
        let spawn_span = storage
            .all_spans()
            .find(|span| span.metadata().name() == "spawn")
            .unwrap();
        assert!(spawn_span["args"].is_debug(&Args {
            oven_count: 1,
            deliverer_count: 1,
        }));

        let baking_timer = storage
            .all_spans()
            .find(|span| span.metadata().name() == "baking_timer")
            .unwrap();
        assert_eq!(baking_timer["index"], 1_u64);
        assert!(baking_timer["order.kind"].is_debug(&PizzaKind::Pepperoni));
        assert_eq!(baking_timer.stats().entered, 1);
        assert!(!baking_timer.stats().is_closed);
    }

    let now = Timers::now();
    let timer_expiration = Timers::next_timer_expiration().unwrap();
    assert_eq!((timer_expiration - now).num_milliseconds(), 50);
    Timers::set_now(timer_expiration);
    let event = api.shared.events.next().await.unwrap();
    assert_eq!(event, DomainEvent::Baked { index: 1, order });

    let storage = storage.lock();
    let baking_timer = storage
        .all_spans()
        .find(|span| span.metadata().name() == "baking_timer")
        .unwrap();
    assert!(baking_timer.stats().is_closed);
}

#[test]
fn workflow_basics() {
    let (_guard, tracing_storage) = enable_tracing_assertions();
    let inputs = Args {
        oven_count: 1,
        deliverer_count: 1,
    };
    TestInstance::<PizzaDelivery>::new(inputs)
        .run(|handle| test_workflow_basics(handle, tracing_storage));
}

async fn test_concurrency_in_workflow(mut api: InEnv<PizzaDelivery, Inverse<Wasm>>) {
    let orders_array = [
        PizzaOrder {
            kind: PizzaKind::Pepperoni,
            delivery_distance: 10,
        },
        PizzaOrder {
            kind: PizzaKind::FourCheese,
            delivery_distance: 5,
        },
    ];
    api.orders
        .send_all(&mut stream::iter(orders_array).map(Ok))
        .await
        .unwrap();

    {
        let events: Vec<_> = api.shared.events.by_ref().take(2).collect().await;
        assert_matches!(
            events.as_slice(),
            [
                DomainEvent::OrderTaken { .. },
                DomainEvent::OrderTaken { .. },
            ]
        );
    }

    let now = Timers::now();
    let next_timer = Timers::next_timer_expiration().unwrap();
    assert_eq!((next_timer - now).num_milliseconds(), 40);
    Timers::set_now(next_timer);

    {
        let events: Vec<_> = api.shared.events.by_ref().take(2).collect().await;
        assert_matches!(
            events.as_slice(),
            [
                DomainEvent::Baked { index: 2, .. },
                DomainEvent::StartedDelivering { index: 2, .. },
            ]
        );
    }
    assert!(api.shared.events.next().now_or_never().is_none());

    let now = Timers::now();
    let next_timer = Timers::next_timer_expiration().unwrap();
    assert_eq!((next_timer - now).num_milliseconds(), 10);
    Timers::set_now(next_timer);

    tardigrade::task::yield_now().await;
    assert!(api.shared.events.next().now_or_never().is_none());
    // You'd think that `DomainEvent::Baked { index: 1, .. }` would be triggered,
    // but this is not the case! In reality, the 1st order will only be polled once
    // the 2nd one is delivered, due to the deliverers bottleneck.
}

#[test]
fn concurrency_in_workflow() {
    let inputs = Args {
        oven_count: 2,
        deliverer_count: 1,
    };
    TestInstance::<PizzaDelivery>::new(inputs).run(test_concurrency_in_workflow);
}
