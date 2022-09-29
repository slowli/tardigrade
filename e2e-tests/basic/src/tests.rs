//! Tests that use the test wrapper from `tardigrade` library.
//!
//! The wrapper emulates the Tardigrade environment without requiring compilation to WASM.

use assert_matches::assert_matches;
use futures::{stream, FutureExt, SinkExt};

use super::*;
use tardigrade::{
    spawn::RemoteWorkflow,
    test::{Runtime, Timers},
    trace::FutureState,
    workflow::Handle,
};

async fn test_workflow_basics(mut api: Handle<PizzaDelivery, RemoteWorkflow>) {
    let mut tracer_handle = api.shared.tracer.unwrap();
    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    api.orders.send(order).await.unwrap();

    let event = api.shared.events.next().await.unwrap();
    assert_eq!(event, DomainEvent::OrderTaken { index: 1, order });
    {
        tracer_handle.update();
        let traced_futures: Vec<_> = tracer_handle
            .futures()
            .iter()
            .map(|(_, state)| state)
            .collect();
        assert_eq!(traced_futures.len(), 2);

        let baking_process_future = traced_futures
            .iter()
            .find(|&state| state.name() == "baking_process (order=1)")
            .unwrap();
        assert_matches!(baking_process_future.state(), FutureState::Polling);

        let baking_timer_future = traced_futures
            .iter()
            .find(|&state| state.name() == "baking_timer")
            .unwrap();
        assert_matches!(baking_timer_future.state(), FutureState::Polling);
    }

    let now = Timers::now();
    let timer_expiration = Timers::next_timer_expiration().unwrap();
    assert_eq!((timer_expiration - now).num_milliseconds(), 50);
    Timers::set_now(timer_expiration);
    let event = api.shared.events.next().await.unwrap();
    assert_eq!(event, DomainEvent::Baked { index: 1, order });
    {
        tracer_handle.update();
        let traced_futures: Vec<_> = tracer_handle
            .futures()
            .iter()
            .map(|(_, state)| state)
            .collect();
        assert_eq!(traced_futures.len(), 3);

        let baking_timer_future = traced_futures
            .iter()
            .find(|&state| state.name() == "baking_timer")
            .unwrap();
        assert_matches!(baking_timer_future.state(), FutureState::Dropped);
    }
}

#[test]
fn workflow_basics() {
    let inputs = Args {
        oven_count: 1,
        deliverer_count: 1,
    };
    Runtime::default().test::<PizzaDelivery, _, _>(inputs, test_workflow_basics);
}

async fn test_concurrency_in_workflow(mut api: Handle<PizzaDelivery, RemoteWorkflow>) {
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

    tardigrade::yield_now().await;
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
    Runtime::default().test::<PizzaDelivery, _, _>(inputs, test_concurrency_in_workflow);
}
