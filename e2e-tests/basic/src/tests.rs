//! Tests that use the test wrapper from `tardigrade` library.
//!
//! The wrapper emulates the Tardigrade environment without requiring compilation to WASM.

use assert_matches::assert_matches;
use futures::{stream, FutureExt, SinkExt};

use super::*;
use tardigrade::{
    test::{TestHandle, TestWorkflow},
    trace::FutureState,
};

async fn test_workflow_basics(handle: TestHandle<PizzaDelivery>) {
    let TestHandle {
        mut api, timers, ..
    } = handle;
    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    api.orders.send(order).await;

    let event = api.shared.events.next().await.unwrap();
    assert_eq!(event, DomainEvent::OrderTaken { index: 1, order });
    {
        let tracer_handle = &mut api.shared.tracer;
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

    let now = timers.now();
    let timer_expiration = timers.next_timer_expiration().unwrap();
    assert_eq!((timer_expiration - now).num_milliseconds(), 50);
    timers.set_now(timer_expiration);
    let event = api.shared.events.next().await.unwrap();
    assert_eq!(event, DomainEvent::Baked { index: 1, order });
    {
        let tracer_handle = &mut api.shared.tracer;
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
    let inputs = Inputs {
        oven_count: 1,
        deliverer_count: 1,
    };
    PizzaDelivery::test(inputs, test_workflow_basics);
}

async fn test_concurrency_in_workflow(handle: TestHandle<PizzaDelivery>) {
    let TestHandle {
        mut api, timers, ..
    } = handle;
    let orders = [
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
        .send_all(&mut stream::iter(orders).map(Ok))
        .await
        .unwrap();

    let events: Vec<_> = api.shared.events.by_ref().take(2).collect().await;
    assert_matches!(
        events.as_slice(),
        [
            DomainEvent::OrderTaken { .. },
            DomainEvent::OrderTaken { .. },
        ]
    );

    let now = timers.now();
    let next_timer = timers.next_timer_expiration().unwrap();
    assert_eq!((next_timer - now).num_milliseconds(), 40);
    timers.set_now(next_timer);

    let events: Vec<_> = api.shared.events.by_ref().take(2).collect().await;
    assert_matches!(
        events.as_slice(),
        [
            DomainEvent::Baked { index: 2, .. },
            DomainEvent::StartedDelivering { index: 2, .. },
        ]
    );
    assert!(api.shared.events.next().now_or_never().is_none());

    let now = timers.now();
    let next_timer = timers.next_timer_expiration().unwrap();
    assert_eq!((next_timer - now).num_milliseconds(), 10);
    timers.set_now(next_timer);

    tardigrade::yield_now().await;
    assert!(api.shared.events.next().now_or_never().is_none());
    // You'd think that `DomainEvent::Baked { index: 1, .. }` would be triggered,
    // but this is not the case! In reality, the 1st order will only be polled once
    // the 2nd one is delivered, due to the deliverers bottleneck.
}

#[test]
fn concurrency_in_workflow() {
    let inputs = Inputs {
        oven_count: 2,
        deliverer_count: 1,
    };
    PizzaDelivery::test(inputs, test_concurrency_in_workflow);
}
