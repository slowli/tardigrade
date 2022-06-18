//! Tests that use the test wrapper.

use assert_matches::assert_matches;

use super::*;
use tardigrade::{
    test::{TestHandle, TestWorkflow},
    trace::FutureState,
};

async fn test_workflow_basics(handle: TestHandle<PizzaDelivery>) {
    let TestHandle {
        mut interface,
        timers,
        ..
    } = handle;
    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    interface.orders.send(order).await;

    let event = interface.shared.events.next().await.unwrap();
    assert_eq!(event, DomainEvent::OrderTaken { index: 1, order });
    {
        let tracer_handle = &mut interface.shared.tracer;
        tracer_handle.update();
        let traced_futures: Vec<_> = tracer_handle.futures().map(|(_, state)| state).collect();
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
    let updated_now = timers.advance_time_to_next_event();
    assert_eq!((updated_now - now).num_milliseconds(), 50);
    let event = interface.shared.events.next().await.unwrap();
    assert_eq!(event, DomainEvent::Baked { index: 1, order });
    {
        let tracer_handle = &mut interface.shared.tracer;
        tracer_handle.update();
        let traced_futures: Vec<_> = tracer_handle.futures().map(|(_, state)| state).collect();
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
    PizzaDelivery::test(inputs.into(), test_workflow_basics);
}
