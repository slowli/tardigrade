//! Tests for the `PizzaDelivery` workflow that use `WorkflowEnv`.

use assert_matches::assert_matches;

use std::{sync::Arc, task::Poll};

use tardigrade::{
    interface::{InboundChannel, OutboundChannel},
    spawn::ManageWorkflowsExt,
    trace::FutureState,
    Decode, Encode, Json,
};
use tardigrade_rt::{
    manager::{WorkflowHandle, WorkflowManager},
    receipt::{
        ChannelEvent, ChannelEventKind, Event, ExecutedFunction, ExecutionError, WakeUpCause,
    },
    test::MockScheduler,
    WorkflowId,
};
use tardigrade_test_basic::{Args, DomainEvent, PizzaDelivery, PizzaKind, PizzaOrder};

use super::{TestResult, MODULE};

pub fn get_workflow(
    manager: &WorkflowManager,
    id: WorkflowId,
) -> WorkflowHandle<'_, PizzaDelivery> {
    manager
        .workflow(id)
        .unwrap()
        .downcast::<PizzaDelivery>()
        .unwrap()
}

#[test]
fn basic_workflow() -> TestResult {
    let clock = Arc::new(MockScheduler::default());
    let spawner = MODULE.for_workflow::<PizzaDelivery>()?;
    let mut manager = WorkflowManager::builder()
        .with_clock(clock.clone())
        .with_spawner("pizza", spawner)
        .build();

    let inputs = Args {
        oven_count: 1,
        deliverer_count: 1,
    };
    let workflow = manager
        .new_workflow::<PizzaDelivery>("pizza", inputs)?
        .build()?;
    let workflow_id = workflow.id();
    let receipt = manager.tick()?.into_inner()?;

    assert_eq!(receipt.executions().len(), 2);
    let init_execution = &receipt.executions()[0];
    assert_matches!(&init_execution.function, ExecutedFunction::Entry { .. });

    // This is the "main" execution of async code.
    let execution = &receipt.executions()[1];
    assert_matches!(
        &execution.function,
        ExecutedFunction::Task {
            wake_up_cause: WakeUpCause::Spawned,
            poll_result: Poll::Pending,
            ..
        }
    );

    assert_eq!(execution.events.len(), 1);
    assert_matches!(
        &execution.events[0],
        Event::Channel(ChannelEvent {
            kind: ChannelEventKind::InboundChannelPolled { result: Poll::Pending },
            channel_name,
            ..
        }) if channel_name == "orders"
    );

    let mut workflow = get_workflow(&manager, workflow_id);
    {
        let persisted = workflow.persisted();
        let mut tasks: Vec<_> = persisted.tasks().collect();
        assert_eq!(tasks.len(), 1);
        let (_, main_task) = tasks.pop().unwrap();
        assert_eq!(main_task.spawned_by(), None);
        assert_eq!(persisted.timers().count(), 0);
    }

    let mut handle = workflow.handle();
    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle.orders.send(order)?;
    let receipt = manager.tick().unwrap().into_inner()?;
    dbg!(&receipt); // TODO: assert on receipt

    let mut workflow = get_workflow(&manager, workflow_id);
    let mut handle = workflow.handle();
    let events = handle.shared.events.take_messages().unwrap();
    assert_eq!(events.message_indices(), 0..1);
    assert_eq!(
        events.decode()?,
        [DomainEvent::OrderTaken { index: 1, order }]
    );

    {
        let persisted = workflow.persisted();
        let pending_events: Vec<_> = persisted.pending_events().collect();
        assert_eq!(pending_events.len(), 1);
        assert_matches!(
            pending_events[0],
            WakeUpCause::Flush { workflow_id: None, channel_name, .. } if channel_name == "events"
        );
    }

    manager.tick()?.into_inner()?;
    let new_time = {
        let persisted = get_workflow(&manager, workflow_id).persisted();
        let timers: Vec<_> = persisted.timers().collect();
        assert_eq!(timers.len(), 1);
        let (_, timer) = timers[0];
        assert!(timer.completed_at().is_none());
        assert!(timer.definition().expires_at > persisted.current_time());
        timer.definition().expires_at
    };
    clock.set_now(new_time);
    manager.set_current_time(new_time);

    let receipt = manager.tick()?.into_inner()?;
    dbg!(&receipt); // TODO: assert on receipt
    let mut handle = get_workflow(&manager, workflow_id).handle();
    let events = handle.shared.events.take_messages().unwrap();
    assert_eq!(events.decode()?, [DomainEvent::Baked { index: 1, order }]);

    let tracer_handle = &mut handle.shared.tracer;
    tracer_handle.take_traces()?;
    let mut futures = tracer_handle.futures().iter().map(|(_, state)| state);
    let baking_future = futures
        .find(|&future| future.name() == "baking_timer")
        .unwrap();
    assert_matches!(baking_future.state(), FutureState::Dropped);

    Ok(())
}

#[test]
fn workflow_with_concurrency() -> TestResult {
    let spawner = MODULE.for_workflow::<PizzaDelivery>()?;
    let mut manager = WorkflowManager::builder()
        .with_spawner("pizza", spawner)
        .build();

    let inputs = Args {
        oven_count: 2,
        deliverer_count: 1,
    };
    let workflow = manager
        .new_workflow::<PizzaDelivery>("pizza", inputs)?
        .build()?;
    let workflow_id = workflow.id();
    manager.tick()?.into_inner()?;

    let mut handle = get_workflow(&manager, workflow_id).handle();
    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle.orders.send(order)?;
    let other_order = PizzaOrder {
        kind: PizzaKind::FourCheese,
        delivery_distance: 10,
    };
    handle.orders.send(other_order)?;

    let mut message_indices = vec![];
    while let Ok(result) = manager.tick() {
        let receipt = result.into_inner()?;
        if let Some(WakeUpCause::InboundMessage { message_index, .. }) = receipt.root_cause() {
            message_indices.push(*message_index);
        }
    }
    assert_eq!(message_indices, [0, 1]);

    let mut handle = get_workflow(&manager, workflow_id).handle();
    let events = handle.shared.events.take_messages().unwrap();
    assert_eq!(
        events.decode()?,
        [
            DomainEvent::OrderTaken { index: 1, order },
            DomainEvent::OrderTaken {
                index: 2,
                order: other_order
            },
        ]
    );
    Ok(())
}

#[test]
fn persisting_workflow() -> TestResult {
    let clock = Arc::new(MockScheduler::default());
    let spawner = MODULE.for_workflow::<PizzaDelivery>()?;
    let mut manager = WorkflowManager::builder()
        .with_clock(Arc::clone(&clock))
        .with_spawner("pizza", spawner)
        .build();

    let inputs = Args {
        oven_count: 1,
        deliverer_count: 1,
    };
    let workflow = manager
        .new_workflow::<PizzaDelivery>("pizza", inputs)?
        .build()?;
    let workflow_id = workflow.id();
    manager.tick()?.into_inner()?;

    let mut handle = get_workflow(&manager, workflow_id).handle();
    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle.orders.send(order)?;
    manager.tick().unwrap().into_inner()?;

    let mut handle = get_workflow(&manager, workflow_id).handle();
    let events = handle.shared.events.take_messages().unwrap();
    assert_eq!(
        events.decode()?,
        [DomainEvent::OrderTaken { index: 1, order }]
    );

    handle.shared.tracer.take_traces()?;
    let traced_futures = handle.shared.tracer.into_futures();
    let persisted_json = manager.persist(serde_json::to_string)?;
    assert!(persisted_json.len() < 5_000, "{}", persisted_json);
    let persisted = serde_json::from_str(&persisted_json)?;
    let mut manager = WorkflowManager::builder()
        .with_clock(Arc::clone(&clock))
        .with_spawner("pizza", MODULE.for_workflow::<PizzaDelivery>()?)
        .restore(persisted)?;

    assert!(!manager.tick()?.into_inner()?.executions().is_empty());
    let new_time = clock.now() + chrono::Duration::milliseconds(100);
    clock.set_now(new_time);
    manager.set_current_time(new_time);
    assert!(!manager.tick()?.into_inner()?.executions().is_empty());

    // Check that the pizza is ready now.
    let mut handle = get_workflow(&manager, workflow_id).handle();
    let events = handle.shared.events.take_messages().unwrap();
    assert_eq!(events.decode()?, [DomainEvent::Baked { index: 1, order }]);

    // We need to flush a second time to get the "started delivering" event.
    manager.tick()?.into_inner()?;
    let mut handle = get_workflow(&manager, workflow_id).handle();
    let events = handle.shared.events.take_messages().unwrap();
    assert_eq!(
        events.decode()?,
        [DomainEvent::StartedDelivering { index: 1, order }]
    );
    // ...and flush again to activate the delivery timer
    manager.tick()?.into_inner()?;

    // Check that the delivery timer is now active.
    let mut handle = get_workflow(&manager, workflow_id).handle();
    let tracer_handle = &mut handle.shared.tracer;
    tracer_handle.set_futures(traced_futures);
    tracer_handle.take_traces()?;
    let mut futures = tracer_handle.futures().iter().map(|(_, state)| state);
    let delivery_future = futures
        .find(|&future| future.name() == "delivery_timer")
        .unwrap();
    assert_matches!(delivery_future.state(), FutureState::Polling);

    Ok(())
}

#[test]
fn untyped_workflow() -> TestResult {
    let spawner = MODULE.for_untyped_workflow("PizzaDelivery").unwrap();
    let mut manager = WorkflowManager::builder()
        .with_spawner("pizza", spawner)
        .build();

    let data = Json.encode_value(Args {
        oven_count: 1,
        deliverer_count: 1,
    });
    let workflow = manager.new_workflow::<()>("pizza", data)?.build()?;
    let workflow_id = workflow.id();
    let receipt = manager.tick()?.into_inner()?;
    assert_eq!(receipt.executions().len(), 2);

    let mut handle = manager.workflow(workflow_id).unwrap().handle();
    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle[InboundChannel("orders")].send(Json.encode_value(order))?;
    let receipt = manager.tick().unwrap().into_inner()?;
    dbg!(&receipt);

    let mut handle = manager.workflow(workflow_id).unwrap().handle();
    let events = handle[OutboundChannel("events")].take_messages().unwrap();
    assert_eq!(events.message_indices(), 0..1);
    let events: Vec<DomainEvent> = events
        .decode()?
        .into_iter()
        .map(|bytes| Json.decode_bytes(bytes))
        .collect();
    assert_eq!(events, [DomainEvent::OrderTaken { index: 1, order }]);

    let chan = handle[InboundChannel("orders")].channel_info();
    assert!(!chan.is_closed());
    assert_eq!(chan.received_messages(), 1);
    let chan = handle[OutboundChannel("events")].channel_info();
    assert_eq!(chan.flushed_messages(), 1);
    let chan = handle[OutboundChannel("traces")].channel_info();
    assert_eq!(chan.flushed_messages(), 0);
    Ok(())
}

#[test]
fn workflow_recovery_after_trap() -> TestResult {
    const SAMPLES: usize = 5;

    let spawner = MODULE.for_untyped_workflow("PizzaDelivery").unwrap();
    let mut manager = WorkflowManager::builder()
        .with_spawner("pizza", spawner)
        .build();

    let data = Json.encode_value(Args {
        oven_count: SAMPLES,
        deliverer_count: 1,
    });
    let workflow = manager.new_workflow::<()>("pizza", data)?.build()?;
    let workflow_id = workflow.id();
    manager.tick()?.into_inner()?;

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    let messages = (0..10).map(|i| {
        if i % 2 == 0 {
            b"invalid".to_vec()
        } else {
            Json.encode_value(order)
        }
    });

    for (i, message) in messages.enumerate() {
        let mut handle = manager.workflow(workflow_id).unwrap().handle();
        handle[InboundChannel("orders")].send(message)?;

        let result = loop {
            let tick_result = manager.tick().unwrap();
            let receipt = tick_result.as_ref().unwrap_or_else(ExecutionError::receipt);
            if matches!(
                receipt.root_cause(),
                Some(WakeUpCause::InboundMessage { .. })
            ) {
                break tick_result;
            }
        };

        if i % 2 == 0 {
            assert!(result.can_drop_erroneous_message());
            let err = result.drop_erroneous_message().into_inner().unwrap_err();
            let panic_info = err.panic_info().unwrap();
            let panic_message = panic_info.message.as_ref().unwrap();
            assert!(
                panic_message.starts_with("Cannot decode bytes"),
                "{}",
                panic_message
            );
            let panic_location = panic_info.location.as_ref().unwrap();
            assert!(
                panic_location.filename.ends_with("codec.rs"),
                "{:?}",
                panic_location
            );

            let err = err.to_string();
            assert!(err.contains("workflow execution failed"), "{}", err);
            assert!(err.contains("Cannot decode bytes"), "{}", err);
        } else {
            result.into_inner()?;

            let mut handle = manager.workflow(workflow_id).unwrap().handle();
            let events = handle[OutboundChannel("events")].take_messages().unwrap();
            assert_eq!(events.message_indices(), (i / 2)..(i / 2 + 1));
        }
    }
    Ok(())
}
