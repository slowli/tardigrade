use assert_matches::assert_matches;
use chrono::Duration;
use once_cell::sync::Lazy;

use std::{error, task::Poll};

use tardigrade::trace::FutureState;
use tardigrade_rt::{
    receipt::{ChannelEvent, ChannelEventKind, Event, ExecutedFunction, WakeUpCause},
    test::{ModuleCompiler, WasmOpt},
    PersistError, Workflow, WorkflowEngine, WorkflowModule,
};
use tardigrade_test_basic::{DomainEvent, Inputs, PizzaDelivery, PizzaKind, PizzaOrder};

static COMPILER: Lazy<ModuleCompiler> = Lazy::new(|| {
    let mut compiler = ModuleCompiler::new(env!("CARGO_PKG_NAME"));
    compiler
        .set_profile("wasm")
        .set_stack_size(32_768)
        .set_wasm_opt(WasmOpt::default());
    compiler
});

#[test]
fn basic_workflow() -> Result<(), Box<dyn error::Error>> {
    let module_bytes = COMPILER.compile();
    let engine = WorkflowEngine::default();
    let module = WorkflowModule::<PizzaDelivery>::new(&engine, &module_bytes)?;

    let inputs = Inputs {
        oven_count: 1,
        deliverer_count: 1,
    };
    let (mut workflow, receipt) = Workflow::new(&module, inputs.into())?;

    assert_eq!(receipt.executions().len(), 1);
    let execution = &receipt.executions()[0];
    assert_matches!(
        &execution.function,
        ExecutedFunction::Task {
            wake_up_cause: WakeUpCause::Spawned(inner_fn),
            poll_result: Poll::Pending,
            ..
        } if matches!(inner_fn.as_ref(), ExecutedFunction::Entry)
    );

    assert_eq!(execution.events.len(), 1);
    assert_matches!(
        &execution.events[0],
        Event::Channel(ChannelEvent {
            kind: ChannelEventKind::InboundChannelPolled,
            result: Poll::Pending,
            channel_name,
            ..
        }) if channel_name == "orders"
    );

    let mut tasks: Vec<_> = workflow.tasks().collect();
    assert_eq!(tasks.len(), 1);
    let (_, main_task) = tasks.pop().unwrap();
    assert_matches!(main_task.spawned_by(), ExecutedFunction::Entry);
    assert_eq!(workflow.timers().count(), 0);

    let mut handle = workflow.handle();

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle.interface.orders.send(order)?;
    let receipt = handle.with(Workflow::tick)?;
    dbg!(&receipt); // FIXME: assert on receipt

    assert_eq!(handle.interface.shared.events.message_indices(), 0..1);
    let events = handle.interface.shared.events.flush_messages()?;
    let events = events.into_output();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], DomainEvent::OrderTaken { index: 1, order });

    let receipt = handle.with(|workflow| {
        let timers: Vec<_> = workflow.timers().collect();
        assert_eq!(timers.len(), 1);
        let (_, timer) = timers[0];
        assert!(!timer.is_completed());
        assert!(timer.definition().expires_at > workflow.current_time());

        let new_time = workflow.current_time() + Duration::milliseconds(100);
        workflow.set_current_time(new_time)
    })?;
    dbg!(&receipt); // FIXME: assert on receipt

    let events = handle.interface.shared.events.flush_messages()?;
    let events = events.into_output();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], DomainEvent::Baked { index: 1, order });

    let tracer_handle = &mut handle.interface.shared.tracer;
    tracer_handle.flush()?;
    let mut futures = tracer_handle.futures().map(|(_, state)| state);
    let baking_future = futures
        .find(|&future| future.name() == "baking_timer")
        .unwrap();
    assert_matches!(baking_future.state(), FutureState::Dropped);

    Ok(())
}

#[test]
fn restoring_workflow() -> Result<(), Box<dyn error::Error>> {
    let module_bytes = COMPILER.compile();
    let engine = WorkflowEngine::default();
    let module = WorkflowModule::<PizzaDelivery>::new(&engine, &module_bytes)?;

    let inputs = Inputs {
        oven_count: 1,
        deliverer_count: 1,
    };
    let (mut workflow, _) = Workflow::new(&module, inputs.into())?;
    let mut handle = workflow.handle();

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle.interface.orders.send(order)?;
    handle.with(Workflow::tick)?;

    let err = handle.with(|workflow| workflow.persist().unwrap_err());
    assert_matches!(err, PersistError::PendingOutboundMessage { .. });

    let events = handle.interface.shared.events.flush_messages()?;
    assert_eq!(
        events.into_output(),
        [DomainEvent::OrderTaken { index: 1, order }]
    );

    let err = handle.with(|workflow| workflow.persist().unwrap_err());
    assert_matches!(
        err,
        PersistError::PendingOutboundMessage { channel_name } if channel_name == "traces"
    );

    handle.interface.shared.tracer.flush()?;
    let persisted = workflow.persist()?;
    let mut workflow = Workflow::restored(&module, persisted)?;
    let mut handle = workflow.handle();

    handle.with(|workflow| {
        let new_time = workflow.current_time() + Duration::milliseconds(100);
        workflow.set_current_time(new_time)
    })?;

    // Check that the pizza is ready now.
    let events = handle.interface.shared.events.flush_messages()?;
    assert_eq!(
        events.into_output(),
        [DomainEvent::Baked { index: 1, order }]
    );
    // We need to flush a second time to get the "started delivering" event.
    let events = handle.interface.shared.events.flush_messages()?;
    assert_eq!(
        events.into_output(),
        [DomainEvent::StartedDelivering { index: 1, order }]
    );

    // Check that the delivery timer is now active.
    let tracer_handle = &mut handle.interface.shared.tracer;
    tracer_handle.flush()?;
    let mut futures = tracer_handle.futures().map(|(_, state)| state);
    let delivery_future = futures
        .find(|&future| future.name() == "delivery_timer")
        .unwrap();
    assert_matches!(delivery_future.state(), FutureState::Polling);

    Ok(())
}
