use assert_matches::assert_matches;
use once_cell::sync::Lazy;

use std::{collections::HashMap, error, task::Poll};

use tardigrade::{
    interface::{InboundChannel, OutboundChannel},
    trace::FutureState,
    workflow::InputsBuilder,
    Decoder, Encoder, Json,
};
use tardigrade_rt::{
    handle::WorkflowEnv,
    receipt::{ChannelEvent, ChannelEventKind, Event, ExecutedFunction, WakeUpCause},
    test::{ModuleCompiler, WasmOpt},
    PersistError, PersistedWorkflow, WorkflowEngine, WorkflowModule,
};
use tardigrade_test_basic::{DomainEvent, Inputs, PizzaDelivery, PizzaKind, PizzaOrder};

mod async_env;
mod tasks;

static MODULE: Lazy<WorkflowModule> = Lazy::new(|| {
    let module_bytes = ModuleCompiler::new(env!("CARGO_PKG_NAME"))
        .set_current_dir(env!("CARGO_MANIFEST_DIR"))
        .set_profile("wasm")
        .set_wasm_opt(WasmOpt::default())
        .compile();
    let engine = WorkflowEngine::default();
    WorkflowModule::new(&engine, &module_bytes).unwrap()
});

#[test]
fn module_information_is_correct() -> Result<(), Box<dyn error::Error>> {
    let interfaces: HashMap<_, _> = MODULE.interfaces().collect();
    assert!(interfaces["PizzaDelivery"]
        .inbound_channel("orders")
        .is_some());
    assert!(interfaces["PizzaDelivery"]
        .inbound_channel("baking_responses")
        .is_none());
    assert!(interfaces["PizzaDeliveryWithTasks"]
        .inbound_channel("baking_responses")
        .is_some());
    Ok(())
}

#[test]
fn basic_workflow() -> Result<(), Box<dyn error::Error>> {
    let spawner = MODULE.for_workflow::<PizzaDelivery>()?;
    let inputs = Inputs {
        oven_count: 1,
        deliverer_count: 1,
    };
    let receipt = spawner.spawn(inputs)?;

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
            kind: ChannelEventKind::InboundChannelPolled { result: Poll::Pending },
            channel_name,
            ..
        }) if channel_name == "orders"
    );

    let mut workflow = receipt.into_inner();
    let mut tasks: Vec<_> = workflow.tasks().collect();
    assert_eq!(tasks.len(), 1);
    let (_, main_task) = tasks.pop().unwrap();
    assert_eq!(main_task.spawned_by(), None);
    assert_eq!(workflow.timers().count(), 0);

    let mut handle = WorkflowEnv::new(&mut workflow).handle();
    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    let receipt = handle.api.orders.send(order)?.flush()?;
    dbg!(&receipt); // FIXME: assert on receipt

    let events = handle.api.shared.events.take_messages()?.into_inner();
    assert_eq!(events.message_indices(), 0..1);
    assert_eq!(
        events.decode()?,
        [DomainEvent::OrderTaken { index: 1, order }]
    );

    let receipt = handle.with(|workflow| {
        let timers: Vec<_> = workflow.timers().collect();
        assert_eq!(timers.len(), 1);
        let (_, timer) = timers[0];
        assert!(timer.completed_at().is_none());
        assert!(timer.definition().expires_at > workflow.current_time());

        let new_time = workflow.current_time() + chrono::Duration::milliseconds(100);
        workflow.set_current_time(new_time)
    })?;
    dbg!(&receipt); // FIXME: assert on receipt

    let events = handle.api.shared.events.take_messages()?;
    assert_eq!(
        events.into_inner().decode()?,
        [DomainEvent::Baked { index: 1, order }]
    );

    let tracer_handle = &mut handle.api.shared.tracer;
    tracer_handle.take_traces()?;
    let mut futures = tracer_handle.futures().iter().map(|(_, state)| state);
    let baking_future = futures
        .find(|&future| future.name() == "baking_timer")
        .unwrap();
    assert_matches!(baking_future.state(), FutureState::Dropped);

    Ok(())
}

#[test]
fn workflow_with_concurrency() -> Result<(), Box<dyn error::Error>> {
    let spawner = MODULE.for_workflow::<PizzaDelivery>()?;
    let inputs = Inputs {
        oven_count: 2,
        deliverer_count: 1,
    };
    let mut workflow = spawner.spawn(inputs)?.into_inner();
    let mut handle = WorkflowEnv::new(&mut workflow).handle();

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle.api.orders.send(order)?.flush()?;

    let other_order = PizzaOrder {
        kind: PizzaKind::FourCheese,
        delivery_distance: 10,
    };
    handle.api.orders.send(other_order)?.flush()?;

    let events = handle.api.shared.events.take_messages()?;
    assert_eq!(
        events.into_inner().decode()?,
        [DomainEvent::OrderTaken { index: 1, order }]
    );

    let events = handle.api.shared.events.take_messages()?;
    assert_eq!(
        events.into_inner().decode()?,
        [DomainEvent::OrderTaken {
            index: 2,
            order: other_order
        }]
    );
    Ok(())
}

#[test]
fn restoring_workflow() -> Result<(), Box<dyn error::Error>> {
    let spawner = MODULE.for_workflow::<PizzaDelivery>()?;
    let inputs = Inputs {
        oven_count: 1,
        deliverer_count: 1,
    };
    let mut workflow = spawner.spawn(inputs)?.into_inner();
    let mut handle = WorkflowEnv::new(&mut workflow).handle();

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle.api.orders.send(order)?.flush()?;

    let err = handle.with(|workflow| workflow.persist().unwrap_err());
    assert_matches!(err, PersistError::PendingOutboundMessage { .. });

    let events = handle.api.shared.events.take_messages()?;
    assert_eq!(
        events.into_inner().decode()?,
        [DomainEvent::OrderTaken { index: 1, order }]
    );

    let err = handle.with(|workflow| workflow.persist().unwrap_err());
    assert_matches!(
        err,
        PersistError::PendingOutboundMessage { channel_name } if channel_name == "traces"
    );

    handle.api.shared.tracer.take_traces()?;
    let traced_futures = handle.api.shared.tracer.into_futures();
    let persisted = workflow.persist()?;
    let persisted_json = serde_json::to_string(&persisted)?;
    assert!(persisted_json.len() < 5_000, "{}", persisted_json);
    let persisted: PersistedWorkflow = serde_json::from_str(&persisted_json)?;

    let mut workflow = persisted.restore(&spawner)?;
    let mut env = WorkflowEnv::new(&mut workflow);
    env.extensions().insert(traced_futures);
    let mut handle = env.handle();

    handle.with(|workflow| {
        let new_time = workflow.current_time() + chrono::Duration::milliseconds(100);
        workflow.set_current_time(new_time)
    })?;

    // Check that the pizza is ready now.
    let events = handle.api.shared.events.take_messages()?;
    assert_eq!(
        events.into_inner().decode()?,
        [DomainEvent::Baked { index: 1, order }]
    );
    // We need to flush a second time to get the "started delivering" event.
    let events = handle.api.shared.events.take_messages()?;
    assert_eq!(
        events.into_inner().decode()?,
        [DomainEvent::StartedDelivering { index: 1, order }]
    );

    // Check that the delivery timer is now active.
    let tracer_handle = &mut handle.api.shared.tracer;
    tracer_handle.take_traces()?;
    let mut futures = tracer_handle.futures().iter().map(|(_, state)| state);
    let delivery_future = futures
        .find(|&future| future.name() == "delivery_timer")
        .unwrap();
    assert_matches!(delivery_future.state(), FutureState::Polling);

    Ok(())
}

#[test]
fn untyped_workflow() -> Result<(), Box<dyn error::Error>> {
    let spawner = MODULE.for_untyped_workflow("PizzaDelivery").unwrap();

    let mut builder = InputsBuilder::new(spawner.interface());
    builder.insert(
        "inputs",
        Json.encode_value(Inputs {
            oven_count: 1,
            deliverer_count: 1,
        }),
    );
    let receipt = spawner.spawn(builder.build())?;

    assert_eq!(receipt.executions().len(), 1);
    let mut workflow = receipt.into_inner();
    let mut handle = WorkflowEnv::new(&mut workflow).handle();

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    let receipt = handle.api[InboundChannel("orders")]
        .send(Json.encode_value(order))?
        .flush()?;
    dbg!(&receipt);

    let events = handle.api[OutboundChannel("events")]
        .take_messages()?
        .into_inner();
    assert_eq!(events.message_indices(), 0..1);
    let events: Vec<DomainEvent> = events
        .decode()?
        .into_iter()
        .map(|bytes| Json.decode_bytes(bytes))
        .collect();
    assert_eq!(events, [DomainEvent::OrderTaken { index: 1, order }]);

    Ok(())
}

#[test]
fn workflow_recovery_after_trap() -> Result<(), Box<dyn error::Error>> {
    const SAMPLES: usize = 5;

    let spawner = MODULE.for_untyped_workflow("PizzaDelivery").unwrap();

    let mut builder = InputsBuilder::new(spawner.interface());
    builder.insert(
        "inputs",
        Json.encode_value(Inputs {
            oven_count: SAMPLES,
            deliverer_count: 1,
        }),
    );
    let mut workflow = spawner.spawn(builder.build())?.into_inner();

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
        workflow.check_persistence()?;
        let result = workflow.revert_on_error(|workflow| {
            let mut handle = WorkflowEnv::new(workflow).handle();
            let sender = &mut handle.api[InboundChannel("orders")];
            sender.send(message).unwrap().flush()
        });

        if i % 2 == 0 {
            let err = result.unwrap_err().to_string();
            assert!(err.contains("workflow execution failed"), "{}", err);
        } else {
            result?;

            let mut handle = WorkflowEnv::new(&mut workflow).handle();
            let events = handle.api[OutboundChannel("events")]
                .take_messages()?
                .into_inner();
            assert_eq!(events.message_indices(), (i / 2)..(i / 2 + 1));

            handle.api[OutboundChannel("traces")].take_messages()?;
        }
    }
    Ok(())
}