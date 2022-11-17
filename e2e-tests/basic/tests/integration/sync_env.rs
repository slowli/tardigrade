//! Tests for the `PizzaDelivery` workflow that use `WorkflowEnv`.

use assert_matches::assert_matches;
use async_trait::async_trait;

use std::{collections::HashSet, task::Poll};

use crate::create_module;
use tardigrade::{
    interface::{InboundChannel, OutboundChannel},
    spawn::ManageWorkflowsExt,
    Decode, Encode, Json,
};
use tardigrade_rt::{
    manager::{AsManager, CreateModule, MessageReceiver, WorkflowManager},
    receipt::{
        ChannelEvent, ChannelEventKind, Event, ExecutedFunction, ExecutionError, WakeUpCause,
    },
    storage::{LocalStorageSnapshot, MessageError, ModuleRecord},
    test::MockScheduler,
    WorkflowModule,
};
use tardigrade_test_basic::{Args, DomainEvent, PizzaDelivery, PizzaKind, PizzaOrder};

use super::{create_manager, enable_tracing_assertions, TestResult};

const DEFINITION_ID: &str = "test::PizzaDelivery";

#[derive(Debug)]
struct Drain<'a, T, C, M> {
    receiver: MessageReceiver<'a, T, C, M>,
    cursor: usize,
}

impl<'a, T, C, M> Drain<'a, T, C, M>
where
    C: Decode<T> + Default,
    M: AsManager,
{
    fn new(receiver: MessageReceiver<'a, T, C, M>) -> Self {
        Self {
            receiver,
            cursor: 0,
        }
    }

    async fn drain(&mut self) -> TestResult<Vec<T>> {
        let mut messages = vec![];
        loop {
            match self.receiver.receive_message(self.cursor).await {
                Ok(message) => {
                    messages.push(message.decode()?);
                    self.cursor += 1;
                }
                Err(MessageError::NonExistingIndex { .. }) => {
                    self.receiver.truncate(self.cursor).await;
                    break Ok(messages);
                }
                Err(err) => break Err(err.into()),
            }
        }
    }
}

#[async_std::test]
async fn basic_workflow() -> TestResult {
    let (_guard, tracing_storage) = enable_tracing_assertions();
    let scheduler = MockScheduler::default();
    let manager = create_manager(scheduler.clone()).await?;

    let inputs = Args {
        oven_count: 1,
        deliverer_count: 1,
    };
    let mut workflow = manager
        .new_workflow::<PizzaDelivery>(DEFINITION_ID, inputs)?
        .build()
        .await?;
    let receipt = manager.tick().await?.into_inner()?;

    assert_eq!(receipt.executions().len(), 2);
    let init_execution = &receipt.executions()[0];
    assert_matches!(&init_execution.function, ExecutedFunction::Entry { .. });

    // This is the "main" execution of async code.
    let execution = &receipt.executions()[1];
    assert_matches!(
        &execution.function,
        ExecutedFunction::Task {
            wake_up_cause: WakeUpCause::Spawned,
            ..
        }
    );
    assert!(execution.task_result.is_none());

    assert_eq!(execution.events.len(), 1);
    assert_matches!(
        &execution.events[0],
        Event::Channel(ChannelEvent {
            kind: ChannelEventKind::InboundChannelPolled { result: Poll::Pending },
            channel_name,
            ..
        }) if channel_name == "orders"
    );

    workflow.update().await?;
    let persisted = workflow.persisted();
    let mut tasks: Vec<_> = persisted.tasks().collect();
    assert_eq!(tasks.len(), 1);
    let (_, main_task) = tasks.pop().unwrap();
    assert_eq!(main_task.spawned_by(), None);
    assert_eq!(persisted.timers().count(), 0);

    let mut handle = workflow.handle();
    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle.orders.send(order).await?;
    manager.tick().await?.into_inner()?; // TODO: assert on receipt

    let event = handle.shared.events.receive_message(0).await?;
    assert_eq!(event.decode()?, DomainEvent::OrderTaken { index: 1, order });

    workflow.update().await?;
    let persisted = workflow.persisted();
    let wakeup_causes: Vec<_> = persisted.pending_wakeup_causes().collect();
    assert_eq!(wakeup_causes.len(), 1);
    assert_matches!(
        wakeup_causes[0],
        WakeUpCause::Flush { workflow_id: None, channel_name, .. } if channel_name == "events"
    );

    manager.tick().await?.into_inner()?;
    {
        let storage = tracing_storage.lock();
        let baking_timer = storage
            .all_spans()
            .find(|span| span.metadata().name() == "baking_timer")
            .unwrap();
        assert_eq!(baking_timer["index"], 1_u64);
        assert!(baking_timer["order.kind"].is_debug(&PizzaKind::Pepperoni));
        assert_eq!(baking_timer.stats().entered, 1);
        assert!(!baking_timer.stats().is_closed);
    }

    let new_time = {
        workflow.update().await?;
        let persisted = workflow.persisted();
        let timers: Vec<_> = persisted.timers().collect();
        assert_eq!(timers.len(), 1);
        let (_, timer) = timers[0];
        assert!(timer.completed_at().is_none());
        assert!(timer.definition().expires_at > persisted.current_time());
        timer.definition().expires_at
    };
    scheduler.set_now(new_time);
    manager.set_current_time(new_time).await;

    manager.tick().await?.into_inner()?; // TODO: assert on receipt
    let events = handle.shared.events.receive_message(1).await?;
    assert_eq!(events.decode()?, DomainEvent::Baked { index: 1, order });

    {
        let storage = tracing_storage.lock();
        let mut captured_spans = storage.all_spans();
        // Check that the capturing layer properly does filtering.
        assert!(captured_spans.len() < 10, "{captured_spans:?}");

        let baking_timer = captured_spans
            .find(|span| span.metadata().name() == "baking_timer")
            .unwrap();
        let stats = baking_timer.stats();
        assert!(stats.entered > 1, "{stats:?}");
        assert!(stats.is_closed);
    }
    Ok(())
}

#[async_std::test]
async fn workflow_with_concurrency() -> TestResult {
    let manager = create_manager(()).await?;

    let inputs = Args {
        oven_count: 2,
        deliverer_count: 1,
    };
    let mut workflow = manager
        .new_workflow::<PizzaDelivery>(DEFINITION_ID, inputs)?
        .build()
        .await?;
    manager.tick().await?.into_inner()?;

    let mut handle = workflow.handle();
    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle.orders.send(order).await?;
    let other_order = PizzaOrder {
        kind: PizzaKind::FourCheese,
        delivery_distance: 10,
    };
    handle.orders.send(other_order).await?;

    let mut message_indices = HashSet::new();
    while let Ok(result) = manager.tick().await {
        let receipt = result.into_inner()?;
        let new_indices = receipt.executions().iter().filter_map(|execution| {
            if let Some(WakeUpCause::InboundMessage { message_index, .. }) = execution.cause() {
                Some(*message_index)
            } else {
                None
            }
        });
        message_indices.extend(new_indices);
    }
    assert_eq!(message_indices, HashSet::from_iter([0, 1]));

    let events = Drain::new(handle.shared.events).drain().await?;
    assert_eq!(
        events,
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

/// Workflow module creator that reuses the statically allocated module.
#[derive(Debug)]
struct DummyModuleCreator;

#[async_trait]
impl CreateModule for DummyModuleCreator {
    async fn create_module(&self, module: &ModuleRecord) -> anyhow::Result<WorkflowModule> {
        assert_eq!(module.id, "test");
        Ok(create_module().await)
    }
}

#[async_std::test]
async fn persisting_workflow() -> TestResult {
    let (_guard, tracing_storage) = enable_tracing_assertions();
    let clock = MockScheduler::default();
    let manager = create_manager(clock.clone()).await?;

    let inputs = Args {
        oven_count: 1,
        deliverer_count: 1,
    };
    let mut workflow = manager
        .new_workflow::<PizzaDelivery>(DEFINITION_ID, inputs)?
        .build()
        .await?;
    let workflow_id = workflow.id();
    let mut handle = workflow.handle();
    manager.tick().await?.into_inner()?;

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle.orders.send(order).await?;
    manager.tick().await?.into_inner()?;

    let mut events_drain = Drain::new(handle.shared.events);
    let events = events_drain.drain().await?;
    assert_eq!(events, [DomainEvent::OrderTaken { index: 1, order }]);
    let events_drain_cursor = events_drain.cursor;

    let mut storage = manager.into_storage();
    let mut snapshot = storage.snapshot();
    for mut module in snapshot.modules_mut() {
        assert_eq!(module.id, "test");
        module.set_bytes(vec![]);
    }
    let persisted_json = serde_json::to_string(&snapshot)?;
    assert!(persisted_json.len() < 5_000, "{persisted_json}");
    let snapshot: LocalStorageSnapshot<'_> = serde_json::from_str(&persisted_json)?;
    storage = snapshot.into();

    let manager = WorkflowManager::builder(storage)
        .with_clock(clock.clone())
        .with_module_creator(DummyModuleCreator)
        .build()
        .await?;

    assert!(!manager.tick().await?.into_inner()?.executions().is_empty());
    let new_time = clock.now() + chrono::Duration::milliseconds(100);
    clock.set_now(new_time);
    manager.set_current_time(new_time).await;
    assert!(!manager.tick().await?.into_inner()?.executions().is_empty());

    // Check that the pizza is ready now.
    let workflow = manager.workflow(workflow_id).await.unwrap();
    let mut workflow = workflow.downcast::<PizzaDelivery>()?;
    let mut events_drain = Drain::new(workflow.handle().shared.events);
    events_drain.cursor = events_drain_cursor;
    let events = events_drain.drain().await?;
    assert_eq!(events, [DomainEvent::Baked { index: 1, order }]);

    // We need to flush a second time to get the "started delivering" event.
    manager.tick().await?.into_inner()?;
    let events = events_drain.drain().await?;
    assert_eq!(events, [DomainEvent::StartedDelivering { index: 1, order }]);
    // ...and flush again to activate the delivery timer
    manager.tick().await?.into_inner()?;

    // Check that the delivery timer is now active.
    {
        let storage = tracing_storage.lock();
        let delivery_timer = storage
            .all_spans()
            .find(|span| span.metadata().name() == "delivery_timer")
            .unwrap();
        let stats = delivery_timer.stats();
        assert!(stats.entered > 0, "{stats:?}");
        assert!(!stats.is_closed);
    }
    Ok(())
}

#[async_std::test]
async fn untyped_workflow() -> TestResult {
    let manager = create_manager(()).await?;

    let data = Json.encode_value(Args {
        oven_count: 1,
        deliverer_count: 1,
    });
    let mut workflow = manager
        .new_workflow::<()>(DEFINITION_ID, data)?
        .build()
        .await?;
    let receipt = manager.tick().await?.into_inner()?;
    assert_eq!(receipt.executions().len(), 2);

    let mut handle = workflow.handle();
    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle[InboundChannel("orders")]
        .send(Json.encode_value(order))
        .await?;
    manager.tick().await?.into_inner()?; // TODO: assert on receipt

    let event = handle[OutboundChannel("events")].receive_message(0).await?;
    let event: DomainEvent = Json.try_decode_bytes(event.decode().unwrap())?;
    assert_eq!(event, DomainEvent::OrderTaken { index: 1, order });

    let chan = handle[InboundChannel("orders")].channel_info().await;
    assert!(!chan.is_closed);
    assert_eq!(chan.received_messages, 1);
    let chan = handle[OutboundChannel("events")].channel_info().await;
    assert_eq!(chan.received_messages, 1);
    Ok(())
}

#[async_std::test]
async fn workflow_recovery_after_trap() -> TestResult {
    const SAMPLES: usize = 5;

    let manager = create_manager(()).await?;

    let data = Json.encode_value(Args {
        oven_count: SAMPLES,
        deliverer_count: 1,
    });
    let mut workflow = manager
        .new_workflow::<()>(DEFINITION_ID, data)?
        .build()
        .await?;
    let mut handle = workflow.handle();
    let mut events_drain = Drain::new(handle.remove(OutboundChannel("events")).unwrap());
    manager.tick().await?.into_inner()?;

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
        handle[InboundChannel("orders")].send(message).await?;

        let result = loop {
            let tick_result = manager.tick().await?;
            let receipt = tick_result.as_ref().unwrap_or_else(ExecutionError::receipt);
            let consumed_message = receipt.executions().iter().any(|execution| {
                matches!(execution.cause(), Some(WakeUpCause::InboundMessage { .. }))
            });
            if consumed_message {
                break tick_result;
            }
        };

        if i % 2 == 0 {
            assert!(result.can_drop_erroneous_message());
            let err = result
                .drop_erroneous_message()
                .await
                .into_inner()
                .unwrap_err();
            let panic_info = err.panic_info().unwrap();
            let panic_message = panic_info.message.as_ref().unwrap();
            assert!(
                panic_message.starts_with("Cannot decode bytes"),
                "{panic_message}"
            );
            let panic_location = panic_info.location.as_ref().unwrap();
            assert!(
                panic_location.filename.ends_with("codec.rs"),
                "{panic_location:?}"
            );

            let err = err.to_string();
            assert!(err.contains("workflow execution failed"), "{err}");
            assert!(err.contains("Cannot decode bytes"), "{err}");
        } else {
            result.into_inner()?;
            let mut events = events_drain.drain().await?;
            assert_eq!(events.len(), 1);
            let event: DomainEvent = Json.try_decode_bytes(events.pop().unwrap())?;
            let expected_idx = (i + 1) / 2;
            assert_matches!(
                event,
                DomainEvent::OrderTaken { index, .. } if index == expected_idx
            );
        }
    }
    Ok(())
}
