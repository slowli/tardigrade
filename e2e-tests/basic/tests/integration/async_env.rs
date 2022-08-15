//! Tests for the `PizzaDelivery` workflow that use `AsyncEnv`.

use assert_matches::assert_matches;
use async_std::task;
use futures::{
    channel::{mpsc, oneshot},
    future::{self, Either},
    stream, FutureExt, SinkExt, Stream, StreamExt, TryStreamExt,
};

use std::time::Duration;

use tardigrade::{
    interface::{InboundChannel, OutboundChannel},
    trace::FutureState,
    Decode, Encode, Json,
};
use tardigrade_rt::{
    handle::future::{
        AsyncEnv, AsyncIoScheduler, MessageReceiver, Rollback, Termination, TracerHandle,
    },
    receipt::{Event, ExecutionResult, Receipt, ResourceEvent, ResourceEventKind, ResourceId},
    test::MockScheduler,
    TimerId, Workflow, WorkflowSpawner,
};
use tardigrade_test_basic::{
    DomainEvent, Inputs, PizzaDelivery, PizzaDeliveryHandle, PizzaKind, PizzaOrder,
};

use super::{TestResult, MODULE};

async fn retry_until_some<T>(mut condition: impl FnMut() -> Option<T>) -> T {
    for _ in 0..5 {
        if let Some(value) = condition() {
            return value;
        }
        task::sleep(Duration::from_millis(10)).await;
    }
    panic!("Run out of attempts waiting for condition");
}

async fn test_async_handle(cancel_workflow: bool) -> TestResult {
    let module = task::spawn_blocking(|| &*MODULE).await;
    let spawner = module.for_workflow::<PizzaDelivery>()?;

    let inputs = Inputs {
        oven_count: 1,
        deliverer_count: 1,
    };
    let workflow = spawner.spawn(inputs)?.init()?.into_inner();
    let mut env = AsyncEnv::new(workflow, AsyncIoScheduler);
    let mut handle = env.handle();

    let (cancel_sx, mut cancel_rx) = oneshot::channel::<()>();
    let join_handle = task::spawn(async move {
        futures::select! {
            term = env.run().fuse() => Either::Left(term),
            _ = cancel_rx => Either::Right(env.into_inner()),
        }
    });

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle.orders.send(order).await?;

    let events: Vec<_> = handle.shared.events.take(4).try_collect().await?;
    assert_matches!(
        events.as_slice(),
        [
            DomainEvent::OrderTaken { .. },
            DomainEvent::Baked { .. },
            DomainEvent::StartedDelivering { .. },
            DomainEvent::Delivered { .. },
        ]
    );

    if cancel_workflow {
        cancel_sx.send(()).unwrap();
        let workflow = match join_handle.await {
            Either::Right(workflow) => workflow,
            other => panic!("expected cancelled workflow, got {:?}", other),
        };
        assert!(!workflow.is_finished());
        assert_eq!(workflow.timers().count(), 0);
    } else {
        drop(handle.orders); // should terminate the workflow
        assert_matches!(join_handle.await, Either::Left(Ok(Termination::Finished)));

        let mut tracer = handle.shared.tracer;
        tracer
            .by_ref()
            .try_for_each(|_| future::ready(Ok(())))
            .await?;
        assert!(tracer
            .futures()
            .iter()
            .all(|(_, fut)| fut.state() == FutureState::Dropped));
        assert_eq!(tracer.futures().len(), 3);
    }

    Ok(())
}

#[async_std::test]
async fn async_handle() -> TestResult {
    test_async_handle(false).await
}

#[async_std::test]
async fn async_handle_with_cancellation() -> TestResult {
    test_async_handle(true).await
}

async fn test_async_handle_with_concurrency(
    spawner: &WorkflowSpawner<PizzaDelivery>,
    inputs: Inputs,
) -> TestResult {
    const ORDER_COUNT: usize = 5;

    println!("testing async handle with {:?}", inputs);

    let workflow = spawner.spawn(inputs)?.init()?.into_inner();
    let mut env = AsyncEnv::new(workflow, AsyncIoScheduler);
    let mut handle = env.handle();
    let join_handle = task::spawn(async move { env.run().await });

    let orders = (0..ORDER_COUNT).map(|i| PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: i as u64,
    });
    let mut orders = stream::iter(orders).map(Ok);
    handle.orders.send_all(&mut orders).await?;
    drop(handle.orders); // will terminate the workflow eventually

    let events: Vec<_> = handle.shared.events.try_collect().await?;
    assert_eq!(events.len(), ORDER_COUNT * 4);

    for i in 1..=ORDER_COUNT {
        let order_events: Vec<_> = events.iter().filter(|event| event.index() == i).collect();
        assert_matches!(
            order_events.as_slice(),
            [
                DomainEvent::OrderTaken { .. },
                DomainEvent::Baked { .. },
                DomainEvent::StartedDelivering { .. },
                DomainEvent::Delivered { .. },
            ]
        );
    }

    join_handle.await?;
    Ok(())
}

#[async_std::test]
async fn async_handle_with_concurrency() -> TestResult {
    let module = task::spawn_blocking(|| &*MODULE).await;
    let spawner = module.for_workflow::<PizzaDelivery>()?;

    let sample_inputs = [
        Inputs {
            oven_count: 1,
            deliverer_count: 2,
        },
        Inputs {
            oven_count: 2,
            deliverer_count: 1,
        },
        Inputs {
            oven_count: 2,
            deliverer_count: 2,
        },
        Inputs {
            oven_count: 3,
            deliverer_count: 3,
        },
        Inputs {
            oven_count: 5,
            deliverer_count: 7,
        },
        Inputs {
            oven_count: 10,
            deliverer_count: 1,
        },
    ];

    for inputs in sample_inputs {
        test_async_handle_with_concurrency(&spawner, inputs).await?;
    }
    Ok(())
}

/// Checks whether a `receipt` contains the specified timer event.
fn check_timer(receipt: &Receipt, timer_id: TimerId, event_kind: ResourceEventKind) -> bool {
    receipt
        .executions()
        .iter()
        .flat_map(|execution| &execution.events)
        .any(|event| match event {
            Event::Resource(ResourceEvent {
                resource_id: ResourceId::Timer(id),
                kind,
                ..
            }) => *id == timer_id && *kind == event_kind,

            _ => false,
        })
}

/// Waits until `receipts` contain the specified timer-related event.
async fn wait_timer(
    receipts: impl Stream<Item = Receipt>,
    timer_id: TimerId,
    event_kind: ResourceEventKind,
) {
    receipts
        .take_while(|receipt| future::ready(!check_timer(receipt, timer_id, event_kind)))
        .for_each(|_| async {})
        .await
}

struct AsyncRig {
    scheduler: MockScheduler,
    events: MessageReceiver<DomainEvent, Json>,
    tracer: TracerHandle<Json>,
    results: mpsc::UnboundedReceiver<ExecutionResult>,
}

async fn initialize_workflow() -> TestResult<AsyncRig> {
    let module = task::spawn_blocking(|| &*MODULE).await;
    let scheduler = MockScheduler::default();
    let spawner = module
        .for_workflow::<PizzaDelivery>()?
        .with_clock(scheduler.clone());

    let inputs = Inputs {
        oven_count: 2,
        deliverer_count: 1,
    };
    let workflow = spawner.spawn(inputs)?.init()?.into_inner();
    let mut env = AsyncEnv::new(workflow, scheduler.clone());
    let mut handle = env.handle();
    let results = env.execution_results();
    task::spawn(async move { env.run().await });

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
    handle
        .orders
        .send_all(&mut stream::iter(orders).map(Ok))
        .await?;
    drop(handle.orders);

    let events: Vec<_> = handle.shared.events.by_ref().take(2).try_collect().await?;
    assert_matches!(
        events.as_slice(),
        [
            DomainEvent::OrderTaken { .. },
            DomainEvent::OrderTaken { .. },
        ]
    );

    Ok(AsyncRig {
        scheduler,
        events: handle.shared.events,
        tracer: handle.shared.tracer,
        results,
    })
}

#[async_std::test]
async fn async_handle_with_mock_scheduler() -> TestResult {
    let AsyncRig {
        scheduler,
        mut events,
        mut tracer,
        results,
    } = initialize_workflow().await?;
    let mut receipts = results.map(|result| match result {
        ExecutionResult::Ok(receipt) => receipt,
        _ => unreachable!(),
    });

    wait_timer(&mut receipts, 1, ResourceEventKind::Created).await;
    let now = scheduler.now();
    let next_timer = retry_until_some(|| {
        scheduler
            .next_timer_expiration()
            .filter(|&timer| (timer - now).num_milliseconds() == 40)
    })
    .await;

    scheduler.set_now(next_timer);
    {
        let events: Vec<_> = events.by_ref().take(2).try_collect().await?;
        assert_matches!(
            events.as_slice(),
            [
                DomainEvent::Baked { index: 2, .. },
                DomainEvent::StartedDelivering { index: 2, .. },
            ]
        );
    }
    assert!(events.next().now_or_never().is_none());

    tracer
        .by_ref()
        .try_take_while(|(_, fut)| {
            let baking_finished =
                fut.name() == "baking_process (order=2)" && fut.state() == FutureState::Dropped;
            future::ready(Ok(!baking_finished))
        })
        .try_for_each(|_| future::ready(Ok(())))
        .await?;
    assert!(!tracer.futures().is_empty());
    let (_, delivery_future) = tracer
        .futures()
        .iter()
        .find(|(_, fut)| fut.name() == "baking_process (order=1)")
        .unwrap();
    assert_eq!(delivery_future.state(), FutureState::Polling);

    let now = scheduler.now();
    let next_timer = retry_until_some(|| scheduler.next_timer_expiration()).await;
    assert_eq!((next_timer - now).num_milliseconds(), 10);
    scheduler.set_now(next_timer);

    wait_timer(&mut receipts, 2, ResourceEventKind::Created).await;
    let next_timer = retry_until_some(|| scheduler.next_timer_expiration()).await;
    scheduler.set_now(next_timer);
    wait_timer(&mut receipts, 0, ResourceEventKind::Dropped).await;

    let events: Vec<_> = events.by_ref().take(2).try_collect().await?;
    assert!(events
        .iter()
        .any(|event| matches!(event, DomainEvent::Baked { index: 1, .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, DomainEvent::Delivered { index: 2, .. })));

    Ok(())
}

#[async_std::test]
async fn async_handle_with_mock_scheduler_and_bulk_update() -> TestResult {
    let AsyncRig {
        scheduler,
        mut events,
        results,
        ..
    } = initialize_workflow().await?;
    let mut receipts = results.map(|result| match result {
        ExecutionResult::Ok(receipt) => receipt,
        _ => unreachable!(),
    });

    let now = scheduler.now();
    scheduler.set_now(now + chrono::Duration::milliseconds(55));
    // ^ Expires both active timers

    {
        let events: Vec<_> = events
            .by_ref()
            .try_take_while(|event| {
                future::ready(Ok(!matches!(event, DomainEvent::StartedDelivering { .. })))
            })
            .try_collect()
            .await?;
        assert!(events.len() == 1 || events.len() == 2);
        assert!(events
            .iter()
            .all(|event| matches!(event, DomainEvent::Baked { .. })));
    }
    assert!(events.next().now_or_never().is_none());

    wait_timer(&mut receipts, 0, ResourceEventKind::Dropped).await;
    Ok(())
}

#[derive(Debug)]
struct CancellableWorkflow {
    spawner: WorkflowSpawner<PizzaDelivery>,
    handle: PizzaDeliveryHandle<AsyncEnv<PizzaDelivery>>,
    join_handle: task::JoinHandle<Workflow<PizzaDelivery>>,
    scheduler: MockScheduler,
    cancel_sx: oneshot::Sender<()>,
}

async fn spawn_cancellable_workflow() -> TestResult<CancellableWorkflow> {
    let module = task::spawn_blocking(|| &*MODULE).await;
    let scheduler = MockScheduler::default();
    let spawner = module
        .for_workflow::<PizzaDelivery>()?
        .with_clock(scheduler.clone());

    let inputs = Inputs {
        oven_count: 1,
        deliverer_count: 1,
    };
    let workflow = spawner.spawn(inputs)?.init()?.into_inner();
    let mut env = AsyncEnv::new(workflow, scheduler.clone());
    let handle = env.handle();

    let (cancel_sx, mut cancel_rx) = oneshot::channel::<()>();
    let join_handle = task::spawn(async move {
        futures::select! {
            _ = env.run().fuse() => unreachable!("workflow should not be completed"),
            _ = cancel_rx => env.into_inner(),
        }
    });

    Ok(CancellableWorkflow {
        spawner,
        handle,
        join_handle,
        scheduler,
        cancel_sx,
    })
}

#[async_std::test]
async fn persisting_workflow() -> TestResult {
    let CancellableWorkflow {
        spawner,
        mut handle,
        join_handle,
        scheduler,
        cancel_sx,
    } = spawn_cancellable_workflow().await?;

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle.orders.send(order).await?;

    let next_timer = retry_until_some(|| scheduler.next_timer_expiration()).await;
    scheduler.set_now(next_timer);
    // Wait until the second timer is active.
    while let Some(event) = handle.shared.events.try_next().await? {
        if matches!(event, DomainEvent::StartedDelivering { .. }) {
            break;
        }
    }

    // Cancel the workflow.
    cancel_sx.send(()).unwrap();
    let mut workflow = join_handle.await;
    let mut tracer = handle.shared.tracer;
    tracer
        .by_ref()
        .try_for_each(|_| future::ready(Ok(())))
        .await?;
    let traced_futures = tracer.into_futures();
    let persisted = workflow.persist()?;

    // Restore the persisted workflow and launch it again.
    let workflow = persisted.restore(&spawner)?;
    let mut env = AsyncEnv::new(workflow, scheduler.clone());
    env.extensions().insert(traced_futures);
    let handle = env.handle();
    let join_handle = task::spawn(async move { env.run().await });

    drop(handle.orders); // should terminate the workflow once the delivery timer is expired
    let next_timer = retry_until_some(|| scheduler.next_timer_expiration()).await;
    scheduler.set_now(next_timer);
    assert_matches!(join_handle.await?, Termination::Finished);

    let mut tracer = handle.shared.tracer;
    tracer
        .by_ref()
        .try_for_each(|_| future::ready(Ok(())))
        .await?;
    assert_eq!(tracer.futures().len(), 3);
    assert!(tracer
        .futures()
        .iter()
        .all(|(_, fut)| fut.state() == FutureState::Dropped));

    Ok(())
}

#[async_std::test]
async fn dynamically_typed_async_handle() -> TestResult {
    let module = task::spawn_blocking(|| &*MODULE).await;
    let spawner = module.for_untyped_workflow("PizzaDelivery").unwrap();
    let data = Json.encode_value(Inputs {
        oven_count: 1,
        deliverer_count: 1,
    });
    let workflow = spawner.spawn(data)?.init()?.into_inner();

    let mut env = AsyncEnv::new(workflow, AsyncIoScheduler);
    let mut handle = env.handle();
    let join_handle = task::spawn(async move { env.run().await.map(|_| env.into_inner()) });

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    handle[InboundChannel("orders")]
        .send(Json.encode_value(order))
        .await?;
    handle.remove(InboundChannel("orders")); // to terminate the workflow

    let events = handle[OutboundChannel("events")].by_ref();
    let events: Vec<DomainEvent> = events
        .map(|res| Json.try_decode_bytes(res.unwrap()))
        .try_collect()
        .await?;
    assert_matches!(
        events.as_slice(),
        [
            DomainEvent::OrderTaken { .. },
            DomainEvent::Baked { .. },
            DomainEvent::StartedDelivering { .. },
            DomainEvent::Delivered { .. },
        ]
    );

    let workflow = join_handle.await?;
    let chan = workflow.inbound_channel("orders").unwrap();
    assert!(chan.is_closed());
    assert_eq!(chan.received_messages(), 1);
    let chan = workflow.outbound_channel("events").unwrap();
    assert_eq!(chan.flushed_messages(), 4);
    let chan = workflow.outbound_channel("traces").unwrap();
    assert_eq!(chan.flushed_messages(), 22);
    Ok(())
}

#[async_std::test]
async fn rollback_strategy() -> TestResult {
    let module = task::spawn_blocking(|| &*MODULE).await;
    let spawner = module.for_untyped_workflow("PizzaDelivery").unwrap();
    let data = Json.encode_value(Inputs {
        oven_count: 1,
        deliverer_count: 1,
    });
    let workflow = spawner.spawn(data)?.init()?.into_inner();

    let mut env = AsyncEnv::new(workflow, AsyncIoScheduler);
    env.set_rollback_strategy(Rollback::any_trap());
    let mut handle = env.handle();
    let results = env.execution_results();
    let join_handle = task::spawn(async move { env.run().await });

    handle[InboundChannel("orders")]
        .send(b"invalid".to_vec())
        .await?;
    handle.remove(InboundChannel("orders")); // to terminate the workflow

    let results: Vec<_> = results.collect().await;
    let err = match results.as_slice() {
        [ExecutionResult::RolledBack(err), ExecutionResult::Ok(_)] => err,
        _ => panic!("unexpected results: {:?}", results),
    };
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

    join_handle.await?;
    Ok(())
}
