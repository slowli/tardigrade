//! Tests for the `PizzaDelivery` workflow that use `AsyncEnv`.

use assert_matches::assert_matches;
use async_std::task;
use chrono::{DateTime, Utc};
use futures::{
    channel::{mpsc, oneshot},
    future::{self, Either},
    stream, FutureExt, SinkExt, Stream, StreamExt, TryStreamExt,
};
use tracing::instrument::WithSubscriber;
use tracing_capture::Storage;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
};

use crate::LocalManager;
use tardigrade::{
    interface::{ReceiverName, SenderName},
    spawn::ManageWorkflowsExt,
    Decode, Encode, Json, TimerId,
};
use tardigrade_rt::{
    driver::{Driver, MessageReceiver, MessageSender, Termination},
    manager::TickResult,
    receipt::{Event, Receipt, ResourceEvent, ResourceEventKind, ResourceId},
    test::MockScheduler,
    AsyncIoScheduler,
};
use tardigrade_test_basic::{Args, DomainEvent, PizzaDelivery, PizzaKind, PizzaOrder};

use super::{create_manager, enable_tracing_assertions, TestResult};

fn spawn_traced_task<T: Send + 'static>(
    future: impl Future<Output = T> + Send + 'static,
) -> task::JoinHandle<T> {
    task::spawn(future.with_current_subscriber())
}

fn drain_stream(stream: &mut (impl Stream + Unpin)) {
    while stream.next().now_or_never().is_some() {
        // Just drop the returned item
    }
}

async fn test_async_handle(cancel_workflow: bool) -> TestResult {
    let (_guard, tracing_storage) = enable_tracing_assertions();
    let mut manager = create_manager(AsyncIoScheduler).await?;

    let inputs = Args {
        oven_count: 1,
        deliverer_count: 1,
    };
    let mut workflow = manager
        .new_workflow::<PizzaDelivery>("test::PizzaDelivery", inputs)?
        .build()
        .await?;
    let workflow_id = workflow.id();
    let handle = workflow.handle();
    let mut driver = Driver::new();
    let mut orders = handle.orders.into_sink(&mut driver);
    let events = handle.shared.events.into_stream(&mut driver);

    let (cancel_sx, mut cancel_rx) = oneshot::channel::<()>();
    let join_handle = spawn_traced_task(async move {
        futures::select! {
            term = driver.drive(&mut manager).fuse() => Either::Left(term),
            _ = cancel_rx => Either::Right(manager),
        }
    });

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    orders.send(order).await?;

    let events: Vec<_> = events.take(4).try_collect().await?;
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
        let manager = match join_handle.await {
            Either::Right(manager) => manager,
            other => panic!("expected cancelled workflow, got {:?}", other),
        };

        let workflow = manager.workflow(workflow_id).await.unwrap();
        let persisted = workflow.persisted();
        assert_eq!(persisted.timers().count(), 0);
    } else {
        drop(orders); // should terminate the workflow
        assert_matches!(join_handle.await, Either::Left(Termination::Finished));
        assert_completed_spans(&tracing_storage.lock());
    }

    Ok(())
}

fn assert_completed_spans(storage: &Storage) {
    let spans: HashSet<_> = storage
        .all_spans()
        .filter_map(|span| {
            if span.stats().is_closed {
                Some(span.metadata().name())
            } else {
                None
            }
        })
        .collect();
    let expected_spans = HashSet::from_iter(["bake", "baking_timer", "delivery_timer"]);
    assert!(spans.is_superset(&expected_spans), "{spans:?}");
}

#[async_std::test]
async fn async_handle() -> TestResult {
    test_async_handle(false).await
}

#[async_std::test]
async fn async_handle_with_cancellation() -> TestResult {
    test_async_handle(true).await
}

async fn test_async_handle_with_concurrency(inputs: Args) -> TestResult {
    const ORDER_COUNT: usize = 5;

    println!("testing async handle with {:?}", inputs);

    let mut manager = create_manager(AsyncIoScheduler).await?;
    let mut workflow = manager
        .new_workflow::<PizzaDelivery>("test::PizzaDelivery", inputs)?
        .build()
        .await?;
    let handle = workflow.handle();
    let mut driver = Driver::new();
    let mut orders_sx = handle.orders.into_sink(&mut driver);
    let events_rx = handle.shared.events.into_stream(&mut driver);
    let join_handle = spawn_traced_task(async move { driver.drive(&mut manager).await });

    let orders = (0..ORDER_COUNT).map(|i| PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: i as u64,
    });
    let mut orders = stream::iter(orders).map(Ok);
    orders_sx.send_all(&mut orders).await?;
    drop(orders_sx); // will terminate the workflow eventually
    join_handle.await;

    let events: Vec<_> = events_rx.try_collect().await?;
    assert_eq!(events.len(), ORDER_COUNT * 4, "{events:#?}");

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
    Ok(())
}

#[async_std::test]
async fn async_handle_with_concurrency() -> TestResult {
    let sample_inputs = [
        Args {
            oven_count: 1,
            deliverer_count: 2,
        },
        Args {
            oven_count: 2,
            deliverer_count: 1,
        },
        Args {
            oven_count: 2,
            deliverer_count: 2,
        },
        Args {
            oven_count: 3,
            deliverer_count: 3,
        },
        Args {
            oven_count: 5,
            deliverer_count: 7,
        },
        Args {
            oven_count: 10,
            deliverer_count: 1,
        },
    ];

    for inputs in sample_inputs {
        test_async_handle_with_concurrency(inputs).await?;
    }
    Ok(())
}

/// Checks whether a `receipt` contains the specified timer event.
fn check_timer(receipt: &Receipt, timer_id: TimerId, event_kind: ResourceEventKind) -> bool {
    receipt.events().any(|event| match event {
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
    scheduler_expirations: Box<dyn Stream<Item = DateTime<Utc>> + Unpin>,
    events: MessageReceiver<DomainEvent, Json>,
    results: mpsc::UnboundedReceiver<TickResult>,
}

async fn initialize_workflow() -> TestResult<AsyncRig> {
    let (scheduler, expirations) = MockScheduler::with_expirations();
    let mut manager = create_manager(scheduler.clone()).await?;

    let inputs = Args {
        oven_count: 2,
        deliverer_count: 1,
    };
    let mut workflow = manager
        .new_workflow::<PizzaDelivery>("test::PizzaDelivery", inputs)?
        .build()
        .await?;
    let handle = workflow.handle();
    let mut driver = Driver::new();
    let mut orders_sx = handle.orders.into_sink(&mut driver);
    let mut events_rx = handle.shared.events.into_stream(&mut driver);
    let results = driver.tick_results();
    spawn_traced_task(async move { driver.drive(&mut manager).await });

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
    orders_sx
        .send_all(&mut stream::iter(orders).map(Ok))
        .await?;
    drop(orders_sx);

    let events: Vec<_> = events_rx.by_ref().take(2).try_collect().await?;
    assert_matches!(
        events.as_slice(),
        [
            DomainEvent::OrderTaken { .. },
            DomainEvent::OrderTaken { .. },
        ]
    );

    Ok(AsyncRig {
        scheduler,
        scheduler_expirations: Box::new(expirations),
        events: events_rx,
        results,
    })
}

#[async_std::test]
async fn async_handle_with_mock_scheduler() -> TestResult {
    let (_guard, tracing_storage) = enable_tracing_assertions();

    let AsyncRig {
        scheduler,
        mut scheduler_expirations,
        mut events,
        results,
    } = initialize_workflow().await?;
    let mut receipts = results.map(|result| result.into_inner().unwrap());

    wait_timer(&mut receipts, 1, ResourceEventKind::Created).await;
    let now = scheduler.now();
    let next_timer = loop {
        let timer = scheduler_expirations.next().await.unwrap();
        if (timer - now).num_milliseconds() == 40 {
            break timer;
        }
    };
    drain_stream(&mut scheduler_expirations);

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

    {
        let storage = tracing_storage.lock();
        let baking_spans: HashMap<_, _> = storage
            .all_spans()
            .filter_map(|span| {
                if span.metadata().name() == "bake" {
                    Some((span["index"].as_uint().unwrap(), span))
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(baking_spans.len(), 2);
        assert!(!baking_spans[&1].stats().is_closed);
        assert!(baking_spans[&2].stats().is_closed);
    }

    let now = scheduler.now();
    let next_timer = scheduler_expirations.next().await.unwrap();
    assert_eq!((next_timer - now).num_milliseconds(), 10);
    scheduler.set_now(next_timer);

    wait_timer(&mut receipts, 2, ResourceEventKind::Created).await;
    let next_timer = scheduler_expirations.next().await.unwrap();
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
    let mut receipts = results.map(|result| result.into_inner().unwrap());

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

struct CancellableWorkflow {
    orders_sx: MessageSender<PizzaOrder, Json>,
    events_rx: MessageReceiver<DomainEvent, Json>,
    join_handle: task::JoinHandle<LocalManager<MockScheduler>>,
    scheduler: MockScheduler,
    scheduler_expirations: Box<dyn Stream<Item = DateTime<Utc>> + Unpin>,
    cancel_sx: oneshot::Sender<()>,
}

async fn spawn_cancellable_workflow() -> TestResult<CancellableWorkflow> {
    let (scheduler, expirations) = MockScheduler::with_expirations();
    let mut manager = create_manager(scheduler.clone()).await?;

    let inputs = Args {
        oven_count: 1,
        deliverer_count: 1,
    };
    let mut workflow = manager
        .new_workflow::<PizzaDelivery>("test::PizzaDelivery", inputs)?
        .build()
        .await?;
    let handle = workflow.handle();
    let mut driver = Driver::new();
    let orders_sx = handle.orders.into_sink(&mut driver);
    let events_rx = handle.shared.events.into_stream(&mut driver);

    let (cancel_sx, mut cancel_rx) = oneshot::channel::<()>();
    let join_handle = spawn_traced_task(async move {
        futures::select! {
            _ = driver.drive(&mut manager).fuse() =>
                unreachable!("workflow should not be completed"),
            _ = cancel_rx => manager,
        }
    });

    Ok(CancellableWorkflow {
        orders_sx,
        events_rx,
        join_handle,
        scheduler,
        scheduler_expirations: Box::new(expirations),
        cancel_sx,
    })
}

#[async_std::test]
async fn launching_env_after_pause() -> TestResult {
    let (_guard, tracing_storage) = enable_tracing_assertions();

    let CancellableWorkflow {
        mut orders_sx,
        mut events_rx,
        join_handle,
        scheduler,
        mut scheduler_expirations,
        cancel_sx,
    } = spawn_cancellable_workflow().await?;

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    orders_sx.send(order).await?;

    let next_timer = scheduler_expirations.next().await.unwrap();
    scheduler.set_now(next_timer);
    // Wait until the second timer is active.
    while let Some(event) = events_rx.try_next().await? {
        if matches!(event, DomainEvent::StartedDelivering { .. }) {
            break;
        }
    }

    // Cancel the workflow.
    cancel_sx.send(()).unwrap();
    let mut manager = join_handle.await;

    // Restore the persisted workflow and launch it again.
    let mut workflow = manager
        .workflow(0)
        .await
        .unwrap()
        .downcast::<PizzaDelivery>()?;
    let mut env = Driver::new();
    let handle = workflow.handle();
    let orders_sx = handle.orders.into_sink(&mut env);
    let join_handle = spawn_traced_task(async move { env.drive(&mut manager).await });

    drop(orders_sx); // should terminate the workflow once the delivery timer is expired
    let next_timer = scheduler_expirations.next().await.unwrap();
    scheduler.set_now(next_timer);
    assert_matches!(join_handle.await, Termination::Finished);

    assert_completed_spans(&tracing_storage.lock());
    Ok(())
}

#[async_std::test]
async fn dynamically_typed_async_handle() -> TestResult {
    let mut manager = create_manager(AsyncIoScheduler).await?;

    let data = Json.encode_value(Args {
        oven_count: 1,
        deliverer_count: 1,
    });
    let mut workflow = manager
        .new_workflow::<()>("test::PizzaDelivery", data)?
        .build()
        .await?;
    let mut handle = workflow.handle();
    let mut env = Driver::new();
    let orders_sx = handle.remove(ReceiverName("orders")).unwrap();
    let orders_id = orders_sx.channel_id();
    let mut orders_sx = orders_sx.into_sink(&mut env);
    let events_rx = handle.remove(SenderName("events")).unwrap();
    let events_id = events_rx.channel_id();
    let events_rx = events_rx.into_stream(&mut env);

    let join_handle = spawn_traced_task(async move {
        env.drive(&mut manager).await;
        manager
    });

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    orders_sx.send(Json.encode_value(order)).await?;
    drop(orders_sx); // to terminate the workflow

    let events: Vec<_> = events_rx
        .map(|res| Decode::<DomainEvent>::try_decode_bytes(&mut Json, res.unwrap()))
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

    let manager = join_handle.await;
    let chan = manager.channel(orders_id).await.unwrap();
    assert!(chan.is_closed);
    assert_eq!(chan.received_messages, 1);
    let chan = manager.channel(events_id).await.unwrap();
    assert_eq!(chan.received_messages, 4);
    Ok(())
}

#[async_std::test]
async fn rollbacks_on_trap() -> TestResult {
    let mut manager = create_manager(AsyncIoScheduler).await?;

    let data = Json.encode_value(Args {
        oven_count: 1,
        deliverer_count: 1,
    });
    let mut workflow = manager
        .new_workflow::<()>("test::PizzaDelivery", data)?
        .build()
        .await?;
    let mut handle = workflow.handle();
    let mut env = Driver::new();
    env.drop_erroneous_messages();
    let orders_sx = handle.remove(ReceiverName("orders")).unwrap();
    let mut orders_sx = orders_sx.into_sink(&mut env);
    let results = env.tick_results();
    let join_handle = spawn_traced_task(async move { env.drive(&mut manager).await });

    orders_sx.send(b"invalid".to_vec()).await?;
    drop(orders_sx); // to terminate the workflow

    let results: Vec<_> = results.map(TickResult::into_inner).collect().await;
    let err = match results.as_slice() {
        [
            Ok(_), // initialization
            Err(err), // receiving message
            Ok(_), // closing receiver
        ] => err,
        _ => panic!("unexpected results: {results:#?}"),
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

    join_handle.await;
    Ok(())
}
