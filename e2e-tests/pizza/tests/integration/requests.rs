//! Tests a variation of workflow with externally executed tasks.

use assert_matches::assert_matches;
use async_std::task;
use async_trait::async_trait;
use futures::{
    channel::mpsc,
    future::{self, BoxFuture},
    FutureExt, StreamExt, TryStreamExt,
};
use test_casing::{test_casing, Product};

use std::{cmp, convert::identity, time::Duration};

use tardigrade::Json;
use tardigrade_rt::{
    runtime::{DriveConfig, Termination},
    storage::InProcessConnection,
    AsyncIoScheduler,
};
use tardigrade_worker::{
    HandleRequest, Request, Worker, WorkerInterface, WorkerStorageConnection, WorkerStoragePool,
};

use crate::{
    create_streaming_manager, spawn_workflow,
    tasks::{assert_event_completeness, assert_event_concurrency, send_orders},
    TestResult,
};
use tardigrade_pizza::{
    requests::{Args, PizzaDeliveryWithRequests},
    DomainEvent, PizzaKind, PizzaOrder,
};

const DEFINITION_ID: &str = "test::PizzaDeliveryWithRequests";

/// Baking worker that is able to perform several requests concurrently.
struct ConcurrentBaking<P> {
    events_sx: mpsc::UnboundedSender<DomainEvent>,
    tasks_sx: mpsc::UnboundedSender<BoxFuture<'static, ()>>,
    connection_pool: Option<P>,
}

impl<P> ConcurrentBaking<P> {
    fn new(events_sx: mpsc::UnboundedSender<DomainEvent>, task_concurrency: Option<usize>) -> Self {
        let (tasks_sx, tasks_rx) = mpsc::unbounded();
        let executor_task = tasks_rx.for_each_concurrent(task_concurrency, identity);
        task::spawn(executor_task);

        Self {
            events_sx,
            tasks_sx,
            connection_pool: None,
        }
    }
}

impl<P: WorkerStoragePool> WorkerInterface for ConcurrentBaking<P> {
    type Request = PizzaOrder;
    type Response = ();
    type Codec = Json;
}

#[async_trait]
impl<P: WorkerStoragePool + Clone + 'static> HandleRequest<P> for ConcurrentBaking<P> {
    async fn initialize(&mut self, connection_pool: &P) {
        self.connection_pool = Some(connection_pool.clone());
    }

    async fn handle_request(
        &mut self,
        request: Request<Self>,
        _connection: &mut P::Connection<'_>,
    ) {
        let index = request.request_id() as usize;
        let (order, response_sx) = request.into_parts();
        let connection_pool = self.connection_pool.clone().unwrap();
        let events_sx = self.events_sx.clone();

        let task = async move {
            events_sx
                .unbounded_send(DomainEvent::OrderTaken { index, order })
                .unwrap();
            task::sleep(order.kind.baking_time()).await;
            let mut connection = connection_pool.connect().await;
            response_sx.send((), &mut connection).await.ok();
            connection.release().await;
            events_sx
                .unbounded_send(DomainEvent::Baked { index, order })
                .unwrap();
        };
        self.tasks_sx.unbounded_send(task.boxed()).unwrap();
    }
}

async fn test_external_tasks(
    oven_count: usize,
    order_count: usize,
    task_concurrency: Option<usize>,
) -> TestResult {
    let (manager, mut commits_rx) = create_streaming_manager(AsyncIoScheduler).await?;
    let storage = manager.storage().as_ref().clone();
    let (executor_events_sx, executor_events_rx) = mpsc::unbounded();
    let baking = ConcurrentBaking::new(executor_events_sx, task_concurrency);
    let worker = Worker::new(baking, InProcessConnection(storage));
    let worker_task = task::spawn(worker.listen("tardigrade.test.Baking"));
    task::sleep(Duration::from_millis(25)).await; // wait for the worker to initialize

    let args = Args { oven_count };
    let (_, handle) =
        spawn_workflow::<_, PizzaDeliveryWithRequests>(&manager, DEFINITION_ID, args).await?;
    let orders_sx = handle.orders;
    let events_rx = handle.shared.events.stream_messages(0..);
    let events_rx = events_rx.map(|message| message.decode());

    let manager = manager.clone();
    let join_handle =
        task::spawn(async move { manager.drive(&mut commits_rx, DriveConfig::new()).await });

    send_orders(orders_sx, order_count).await?;
    let events: Vec<_> = events_rx.try_collect().await?;
    assert_eq!(events.len(), 2 * order_count, "{events:?}");
    assert_event_completeness(&events, order_count);

    // Check that concurrency is properly controlled by the workflow.
    worker_task.cancel().await;
    let executor_events: Vec<_> = executor_events_rx.collect().await;
    let expected_concurrency = cmp::min(oven_count, task_concurrency.unwrap_or(usize::MAX));
    assert_event_concurrency(&executor_events, expected_concurrency);

    assert_matches!(join_handle.await, Termination::Finished);
    Ok(())
}

#[async_std::test]
async fn external_task_basics() -> TestResult {
    test_external_tasks(1, 1, None).await
}

const CONCURRENCY_CASES: [Option<usize>; 4] = [None, Some(1), Some(2), Some(3)];

#[test_casing(4, CONCURRENCY_CASES)]
#[async_std::test]
async fn sequential_external_tasks(task_concurrency: Option<usize>) -> TestResult {
    test_external_tasks(1, 4, task_concurrency).await
}

#[test_casing(12, Product((2..=4, CONCURRENCY_CASES)))]
#[async_std::test]
async fn concurrent_external_tasks(
    oven_count: usize,
    task_concurrency: Option<usize>,
) -> TestResult {
    test_external_tasks(oven_count, 10, task_concurrency).await
}

struct RestrictedBaking<P> {
    tasks_sx: mpsc::UnboundedSender<BoxFuture<'static, ()>>,
    connection_pool: Option<P>,
}

impl<P> RestrictedBaking<P> {
    fn new(successful_task_count: usize) -> Self {
        let (tasks_sx, tasks_rx) = mpsc::unbounded();
        let executor_task = tasks_rx
            .buffer_unordered(successful_task_count)
            .take(successful_task_count)
            .for_each(future::ready);
        task::spawn(executor_task);

        Self {
            tasks_sx,
            connection_pool: None,
        }
    }
}

impl<P: WorkerStoragePool> WorkerInterface for RestrictedBaking<P> {
    type Request = PizzaOrder;
    type Response = ();
    type Codec = Json;
}

#[async_trait]
impl<P: WorkerStoragePool + Clone + 'static> HandleRequest<P> for RestrictedBaking<P> {
    async fn initialize(&mut self, connection_pool: &P) {
        self.connection_pool = Some(connection_pool.clone());
    }

    async fn handle_request(&mut self, request: Request<Self>, connection: &mut P::Connection<'_>) {
        let response_channel_id = request.response_channel_id();
        let (order, response_sx) = request.into_parts();
        let connection_pool = self.connection_pool.clone().unwrap();

        // Do not create a task if we know it cannot be sent for execution.
        if self.tasks_sx.is_closed() {
            connection
                .close_response_channel(response_channel_id)
                .await
                .ok();
            return;
        }

        let task = async move {
            task::sleep(order.kind.baking_time()).await;
            let mut connection = connection_pool.connect().await;
            response_sx.send((), &mut connection).await.ok();
            connection.release().await;
        };
        if self.tasks_sx.unbounded_send(task.boxed()).is_err() {
            connection
                .close_response_channel(response_channel_id)
                .await
                .ok();
        }
    }
}

#[test_casing(4, [(5, 2), (5, 4), (10, 3), (10, 7)])]
#[async_std::test]
async fn closing_task_responses_on_host(
    order_count: usize,
    successful_task_count: usize,
) -> TestResult {
    let (manager, mut commits_rx) = create_streaming_manager(AsyncIoScheduler).await?;
    let storage = manager.storage().as_ref().clone();
    let baking = RestrictedBaking::new(successful_task_count);
    let worker = Worker::new(baking, InProcessConnection(storage));
    task::spawn(worker.listen("tardigrade.test.Baking"));
    task::sleep(Duration::from_millis(25)).await; // wait for the worker to initialize

    let args = Args { oven_count: 2 };
    let (_, handle) =
        spawn_workflow::<_, PizzaDeliveryWithRequests>(&manager, DEFINITION_ID, args).await?;

    let orders_sx = handle.orders;
    let events_rx = handle.shared.events.stream_messages(0..);
    let events_rx = events_rx.map(|message| message.decode());

    let manager = manager.clone();
    let join_handle =
        task::spawn(async move { manager.drive(&mut commits_rx, DriveConfig::default()).await });

    let orders = (0..order_count).map(|i| PizzaOrder {
        kind: match i % 3 {
            0 => PizzaKind::Margherita,
            1 => PizzaKind::Pepperoni,
            2 => PizzaKind::FourCheese,
            _ => unreachable!(),
        },
        delivery_distance: 10,
    });
    orders_sx.send_all(orders).await?;
    orders_sx.close().await;

    let events: Vec<_> = events_rx.try_collect().await?;
    assert_eq!(
        events.len(),
        order_count + successful_task_count,
        "{events:?}"
    );
    let baked_count = events
        .iter()
        .filter(|event| matches!(event, DomainEvent::Baked { .. }))
        .count();
    assert_eq!(baked_count, successful_task_count, "{events:?}");

    assert_matches!(join_handle.await, Termination::Finished);
    Ok(())
}
