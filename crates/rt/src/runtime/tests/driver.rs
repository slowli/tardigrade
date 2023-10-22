//! Tests for `Driver`.

use assert_matches::assert_matches;
use async_std::task;
use chrono::TimeZone;
use futures::{
    future::{self, Either},
    FutureExt, StreamExt,
};

use std::{sync::Arc, task::Poll, time::Duration};

use super::*;
use crate::{
    receipt::{ResourceEventKind, ResourceId},
    storage::Streaming,
    MockScheduler,
};
use tardigrade::{TimerDefinition, TimerId};

type StreamingStorage = Streaming<Arc<LocalStorage>>;
type StreamingRuntime = Runtime<MockEngine, MockScheduler, StreamingStorage>;

fn create_storage() -> (StreamingStorage, CommitStream) {
    let storage = Arc::new(LocalStorage::default());
    let (mut storage, router_task) = Streaming::new(storage);
    task::spawn(router_task);
    let commits_rx = storage.stream_commits();
    (storage, commits_rx)
}

async fn create_test_runtime(storage: StreamingStorage, poll_fns: MockAnswers) -> StreamingRuntime {
    let clock = MockScheduler::default();
    create_test_runtime_with_storage(poll_fns, clock, storage).await
}

fn poll_orders(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let channels = ctx.data().persisted.channels();
    let orders_id = channels.channel_id("orders").unwrap();
    let mut orders = ctx.data_mut().receiver(orders_id);
    let poll_result = orders.poll_next().into_inner(ctx)?;
    assert!(poll_result.is_pending());

    Ok(Poll::Pending)
}

fn handle_order(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let channels = ctx.data().persisted.channels();
    let orders_id = channels.channel_id("orders").unwrap();
    let events_id = channels.channel_id("events").unwrap();

    let mut orders = ctx.data_mut().receiver(orders_id);
    let poll_result = orders.poll_next().into_inner(ctx)?;
    assert_matches!(poll_result, Poll::Ready(Some(_)));
    let mut events = ctx.data_mut().sender(events_id);
    events.start_send(b"event #1".to_vec())?;

    Ok(Poll::Ready(()))
}

#[async_std::test]
async fn completing_workflow_via_driver() {
    let poll_fns = MockAnswers::from_values([poll_orders, handle_order]);
    let (storage, mut commits_rx) = create_storage();
    let runtime = create_test_runtime(storage, poll_fns).await;
    let workflow = create_test_workflow(&runtime).await;

    let driver_task = {
        let runtime = runtime.clone();
        task::spawn(async move { runtime.drive(&mut commits_rx, DriveConfig::new()).await })
    };

    let mut handle = workflow.handle().await.with_indexing();
    let orders_sx = handle.remove(ReceiverAt("orders")).unwrap();
    let events_rx = handle.remove(SenderAt("events")).unwrap();
    let events_rx = events_rx
        .stream_messages(0..)
        .map(|message| message.decode().unwrap());
    drop(handle);

    orders_sx.send(b"order".to_vec()).await.unwrap();
    orders_sx.close().await;

    assert_matches!(driver_task.await, Termination::Finished);
    let events: Vec<_> = events_rx.collect().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], b"event #1");
}

async fn test_driver_with_multiple_messages(start_after_tick: bool) {
    let poll_orders_and_send_event: MockPollFn = |ctx| {
        let channels = ctx.data().persisted.channels();
        let orders_id = channels.channel_id("orders").unwrap();
        let events_id = channels.channel_id("events").unwrap();

        let mut orders = ctx.data_mut().receiver(orders_id);
        let poll_result = orders.poll_next().into_inner(ctx)?;
        assert!(poll_result.is_pending());
        ctx.data_mut()
            .sender(events_id)
            .start_send(b"event #0".to_vec())?;

        Ok(Poll::Pending)
    };
    let poll_fns = MockAnswers::from_values([poll_orders_and_send_event, handle_order]);

    let (storage, mut commits_rx) = create_storage();
    let runtime = create_test_runtime(storage, poll_fns).await;
    let workflow = create_test_workflow(&runtime).await;

    let mut handle = workflow.handle().await.with_indexing();
    let events_rx = handle.remove(SenderAt("events")).unwrap();
    let mut events_rx = events_rx
        .stream_messages(0..)
        .map(|message| message.decode().unwrap());
    let orders_sx = handle.remove(ReceiverAt("orders")).unwrap();
    drop(handle);

    if start_after_tick {
        runtime.tick().await.unwrap().into_inner().unwrap();
        let event = events_rx.next().await.unwrap();
        assert_eq!(event, b"event #0");
    }

    let orders_task = async move {
        orders_sx.send(b"order".to_vec()).await.unwrap();
        orders_sx.close().await;
    };
    let drive_task = runtime.clone().drive(&mut commits_rx, DriveConfig::new());
    let ((), termination) = future::join(orders_task, drive_task).await;
    assert_matches!(termination, Termination::Finished);

    let events: Vec<_> = events_rx.collect().await;
    assert_eq!(events.last().unwrap(), b"event #1");
    if start_after_tick {
        assert_eq!(events.len(), 1);
    } else {
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], b"event #0");
    }
}

#[async_std::test]
async fn driver_with_multiple_messages() {
    test_driver_with_multiple_messages(false).await;
}

#[async_std::test]
async fn starting_driver_after_manual_tick() {
    test_driver_with_multiple_messages(true).await;
}

#[async_std::test]
async fn selecting_from_driver_and_other_future() {
    let handle_order_and_poll_order: MockPollFn = |ctx| {
        let _ = handle_order(ctx)?;
        poll_orders(ctx)
    };
    let poll_fns =
        MockAnswers::from_values([poll_orders, handle_order_and_poll_order, handle_order]);

    let (storage, mut commits_rx) = create_storage();
    let runtime = create_test_runtime(storage, poll_fns).await;
    let mut workflow = create_test_workflow(&runtime).await;
    let mut drive_config = DriveConfig::new();
    let mut tick_results = drive_config.tick_results();

    let mut handle = workflow.handle().await.with_indexing();
    let orders_sx = handle.remove(ReceiverAt("orders")).unwrap();
    let orders_id = orders_sx.channel_id();
    let events_rx = handle.remove(SenderAt("events")).unwrap();
    let mut events_rx = events_rx
        .stream_messages(0..)
        .map(|message| message.decode().unwrap());
    drop(handle);

    let api_actions = orders_sx.send(b"order".to_vec()).map(Result::unwrap);
    let until_consumed_order = async move {
        let mut consumed_order = false;
        while let Some(tick_result) = tick_results.next().await {
            let receipt = tick_result.as_ref().unwrap();
            if receipt
                .events()
                .any(|event| is_consumption(event, orders_id))
            {
                consumed_order = true;
                break;
            }
        }
        assert!(consumed_order);
    };
    futures::pin_mut!(until_consumed_order);

    {
        let driver_task = runtime.clone().drive(&mut commits_rx, drive_config);
        futures::pin_mut!(driver_task);
        let select_task = future::select(driver_task, until_consumed_order);

        let ((), select_result) = future::join(api_actions, select_task).await;
        match select_result {
            Either::Right(((), _)) => { /* just as expected */ }
            Either::Left((driver_result, _)) => panic!("{driver_result:?}"),
        }
    }
    let event = events_rx.next().await.unwrap();
    assert_eq!(event, b"event #1");

    // The `runtime` can be used again.
    let mut handle = workflow.handle().await.with_indexing();
    let orders = handle.remove(ReceiverAt("orders")).unwrap();
    orders.send(b"order #2".to_vec()).await.unwrap();
    runtime.tick().await.unwrap().into_inner().unwrap();
    workflow.update().await.unwrap_err(); // should be completed
}

async fn test_dropping_storage_with_driver(drop_before_start: bool) {
    const TIMER_ID: TimerId = 0;

    let create_timer: MockPollFn = |ctx| {
        let timer_id = ctx.data_mut().create_timer(TimerDefinition {
            expires_at: Utc.timestamp_millis_opt(50).unwrap(),
        });
        assert_eq!(timer_id, TIMER_ID);

        let poll_result = ctx.data_mut().timer(timer_id).poll().into_inner(ctx)?;
        assert!(poll_result.is_pending());
        Ok(Poll::Pending)
    };
    let complete_timer: MockPollFn = |ctx| {
        let poll_result = ctx.data_mut().timer(TIMER_ID).poll().into_inner(ctx)?;
        assert!(poll_result.is_ready());
        Ok(Poll::Ready(()))
    };
    let poll_fns = MockAnswers::from_values([create_timer, complete_timer]);

    let (storage, mut commits_rx) = create_storage();
    let clock = MockScheduler::default();
    let runtime = create_test_runtime_with_storage(poll_fns, clock.clone(), storage).await;
    create_test_workflow(&runtime).await;
    let mut runtime = Some(runtime);

    let mut config = DriveConfig::new();
    let mut tick_results = config.tick_results();
    let driver_task = {
        let runtime = runtime.as_ref().unwrap().clone();
        async move { runtime.drive(&mut commits_rx, config).await }
    };

    if drop_before_start {
        runtime.take();
    }
    let driver_task = task::spawn(driver_task);

    let receipt = tick_results.next().await.unwrap().into_inner().unwrap();
    assert!(receipt.events().any(|event| {
        event.as_resource_event().map_or(false, |event| {
            matches!(event.resource_id, ResourceId::Timer(TIMER_ID))
        })
    }));

    runtime.take();
    clock.set_now(clock.now() + chrono::Duration::milliseconds(100));

    let receipt = tick_results.next().await.unwrap().into_inner().unwrap();
    let timer_event = receipt.events().find_map(|event| {
        if let Some(event) = event.as_resource_event() {
            if matches!(event.resource_id, ResourceId::Timer(TIMER_ID)) {
                return Some(event.kind);
            }
        }
        None
    });
    let timer_event = timer_event.unwrap();
    assert_matches!(timer_event, ResourceEventKind::Polled(Poll::Ready(())));

    assert_matches!(driver_task.await, Termination::Finished);
}

#[async_std::test]
async fn dropping_storage_before_driving_runtime() {
    test_dropping_storage_with_driver(true).await;
}

#[async_std::test]
async fn dropping_storage_when_driving_runtime() {
    test_dropping_storage_with_driver(false).await;
}

#[async_std::test]
async fn not_waiting_for_new_workflow_with_default_config() {
    let poll_fns = MockAnswers::from_values([poll_orders, handle_order]);
    let (storage, mut commits_rx) = create_storage();
    let runtime = create_test_runtime(storage, poll_fns).await;

    // The runtime should immediately terminate.
    let termination = runtime.drive(&mut commits_rx, DriveConfig::new()).await;
    assert_matches!(termination, Termination::Finished);
}

#[async_std::test]
async fn waiting_for_new_workflow() {
    let poll_fns = MockAnswers::from_values([poll_orders, handle_order]);
    let (storage, mut commits_rx) = create_storage();
    let runtime = create_test_runtime(storage, poll_fns).await;

    let mut config = DriveConfig::new();
    config.wait_for_workflows();
    let mut results_rx = config.tick_results();
    let driver_task = {
        let runtime = runtime.clone();
        task::spawn(async move { runtime.drive(&mut commits_rx, config).await })
    };

    task::sleep(Duration::from_millis(50)).await;
    assert!(driver_task.now_or_never().is_none());

    let mut workflow = create_test_workflow(&runtime).await;
    let result = results_rx.next().await.unwrap();
    assert_eq!(result.workflow_id(), workflow.id());

    let receipt = result.into_inner().unwrap();
    let has_polling_event = receipt.events().any(|event| {
        if let Some(event) = event.as_channel_event() {
            return matches!(
                event.kind,
                ChannelEventKind::ReceiverPolled {
                    result: Poll::Pending,
                }
            );
        }
        false
    });
    assert!(has_polling_event, "{receipt:#?}");

    workflow.update().await.unwrap();
    assert_eq!(workflow.record().execution_count, 1);

    let mut handle = workflow.handle().await.with_indexing();
    let orders_sx = handle.remove(ReceiverAt("orders")).unwrap();
    orders_sx.send(b"order".to_vec()).await.unwrap();
    orders_sx.close().await;

    let result = results_rx.next().await.unwrap();
    assert_eq!(result.workflow_id(), workflow.id());
    let receipt = result.into_inner().unwrap();
    assert_main_task_completed(&receipt);

    workflow.update().await.unwrap_err();
    assert_eq!(workflow.record().execution_count, 2);
    let workflow = runtime.storage().any_workflow(workflow.id()).await.unwrap();
    assert!(workflow.is_completed());
}

fn assert_main_task_completed(receipt: &Receipt) {
    let main_execution = receipt
        .executions()
        .iter()
        .find(|execution| {
            matches!(
                execution.function,
                ExecutedFunction::Task { task_id: 0, .. }
            )
        })
        .unwrap();
    assert_matches!(main_execution.task_result, Some(Ok(())));
}

#[async_std::test]
async fn waiting_for_repaired_workflow() {
    let poll_fns =
        MockAnswers::from_values([poll_orders, error_after_consuming_message, handle_order]);
    let (storage, mut commits_rx) = create_storage();
    let runtime = create_test_runtime(storage, poll_fns).await;
    let mut workflow = create_test_workflow(&runtime).await;

    let mut config = DriveConfig::new();
    config.wait_for_workflows();
    let mut results_rx = config.tick_results();
    {
        let runtime = runtime.clone();
        task::spawn(async move { runtime.drive(&mut commits_rx, config).await });
    }

    let result = results_rx.next().await.unwrap();
    assert_eq!(result.workflow_id(), workflow.id());
    workflow.update().await.unwrap();
    assert_eq!(workflow.record().execution_count, 1);

    let mut handle = workflow.handle().await.with_indexing();
    let orders_sx = handle.remove(ReceiverAt("orders")).unwrap();
    orders_sx.send(b"bogus".to_vec()).await.unwrap();

    let result = results_rx.next().await.unwrap();
    assert_eq!(result.workflow_id(), workflow.id());
    let err = result.into_inner().unwrap_err();
    let err = err.trap().to_string();
    assert!(err.contains("oops"), "{err}");

    workflow.update().await.unwrap_err();
    assert_eq!(workflow.record().execution_count, 2);
    let workflow_id = workflow.id();
    let workflow = runtime.storage().any_workflow(workflow_id).await.unwrap();
    assert!(workflow.is_errored());

    let workflow = workflow.unwrap_errored();
    let mut err_messages: Vec<_> = workflow.messages().collect();
    assert_eq!(err_messages.len(), 1);
    let err_message = err_messages.pop().unwrap();
    err_message.drop_for_workflow().await.unwrap();
    workflow.consider_repaired().await.unwrap();

    orders_sx.send(b"order".to_vec()).await.unwrap();
    orders_sx.close().await;

    let result = results_rx.next().await.unwrap();
    let receipt = result.into_inner().unwrap();
    assert_main_task_completed(&receipt);

    let workflow = runtime.storage().any_workflow(workflow_id).await.unwrap();
    assert!(workflow.is_completed());
}
