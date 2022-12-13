//! Tests for `Driver`.

#![allow(clippy::similar_names)] // `*_sx` / `*_rx` is conventional naming

use assert_matches::assert_matches;
use async_std::task;
use futures::{future::Either, StreamExt};

use std::{sync::Arc, task::Poll};

use super::*;
use crate::{
    backends::MockScheduler,
    engine::{AsWorkflowData, MockAnswers, MockEngine, MockInstance, MockPollFn},
    handle::StorageRef,
    manager::{
        tests::{create_test_manager_with_storage, create_test_workflow, is_consumption},
        WorkflowManager,
    },
    storage::{LocalStorage, StreamCommits, Streaming},
};
use tardigrade::handle::{ReceiverAt, SenderAt, WithIndexing};

type StreamingStorage = Streaming<Arc<LocalStorage>>;
type StreamingManager = WorkflowManager<MockEngine, MockScheduler, StreamingStorage>;

async fn create_test_manager(poll_fns: MockAnswers) -> (StreamingManager, StreamingStorage) {
    let storage = Arc::new(LocalStorage::default());
    let (storage, router_task) = Streaming::new(storage);
    task::spawn(router_task);

    let clock = MockScheduler::default();
    let manager = create_test_manager_with_storage(poll_fns, clock, storage.clone()).await;
    (manager, storage)
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

    let (mut manager, mut storage) = create_test_manager(poll_fns).await;
    let workflow_id = create_test_workflow(&manager).await.id();

    let (commits_sx, commits_rx) = mpsc::channel(16);
    storage.stream_commits(commits_sx);
    let driver = Driver::new(commits_rx);
    let driver_task = task::spawn(async move { driver.drive(&mut manager).await });

    let storage = StorageRef::from(&storage);
    let workflow = storage.workflow(workflow_id).await.unwrap();
    let mut handle = workflow.handle().await.with_indexing();
    let orders_sx = handle.remove(ReceiverAt("orders")).unwrap();
    let events_rx = handle.remove(SenderAt("events")).unwrap();
    let events_rx = events_rx
        .stream_messages(0)
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

    let (mut manager, mut storage) = create_test_manager(poll_fns).await;
    let (commits_sx, commits_rx) = mpsc::channel(16);
    storage.stream_commits(commits_sx);
    let driver = Driver::new(commits_rx);
    let workflow_id = create_test_workflow(&manager).await.id();

    let storage = StorageRef::from(&storage);
    let workflow = storage.workflow(workflow_id).await.unwrap();
    let mut handle = workflow.handle().await.with_indexing();
    let events_rx = handle.remove(SenderAt("events")).unwrap();
    let mut events_rx = events_rx
        .stream_messages(0)
        .map(|message| message.decode().unwrap());
    let orders_sx = handle.remove(ReceiverAt("orders")).unwrap();
    drop(handle);

    if start_after_tick {
        manager.tick().await.unwrap().into_inner().unwrap();
        let event = events_rx.next().await.unwrap();
        assert_eq!(event, b"event #0");
    }

    let driver_task = task::spawn(async move { driver.drive(&mut manager).await });
    orders_sx.send(b"order".to_vec()).await.unwrap();
    orders_sx.close().await;
    assert_matches!(driver_task.await, Termination::Finished);

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

    let (mut manager, mut storage) = create_test_manager(poll_fns).await;
    let workflow_id = create_test_workflow(&manager).await.id();
    let (commits_sx, commits_rx) = mpsc::channel(16);
    storage.stream_commits(commits_sx);
    let mut driver = Driver::new(commits_rx);
    let mut tick_results = driver.tick_results();

    let storage = StorageRef::from(&storage);
    let workflow = storage.workflow(workflow_id).await.unwrap();
    let mut handle = workflow.handle().await.with_indexing();
    let orders_sx = handle.remove(ReceiverAt("orders")).unwrap();
    let orders_id = orders_sx.channel_id();
    let events_rx = handle.remove(SenderAt("events")).unwrap();
    let mut events_rx = events_rx
        .stream_messages(0)
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
        let driver_task = driver.drive(&mut manager);
        futures::pin_mut!(driver_task);
        let select_task = future::select(driver_task, until_consumed_order);

        let (_, select_result) = future::join(api_actions, select_task).await;
        match select_result {
            Either::Right(((), _)) => { /* just as expected */ }
            Either::Left((driver_result, _)) => panic!("{driver_result:?}"),
        }
    }
    let event = events_rx.next().await.unwrap();
    assert_eq!(event, b"event #1");

    // The `manager` can be used again.
    let mut workflow = manager.storage().workflow(workflow_id).await.unwrap();
    let mut handle = workflow.handle().await.with_indexing();
    let orders = handle.remove(ReceiverAt("orders")).unwrap();
    orders.send(b"order #2".to_vec()).await.unwrap();
    manager.tick().await.unwrap().into_inner().unwrap();
    workflow.update().await.unwrap_err(); // should be completed
}
