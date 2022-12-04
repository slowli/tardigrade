//! Tests for `Driver`.

use assert_matches::assert_matches;
use futures::{
    future::{self, Either},
    SinkExt, TryStreamExt,
};

use super::*;
use crate::{
    backends::MockScheduler,
    engine::{AsWorkflowData, MockAnswers, MockInstance, MockPollFn},
    manager::tests::{create_test_manager, create_test_workflow, is_consumption},
};
use tardigrade::interface::{ReceiverAt, SenderAt, WithIndexing};

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

    let mut manager = create_test_manager(poll_fns, MockScheduler::default()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let mut handle = workflow.handle().with_indexing();
    let mut driver = Driver::new();
    let orders_sx = handle.remove(ReceiverAt("orders")).unwrap();
    let mut orders_sx = orders_sx.into_sink(&mut driver);
    let events_rx = handle.remove(SenderAt("events")).unwrap();
    let events_rx = events_rx.into_stream(&mut driver);
    drop(handle);

    let input_actions = async move {
        orders_sx.send(b"order".to_vec()).await.unwrap();
    };
    let (_, driver_result) = future::join(input_actions, driver.drive(&mut manager)).await;

    assert_matches!(driver_result, Termination::Finished);
    let events: Vec<_> = events_rx.try_collect().await.unwrap();
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

    let mut manager = create_test_manager(poll_fns, MockScheduler::default()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let mut handle = workflow.handle().with_indexing();
    let events_rx = handle.remove(SenderAt("events")).unwrap();
    let orders_sx = handle.remove(ReceiverAt("orders")).unwrap();
    drop(handle);

    if start_after_tick {
        manager.tick().await.unwrap().into_inner().unwrap();
        let event = events_rx.receive_message(0).await.unwrap();
        assert_eq!(event.decode().unwrap(), b"event #0");
    }

    let mut driver = Driver::new();
    let mut orders_sx = orders_sx.into_sink(&mut driver);
    let events_rx = events_rx.into_stream(&mut driver);

    let input_actions = async move {
        orders_sx.send(b"order".to_vec()).await.unwrap();
    };
    let (_, driver_result) = future::join(input_actions, driver.drive(&mut manager)).await;

    assert_matches!(driver_result, Termination::Finished);
    let events: Vec<_> = events_rx.try_collect().await.unwrap();
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

    let mut manager = create_test_manager(poll_fns, MockScheduler::default()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let mut handle = workflow.handle().with_indexing();
    let mut driver = Driver::new();
    let mut tick_results = driver.tick_results();
    let orders_sx = handle.remove(ReceiverAt("orders")).unwrap();
    let orders_id = orders_sx.channel_id();
    let mut orders_sx = orders_sx.into_sink(&mut driver);
    let events_rx = handle.remove(SenderAt("events")).unwrap();
    let events_rx = events_rx.into_stream(&mut driver);
    drop(handle);

    let input_actions = orders_sx.send(b"order".to_vec()).map(Result::unwrap);
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

        let (_, select_result) = future::join(input_actions, select_task).await;
        match select_result {
            Either::Right(((), _)) => { /* just as expected */ }
            Either::Left((driver_result, _)) => panic!("{driver_result:?}"),
        }
    }
    let events: Vec<_> = events_rx.try_collect().await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], b"event #1");

    // The `manager` can be used again.
    let mut workflow = manager.workflow(workflow_id).await.unwrap();
    let mut handle = workflow.handle().with_indexing();
    handle[ReceiverAt("orders")]
        .send(b"order #2".to_vec())
        .await
        .unwrap();
    manager.tick().await.unwrap().into_inner().unwrap();
    workflow.update().await.unwrap_err();
}
