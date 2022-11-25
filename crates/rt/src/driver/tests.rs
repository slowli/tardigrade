//! Tests for `Driver`.

use assert_matches::assert_matches;
use futures::{
    future::{self, Either},
    SinkExt, TryStreamExt,
};
use mimicry::Answers;
use wasmtime::{AsContextMut, StoreContextMut};

use super::*;
use crate::{
    data::{WasmContextPtr, WorkflowData, WorkflowFunctions},
    manager::tests::{create_test_manager, create_test_workflow, is_consumption},
    mock_scheduler::MockScheduler,
    module::{ExportsMock, MockPollFn},
    utils::WasmAllocator,
};
use tardigrade::{
    abi::AllocateBytes,
    interface::{ReceiverName, SenderName},
};

const POLL_CX: WasmContextPtr = 123;

fn poll_orders(mut ctx: StoreContextMut<'_, WorkflowData>) -> anyhow::Result<Poll<()>> {
    let orders = Some(ctx.data().receiver_ref(None, "orders"));
    let poll_result =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
    assert_eq!(poll_result, -1); // Pending

    Ok(Poll::Pending)
}

fn handle_order(mut ctx: StoreContextMut<'_, WorkflowData>) -> anyhow::Result<Poll<()>> {
    let orders = Some(ctx.data().receiver_ref(None, "orders"));
    let poll_result =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
    assert_ne!(poll_result, -1);
    assert_ne!(poll_result, -2); // Ready(Some(_))

    let events = Some(ctx.data_mut().sender_ref(None, "events"));
    let (event_ptr, event_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"event #1")?;
    WorkflowFunctions::start_send(ctx.as_context_mut(), events, event_ptr, event_len)?;

    Ok(Poll::Ready(()))
}

#[async_std::test]
async fn completing_workflow_via_driver() {
    let poll_fns = Answers::from_values([poll_orders, handle_order]);
    let _guard = ExportsMock::prepare(poll_fns);

    let mut manager = create_test_manager(MockScheduler::default()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let mut handle = workflow.handle();
    let mut driver = Driver::new();
    let orders_sx = handle.remove(ReceiverName("orders")).unwrap();
    let mut orders_sx = orders_sx.into_sink(&mut driver);
    let events_rx = handle.remove(SenderName("events")).unwrap();
    let events_rx = events_rx.into_stream(&mut driver);

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
    let poll_orders_and_send_event: MockPollFn = |mut ctx| {
        let orders = Some(ctx.data().receiver_ref(None, "orders"));
        let poll_result =
            WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
        assert_eq!(poll_result, -1); // Pending

        let events = Some(ctx.data_mut().sender_ref(None, "events"));
        let (event_ptr, event_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"event #0")?;
        WorkflowFunctions::start_send(ctx.as_context_mut(), events, event_ptr, event_len)?;

        Ok(Poll::Pending)
    };
    let poll_fns = Answers::from_values([poll_orders_and_send_event, handle_order]);
    let _guard = ExportsMock::prepare(poll_fns);

    let mut manager = create_test_manager(MockScheduler::default()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let mut handle = workflow.handle();
    let events_rx = handle.remove(SenderName("events")).unwrap();
    let orders_sx = handle.remove(ReceiverName("orders")).unwrap();

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
    let handle_order_and_poll_order: MockPollFn = |mut ctx| {
        let _ = handle_order(ctx.as_context_mut())?;
        poll_orders(ctx)
    };
    let poll_fns = Answers::from_values([poll_orders, handle_order_and_poll_order, handle_order]);
    let _guard = ExportsMock::prepare(poll_fns);

    let mut manager = create_test_manager(MockScheduler::default()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let mut handle = workflow.handle();
    let mut driver = Driver::new();
    let mut tick_results = driver.tick_results();
    let orders_sx = handle.remove(ReceiverName("orders")).unwrap();
    let orders_id = orders_sx.channel_id();
    let mut orders_sx = orders_sx.into_sink(&mut driver);
    let events_rx = handle.remove(SenderName("events")).unwrap();
    let events_rx = events_rx.into_stream(&mut driver);

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
    let mut handle = workflow.handle();
    handle[ReceiverName("orders")]
        .send(b"order #2".to_vec())
        .await
        .unwrap();
    manager.tick().await.unwrap().into_inner().unwrap();
    workflow.update().await.unwrap_err();
}
