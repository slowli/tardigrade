//! Tests for `WorkflowManager`.

use anyhow::anyhow;
use assert_matches::assert_matches;
use mimicry::Answers;

use std::{collections::HashSet, task::Poll};

mod spawn;

use super::*;
use crate::{
    engine::{AsWorkflowData, MockAnswers, MockEngine, MockInstance, MockPollFn},
    receipt::{
        ChannelEvent, ChannelEventKind, Event, ExecutedFunction, ExecutionError, Receipt,
        WakeUpCause,
    },
    storage::LocalStorage,
    workflow::WorkflowAndChannelIds,
};
use tardigrade::{
    handle::{Handle, HandleMap, HandlePath, ReceiverAt, SenderAt, WithIndexing},
    spawn::{ManageChannels, ManageWorkflows},
};

const DEFINITION_ID: &str = "test@latest::TestWorkflow";

type LocalManager<C = ()> = WorkflowManager<MockEngine, C, LocalStorage>;

fn channel_id(ids: &WorkflowAndChannelIds, path: &str) -> ChannelId {
    ids.channel_ids[&HandlePath::new(path)].factor()
}

pub(crate) async fn create_test_manager<C: Clock>(
    poll_fns: MockAnswers,
    clock: C,
) -> LocalManager<C> {
    let engine = MockEngine::new(poll_fns);
    let module_record = ModuleRecord {
        id: "test@latest".to_owned(),
        bytes: Arc::new([]),
        tracing_metadata: PersistedMetadata::default(),
    };
    let module = engine.create_module(&module_record).await.unwrap();

    let mut manager = WorkflowManager::builder(engine, LocalStorage::default())
        .with_clock(clock)
        .build()
        .await
        .unwrap();

    manager.insert_module("test@latest", module).await;
    manager
}

pub(crate) async fn create_test_workflow<C: Clock>(
    manager: &LocalManager<C>,
) -> WorkflowHandle<'_, (), LocalManager<C>> {
    let builder = manager.new_workflow(DEFINITION_ID).unwrap();
    let (handles, _) = builder.handles(|_| { /* use default config */ }).await;
    builder
        .build(b"test_input".to_vec(), handles)
        .await
        .unwrap()
}

async fn tick_workflow(manager: &LocalManager, id: WorkflowId) -> Result<Receipt, ExecutionError> {
    let mut transaction = manager.storage.transaction().await;
    transaction.prepare_wakers_for_workflow(id);
    transaction.commit().await;
    let result = manager.tick().await.unwrap();
    assert_eq!(result.workflow_id(), id);
    result.drop_handle().into_inner()
}

pub(crate) fn is_consumption(event: &Event, channel_id: ChannelId) -> bool {
    if let Some(event) = event.as_channel_event() {
        event.channel_id == channel_id
            && matches!(
                event.kind,
                ChannelEventKind::ReceiverPolled {
                    result: Poll::Ready(_)
                }
            )
    } else {
        false
    }
}

async fn feed_message(
    manager: &LocalManager,
    workflow_id: WorkflowId,
    channel_id: ChannelId,
) -> Result<Receipt, ExecutionError> {
    let result = tick_workflow(manager, workflow_id).await;
    let receipt = result.as_ref().unwrap_or_else(ExecutionError::receipt);
    let consumed = receipt
        .events()
        .any(|event| is_consumption(event, channel_id));
    assert!(consumed, "{receipt:?}");
    result
}

fn initialize_task(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let channels = ctx.data().persisted.channels();
    let orders_id = channels.channel_id("orders").unwrap();
    let traces_id = channels.channel_id("traces").unwrap();

    let mut orders = ctx.data_mut().receiver(orders_id);
    let poll_result = orders.poll_next().into_inner(ctx)?;
    assert!(poll_result.is_pending());

    let mut traces = ctx.data_mut().sender(traces_id);
    let _ = traces.start_send(b"trace #1".to_vec());
    // ^ Intentionally ignore send result

    Ok(Poll::Pending)
}

#[async_std::test]
async fn instantiating_workflow() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let workflow = create_test_workflow(&manager).await;

    let storage = manager.storage.readonly_transaction().await;
    let record = storage.workflow(workflow.id()).await.unwrap();
    assert_eq!(record.module_id, "test@latest");
    assert_eq!(record.name_in_module, "TestWorkflow");
    let orders_id = channel_id(workflow.ids(), "orders");
    let orders_record = storage.channel(orders_id).await.unwrap();
    assert_eq!(orders_record.receiver_workflow_id, Some(workflow.id()));
    assert_eq!(orders_record.sender_workflow_ids, HashSet::new());
    assert!(orders_record.has_external_sender);

    let traces_id = channel_id(workflow.ids(), "traces");
    let traces_record = storage.channel(traces_id).await.unwrap();
    assert_eq!(traces_record.receiver_workflow_id, None);
    assert_eq!(
        traces_record.sender_workflow_ids,
        HashSet::from_iter([workflow.id()])
    );
    drop(storage);

    poll_fn_sx
        .send(initialize_task)
        .async_scope(test_initializing_workflow(&manager, workflow.ids()))
        .await;
}

async fn test_initializing_workflow(manager: &LocalManager<()>, ids: &WorkflowAndChannelIds) {
    let receipt = manager.tick().await.unwrap().into_inner().unwrap();
    assert_eq!(receipt.executions().len(), 2);
    let main_execution = &receipt.executions()[0];
    assert_matches!(main_execution.function, ExecutedFunction::Entry { .. });

    let traces_id = channel_id(ids, "traces");
    let transaction = manager.storage.readonly_transaction().await;
    let traces = transaction.channel(traces_id).await.unwrap();
    assert_eq!(traces.received_messages, 1);
    let message = transaction.channel_message(traces_id, 0).await.unwrap();
    assert_eq!(message, b"trace #1");
}

#[async_std::test]
async fn initializing_workflow_with_closed_channels() {
    let test_channels: MockPollFn = |ctx| {
        let channels = ctx.data().persisted.channels();
        let orders_id = channels.channel_id("orders").unwrap();
        let traces_id = channels.channel_id("traces").unwrap();

        let mut traces = ctx.data_mut().sender(traces_id);
        let poll_result = traces.poll_ready().into_inner(ctx)?;
        assert_matches!(poll_result, Poll::Ready(Err(SendError::Closed)));

        let mut orders = ctx.data_mut().receiver(orders_id);
        let poll_result = orders.poll_next().into_inner(ctx)?;
        assert_matches!(poll_result, Poll::Ready(None));

        Ok(Poll::Pending)
    };

    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = &create_test_manager(poll_fns, ()).await;
    let builder = manager.new_workflow::<()>(DEFINITION_ID).unwrap();
    let handles = builder.handles(|config| {
        let config = config.with_indexing();
        config[ReceiverAt("orders")].close();
        config[SenderAt("traces")].close();
    });
    let (local_handles, _) = handles.await;
    let workflow = builder
        .build(b"test_input".to_vec(), local_handles)
        .await
        .unwrap();
    let workflow_id = workflow.id();

    assert_eq!(channel_id(workflow.ids(), "orders"), 0);
    assert_eq!(channel_id(workflow.ids(), "traces"), 0);

    poll_fn_sx
        .send(test_channels)
        .async_scope(tick_workflow(manager, workflow_id))
        .await
        .unwrap();

    let workflow = manager.workflow(workflow_id).await.unwrap();
    let mut handle = workflow.handle().await.with_indexing();
    let channel_info = handle[ReceiverAt("orders")].channel_info();
    assert!(channel_info.is_closed);
    assert_eq!(channel_info.received_messages, 0);
    let channel_info = handle[SenderAt("traces")].channel_info();
    assert!(channel_info.is_closed);

    let orders = handle.remove(ReceiverAt("orders")).unwrap();
    assert!(!orders.can_manipulate()); // the channel is closed
}

#[async_std::test]
async fn closing_workflow_channels() {
    let block_on_flush: MockPollFn = |ctx| {
        let channels = ctx.data().persisted.channels();
        let events_id = channels.channel_id("events").unwrap();
        let mut events = ctx.data_mut().sender(events_id);

        events.start_send(b"event #1".to_vec())?;
        let poll_result = events.poll_flush().into_inner(ctx)?;
        assert!(poll_result.is_pending());
        Ok(Poll::Pending)
    };
    let drop_receiver: MockPollFn = |ctx| {
        // Check that the channel sender is closed. Note that flushing the channel
        // must succeed (messages are flushed before the closure).
        let channels = ctx.data().persisted.channels();
        let orders_id = channels.channel_id("orders").unwrap();
        let events_id = channels.channel_id("events").unwrap();

        let mut events = ctx.data_mut().sender(events_id);
        let poll_result = events.poll_flush().into_inner(ctx)?;
        assert_matches!(poll_result, Poll::Ready(Ok(_)));

        let mut events = ctx.data_mut().sender(events_id);
        let poll_result = events.poll_ready().into_inner(ctx)?;
        assert_matches!(poll_result, Poll::Ready(Err(SendError::Closed)));

        let _wakers = ctx.data_mut().receiver(orders_id).drop();
        Ok(Poll::Pending)
    };

    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let events_id = channel_id(workflow.ids(), "events");
    let orders_id = channel_id(workflow.ids(), "orders");

    poll_fn_sx
        .send(block_on_flush)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    manager.close_host_receiver(events_id).await;
    let channel_info = manager.channel(events_id).await.unwrap();
    assert!(channel_info.is_closed);

    workflow.update().await.unwrap();
    let events: Vec<_> = workflow.persisted().pending_wakeup_causes().collect();
    assert_matches!(
        events.as_slice(),
        [WakeUpCause::Flush { channel_id, .. }] if *channel_id == events_id
    );

    poll_fn_sx
        .send(drop_receiver)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    let channel_info = manager.channel(orders_id).await.unwrap();
    assert!(channel_info.is_closed);
}

fn extract_channel_events(
    receipt: &Receipt,
    target_channel_id: ChannelId,
) -> Vec<&ChannelEventKind> {
    let channel_events = receipt.events().filter_map(|event| {
        if let Some(ChannelEvent { kind, channel_id }) = event.as_channel_event() {
            if *channel_id == target_channel_id {
                return Some(kind);
            }
        }
        None
    });
    channel_events.collect()
}

async fn test_closing_receiver_from_host_side(with_message: bool) {
    let poll_receiver: MockPollFn = |ctx| {
        let channels = ctx.data().persisted.channels();
        let orders_id = channels.channel_id("orders").unwrap();
        let mut orders = ctx.data_mut().receiver(orders_id);
        let poll_result = orders.poll_next().into_inner(ctx)?;
        assert_matches!(poll_result, Poll::Ready(Some(_)));

        let mut orders = ctx.data_mut().receiver(orders_id);
        let poll_result = orders.poll_next().into_inner(ctx)?;
        assert!(poll_result.is_pending());
        // ^ we don't dispatch the closure signal immediately
        Ok(Poll::Pending)
    };
    let test_closed_channel: MockPollFn = |ctx| {
        let channels = ctx.data().persisted.channels();
        let orders_id = channels.channel_id("orders").unwrap();
        let mut orders = ctx.data_mut().receiver(orders_id);
        let poll_result = orders.poll_next().into_inner(ctx)?;
        assert_matches!(poll_result, Poll::Ready(None));
        Ok(Poll::Pending)
    };

    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let orders_id = channel_id(workflow.ids(), "orders");

    poll_fn_sx
        .send(initialize_task)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    if with_message {
        manager
            .send_message(orders_id, b"order #1".to_vec())
            .await
            .unwrap();
    }
    manager.close_host_sender(orders_id).await;

    if with_message {
        let receipt = poll_fn_sx
            .send(poll_receiver)
            .async_scope(feed_message(&manager, workflow_id, orders_id))
            .await
            .unwrap();
        let order_events = extract_channel_events(&receipt, orders_id);
        assert_matches!(
            order_events.as_slice(),
            [
                ChannelEventKind::ReceiverPolled {
                    result: Poll::Ready(Some(8)),
                },
                ChannelEventKind::ReceiverPolled {
                    result: Poll::Pending,
                },
            ]
        );
    }

    let receipt = poll_fn_sx
        .send(test_closed_channel)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    let order_events = extract_channel_events(&receipt, orders_id);
    assert_matches!(
        order_events.as_slice(),
        [ChannelEventKind::ReceiverPolled {
            result: Poll::Ready(None),
        }]
    );
}

#[async_std::test]
async fn closing_receiver_from_host_side() {
    test_closing_receiver_from_host_side(false).await;
}

#[async_std::test]
async fn closing_receiver_with_message_from_host_side() {
    test_closing_receiver_from_host_side(true).await;
}

#[async_std::test]
async fn error_initializing_workflow() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();

    let err = poll_fn_sx
        .send(|_| Err(anyhow!("oops")))
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap_err();
    let err = err.trap().to_string();
    assert!(err.contains("oops"), "{err}");

    workflow.update().await.unwrap_err();
    let block_err = manager.tick().await.unwrap_err();
    assert!(block_err.nearest_timer_expiration().is_none());

    {
        let workflow = manager.any_workflow(workflow_id).await.unwrap();
        assert!(workflow.is_errored());
        let workflow = workflow.unwrap_errored();
        assert_eq!(workflow.id(), workflow_id);
        let err = workflow.error().trap().to_string();
        assert!(err.contains("oops"), "{err}");
        assert_eq!(workflow.messages().count(), 0);

        workflow.consider_repaired().await.unwrap();
    }

    let receipt = poll_fn_sx
        .send(initialize_task)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    assert!(!receipt.executions().is_empty());
}

fn poll_receiver(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let channels = ctx.data().persisted.channels();
    let orders_id = channels.channel_id("orders").unwrap();
    let mut orders = ctx.data_mut().receiver(orders_id);
    let poll_result = orders.poll_next().into_inner(ctx)?;
    assert!(poll_result.is_pending());

    Ok(Poll::Pending)
}

fn consume_message(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let channels = ctx.data().persisted.channels();
    let orders_id = channels.channel_id("orders").unwrap();
    let mut orders = ctx.data_mut().receiver(orders_id);
    let poll_result = orders.poll_next().into_inner(ctx)?;
    assert_matches!(poll_result, Poll::Ready(Some(_)));

    Ok(Poll::Pending)
}

#[async_std::test]
async fn sending_message_to_workflow() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let orders_id = channel_id(workflow.ids(), "orders");

    poll_fn_sx
        .send(poll_receiver)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    manager
        .send_message(orders_id, b"order #1".to_vec())
        .await
        .unwrap();

    {
        let transaction = manager.storage.readonly_transaction().await;
        let order_count = transaction
            .channel(orders_id)
            .await
            .unwrap()
            .received_messages;
        assert_eq!(order_count, 1);
        let order = transaction.channel_message(orders_id, 0).await.unwrap();
        assert_eq!(order, b"order #1");
    }

    let receipt = poll_fn_sx
        .send(consume_message)
        .async_scope(feed_message(&manager, workflow_id, orders_id))
        .await
        .unwrap();
    assert_eq!(receipt.executions().len(), 2); // waker + task
    let waker_execution = &receipt.executions()[1];
    assert_matches!(
        waker_execution.function,
        ExecutedFunction::Task {
            wake_up_cause: WakeUpCause::InboundMessage { .. },
            ..
        }
    );

    workflow.update().await.unwrap();
    let orders_state = workflow.persisted().receiver(orders_id).unwrap();
    assert_eq!(orders_state.received_message_count(), 1);
}

#[async_std::test]
async fn error_processing_inbound_message_in_workflow() {
    let error_after_consuming_message: MockPollFn = |ctx| {
        let channels = ctx.data().persisted.channels();
        let orders_id = channels.channel_id("orders").unwrap();
        let mut orders = ctx.data_mut().receiver(orders_id);
        let poll_result = orders.poll_next().into_inner(ctx)?;
        assert_matches!(poll_result, Poll::Ready(Some(_)));

        Err(anyhow!("oops"))
    };

    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let orders_id = channel_id(workflow.ids(), "orders");

    poll_fn_sx
        .send(initialize_task)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    manager
        .send_message(orders_id, b"test".to_vec())
        .await
        .unwrap();
    let err = poll_fn_sx
        .send(error_after_consuming_message)
        .async_scope(feed_message(&manager, workflow_id, orders_id))
        .await
        .unwrap_err();
    let err = err.trap().to_string();
    assert!(err.contains("oops"), "{err}");

    let channel_info = manager.channel(orders_id).await.unwrap();
    assert!(!channel_info.is_closed);
    assert_eq!(channel_info.received_messages, 1);

    {
        let workflow = manager.any_workflow(workflow_id).await.unwrap();
        let workflow = workflow.unwrap_errored();
        let mut message_refs: Vec<_> = workflow.messages().collect();
        assert_eq!(message_refs.len(), 1);
        let message_ref = message_refs.pop().unwrap();
        let message = message_ref.receive().await.unwrap();
        assert_eq!(message.decode().unwrap(), b"test");
        message_ref.drop_for_workflow().await.unwrap();

        let message_ref = workflow.messages().next().unwrap();
        message_ref.drop_for_workflow().await.unwrap_err();
        workflow.consider_repaired().await.unwrap();
    }

    workflow.update().await.unwrap();
    let mut channels = workflow.persisted().receivers();
    let orders_cursor = channels.find_map(|(id, state)| {
        if id == orders_id {
            Some(state.received_message_count())
        } else {
            None
        }
    });
    assert_eq!(orders_cursor, Some(1));
}

#[async_std::test]
async fn workflow_not_consuming_inbound_message() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let orders_id = channel_id(workflow.ids(), "orders");

    poll_fn_sx
        .send(poll_receiver)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    manager
        .send_message(orders_id, b"order #1".to_vec())
        .await
        .unwrap();
    let tick_result = poll_fn_sx
        .send(|_| Ok(Poll::Pending))
        .async_scope(manager.tick())
        .await;
    tick_result.unwrap().into_inner().unwrap();

    let transaction = manager.storage.readonly_transaction().await;
    let orders_channel = transaction.channel(orders_id).await.unwrap();
    assert_eq!(orders_channel.received_messages, 1);
    let order = transaction.channel_message(orders_id, 0).await.unwrap();
    assert_eq!(order, b"order #1");
    drop(transaction);

    // Workflow wakers should be consumed to not trigger the infinite loop.
    workflow.update().await.unwrap();
    let events: Vec<_> = workflow.persisted().pending_wakeup_causes().collect();
    assert!(events.is_empty(), "{events:?}");
}

#[async_std::test]
async fn handles_shape_mismatch_error() {
    let (poll_fns, _) = Answers::channel();
    let manager = &create_test_manager(poll_fns, ()).await;

    let builder = manager.new_workflow::<()>(DEFINITION_ID).unwrap();
    let err = builder
        .build(b"test_input".to_vec(), HandleMap::new())
        .await
        .unwrap_err();
    let err = format!("{err:#}");
    assert!(err.contains("invalid shape of provided handles"), "{err}");
    assert!(err.contains("missing"), "{err}");

    let mut handles = HandleMap::new();
    handles.insert(
        "orders".into(),
        Handle::Receiver(MessageReceiver::closed(manager)),
    );
    handles.insert(
        "events".into(),
        Handle::Sender(MessageSender::closed(manager)),
    );
    handles.insert(
        "traces".into(),
        Handle::Receiver(MessageReceiver::closed(manager)),
    );
    let builder = manager.new_workflow::<()>(DEFINITION_ID).unwrap();
    let err = builder
        .build(b"test_input".to_vec(), handles)
        .await
        .unwrap_err();
    let err = format!("{err:#}");
    assert!(err.contains("invalid shape of provided handles"), "{err}");
    assert!(err.contains("channel sender `traces`"), "{err}");
}

#[async_std::test]
async fn non_owned_channel_error() {
    let (poll_fns, _) = Answers::channel();
    let manager = &create_test_manager(poll_fns, ()).await;
    let workflow = create_test_workflow(manager).await;

    let orders_id = channel_id(workflow.ids(), "orders");
    let orders_rx = manager.receiver(orders_id).await.unwrap();
    let traces_id = channel_id(workflow.ids(), "traces");
    let traces_sx = manager.sender(traces_id).await.unwrap();
    let mut handles = HandleMap::new();
    handles.insert("orders".into(), Handle::Receiver(orders_rx));
    handles.insert("events".into(), Handle::Sender(traces_sx.clone()));
    handles.insert("traces".into(), Handle::Sender(traces_sx.clone()));

    let builder = manager.new_workflow::<()>(DEFINITION_ID).unwrap();
    let err = builder
        .build(b"test_input".to_vec(), handles)
        .await
        .unwrap_err();
    let err = err.to_string();
    assert!(err.contains("receiver for channel"), "{err}");
    assert!(
        err.contains("at `orders` is not owned by requester"),
        "{err}"
    );

    let (_, new_rx) = manager.create_channel().await;
    let mut handles = HandleMap::new();
    handles.insert("orders".into(), Handle::Receiver(new_rx));
    handles.insert("events".into(), Handle::Sender(traces_sx.clone()));
    handles.insert("traces".into(), Handle::Sender(traces_sx.clone()));
    let builder = manager.new_workflow::<()>(DEFINITION_ID).unwrap();
    builder
        .build(b"test_input".to_vec(), handles)
        .await
        .unwrap();

    manager.close_host_sender(traces_id).await;
    let mut handles = HandleMap::new();
    handles.insert(
        "orders".into(),
        Handle::Receiver(MessageReceiver::closed(manager)),
    );
    handles.insert(
        "events".into(),
        Handle::Sender(MessageSender::closed(manager)),
    );
    handles.insert("traces".into(), Handle::Sender(traces_sx));

    let builder = manager.new_workflow::<()>(DEFINITION_ID).unwrap();
    let err = builder
        .build(b"test_input".to_vec(), handles)
        .await
        .unwrap_err();
    let err = err.to_string();
    assert!(err.contains("sender for channel"), "{err}");
    assert!(
        err.contains("at `traces` is not owned by requester"),
        "{err}"
    );
}
