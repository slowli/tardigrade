//! Tests related to interaction among workflows.

use futures::future;
use mimicry::AnswersSender;

use super::*;
use crate::{
    data::{tests::complete_task_with_error, ReportedErrorKind},
    receipt::{
        ChannelEventKind, ResourceEvent, ResourceEventKind, ResourceId, StubEvent, StubEventKind,
        StubId,
    },
    storage::{LocalTransaction, Readonly, WorkflowWaker},
    workflow::ChannelIds,
};
use tardigrade::{
    interface::{Handle, WithIndexing},
    task::JoinError,
};

const CHILD_ID: WorkflowId = 1;

async fn wakers_for_workflow(
    manager: &LocalManager,
    workflow_id: WorkflowId,
) -> Vec<WorkflowWaker> {
    let transaction = manager.storage.readonly_transaction().await;
    transaction
        .as_ref()
        .wakers_for_workflow(workflow_id)
        .cloned()
        .collect()
}

async fn peek_channel(
    transaction: &Readonly<LocalTransaction<'_>>,
    channel_id: ChannelId,
) -> Vec<Vec<u8>> {
    let channel = transaction.channel(channel_id).await.unwrap();
    let len = channel.received_messages;
    let messages = (0..len).map(|idx| transaction.channel_message(channel_id, idx));
    future::try_join_all(messages).await.unwrap()
}

fn allocate_channels(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let mut local_ids = ChannelIds::new();
    local_ids.insert("orders".into(), Handle::Receiver(1));
    local_ids.insert("events".into(), Handle::Sender(2));
    local_ids.insert("traces".into(), Handle::Sender(3));
    ctx.create_channels_for_stub(CHILD_ID, local_ids)?;
    Ok(Poll::Pending)
}

fn spawn_child(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let args = b"child_input".to_vec();
    let mapped_ids = ctx.take_stub_channels(CHILD_ID);
    ctx.data_mut()
        .create_workflow_stub(CHILD_ID, DEFINITION_ID, args, mapped_ids)?;
    Ok(Poll::Pending)
}

fn poll_child_traces(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let child = ctx.data().persisted.child_workflow(CHILD_ID).unwrap();
    let child_traces_id = child.channels().channel_id("traces").unwrap();
    let mut child_traces = ctx.data_mut().receiver(child_traces_id);
    let poll_result = child_traces.poll_next().into_inner(ctx)?;
    assert!(poll_result.is_pending());

    Ok(Poll::Pending)
}

async fn initialize_child(
    manager: &LocalManager,
    workflow_id: WorkflowId,
    poll_fn_sx: &mut AnswersSender<MockPollFn>,
) -> (Receipt, Receipt) {
    poll_fn_sx
        .send_all([allocate_channels, spawn_child])
        .async_scope(async {
            // Allocate channels for the created child.
            let channels_receipt = tick_workflow(manager, workflow_id).await.unwrap();
            // ...Then initialize the child.
            let child_receipt = tick_workflow(manager, workflow_id).await.unwrap();
            (channels_receipt, child_receipt)
        })
        .await
}

#[async_std::test]
async fn spawning_child_workflow() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();

    let (channels_receipt, child_receipt) =
        initialize_child(&manager, workflow_id, &mut poll_fn_sx).await;
    let executions = channels_receipt.executions().iter();
    let init_count = executions
        .filter(|execution| execution.function.is_stub_initialization())
        .count();
    assert_eq!(init_count, 1);

    let stub_events = channels_receipt.events().filter_map(|event| match event {
        Event::Stub(StubEvent { stub_id, kind }) => Some((*stub_id, kind)),
        _ => None,
    });
    let stub_events: HashMap<_, _> = stub_events.collect();
    assert_eq!(stub_events.len(), 3, "{stub_events:?}");
    for id in 1..=3 {
        assert_matches!(
            stub_events[&StubId::Channel(id)],
            StubEventKind::Mapped(Ok(_))
        );
    }

    let child_stub_events = child_receipt.events().filter_map(|event| match event {
        Event::Stub(event) => Some(event),
        _ => None,
    });
    let child_stub_events: Vec<_> = child_stub_events.collect();
    assert_matches!(
        child_stub_events.as_slice(),
        [
            StubEvent {
                stub_id: StubId::Workflow(CHILD_ID),
                kind: StubEventKind::Created,
            },
            StubEvent {
                stub_id: StubId::Workflow(CHILD_ID),
                kind: StubEventKind::Mapped(Ok(CHILD_ID)),
            },
        ]
    );

    workflow.update().await.unwrap();
    let persisted = workflow.persisted();
    let wakeup_causes: Vec<_> = persisted.pending_wakeup_causes().collect();
    assert_matches!(wakeup_causes.as_slice(), [WakeUpCause::StubInitialized]);
    let mut children: Vec<_> = persisted.child_workflows().collect();
    assert_eq!(children.len(), 1);
    let (child_id, child) = children.pop().unwrap();
    assert_eq!(child_id, CHILD_ID);
    let traces_id = child.channels().channel_id("traces").unwrap();

    poll_fn_sx
        .send(poll_child_traces)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    workflow.update().await.unwrap();
    let persisted = workflow.persisted();
    let wakeup_causes: Vec<_> = persisted.pending_wakeup_causes().collect();
    assert!(wakeup_causes.is_empty(), "{wakeup_causes:?}");

    {
        let transaction = manager.storage.readonly_transaction().await;
        assert_eq!(transaction.count_active_workflows().await, 2);
        assert!(transaction.workflow(child_id).await.is_some());

        let traces = transaction.channel(traces_id).await.unwrap();
        assert_eq!(traces.receiver_workflow_id, Some(workflow_id));
        assert_eq!(traces.sender_workflow_ids, HashSet::from_iter([child_id]));
        assert!(!traces.has_external_sender);
    }

    send_message_from_child(&manager, traces_id, &mut poll_fn_sx).await;
}

async fn send_message_from_child(
    manager: &LocalManager,
    traces_id: ChannelId,
    poll_fn_sx: &mut AnswersSender<MockPollFn>,
) {
    let workflow_id = 0;

    // Emulate the child workflow putting a message to the `traces` channel.
    poll_fn_sx
        .send(|_| Ok(Poll::Pending))
        .async_scope(tick_workflow(manager, CHILD_ID))
        .await
        .unwrap();
    manager
        .send_message(traces_id, b"trace".to_vec())
        .await
        .unwrap();

    let receive_child_trace: MockPollFn = |ctx| {
        let child_channels = ctx
            .data()
            .persisted
            .child_workflow(CHILD_ID)
            .unwrap()
            .channels();
        let child_traces_id = child_channels.channel_id("traces").unwrap();
        let mut child_traces = ctx.data_mut().receiver(child_traces_id);
        let poll_result = child_traces.poll_next().into_inner(ctx)?;
        assert_matches!(poll_result, Poll::Ready(Some(_)));

        Ok(Poll::Pending)
    };
    let receipt = poll_fn_sx
        .send(receive_child_trace)
        .async_scope(feed_message(manager, workflow_id, traces_id))
        .await
        .unwrap();

    let events = extract_channel_events(&receipt, traces_id);
    assert_matches!(
        events.as_slice(),
        [ChannelEventKind::ReceiverPolled {
            result: Poll::Ready(Some(5)),
        }]
    );

    // Check channel handles for child workflow
    let handle = manager.workflow(CHILD_ID).await.unwrap().handle();
    let mut handle = handle.with_indexing();
    let mut child_orders = handle.remove(ReceiverAt("orders")).unwrap();
    let child_orders_id = child_orders.channel_id();
    child_orders.send(b"test".to_vec()).await.unwrap();
    child_orders.close().await; // no-op: a channel sender is not owned by the host

    let child_events = handle.remove(SenderAt("events")).unwrap();
    let child_events_id = child_events.channel_id();
    assert!(!child_events.can_manipulate());
    child_events.close().await; // no-op: the channel receiver is not owned by the host

    let transaction = manager.storage.readonly_transaction().await;
    let child_orders = transaction.channel(child_orders_id).await.unwrap();
    assert!(!child_orders.is_closed);
    let child_events = transaction.channel(child_events_id).await.unwrap();
    assert!(!child_events.is_closed);
}

#[async_std::test]
async fn sending_message_to_child() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    initialize_child(&manager, workflow_id, &mut poll_fn_sx).await;

    let send_message_to_child: MockPollFn = |ctx| {
        let child = ctx.data().persisted.child_workflow(CHILD_ID);
        let child_channels = child.unwrap().channels();
        let child_orders_id = child_channels.channel_id("orders").unwrap();
        let mut child_orders = ctx.data_mut().sender(child_orders_id);
        let poll_result = child_orders.poll_ready().into_inner(ctx)?;
        assert_matches!(poll_result, Poll::Ready(Ok(_)));

        let mut child_orders = ctx.data_mut().sender(child_orders_id);
        child_orders.start_send(b"child_order".to_vec())?;
        Ok(Poll::Pending)
    };
    poll_fn_sx
        .send(send_message_to_child)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();

    let child = manager.workflow(CHILD_ID).await.unwrap();
    let child_channels = child.persisted().channels();
    let child_orders_id = child_channels.channel_id("orders").unwrap();
    {
        let transaction = manager.storage.readonly_transaction().await;
        let child_orders = peek_channel(&transaction, child_orders_id).await;
        assert_eq!(child_orders.len(), 1);
        assert_eq!(child_orders[0], b"child_order");
    }

    let init_child: MockPollFn = |ctx| {
        let channels = ctx.data().persisted.channels();
        let orders_id = channels.channel_id("orders").unwrap();
        let mut orders = ctx.data_mut().receiver(orders_id);
        let poll_result = orders.poll_next().into_inner(ctx)?;
        assert!(poll_result.is_pending());
        Ok(Poll::Pending)
    };
    poll_fn_sx
        .send(init_child)
        .async_scope(tick_workflow(&manager, CHILD_ID))
        .await
        .unwrap();

    let poll_orders_in_child: MockPollFn = |ctx| {
        let channels = ctx.data().persisted.channels();
        let orders_id = channels.channel_id("orders").unwrap();
        let mut orders = ctx.data_mut().receiver(orders_id);
        let poll_result = orders.poll_next().into_inner(ctx)?;
        assert_matches!(poll_result, Poll::Ready(Some(_)));
        Ok(Poll::Pending)
    };
    poll_fn_sx
        .send(poll_orders_in_child)
        .async_scope(feed_message(&manager, CHILD_ID, child_orders_id))
        .await
        .unwrap();
}

fn test_child_channels_after_closure(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let child_channels = ctx
        .data()
        .persisted
        .child_workflow(CHILD_ID)
        .unwrap()
        .channels();
    let child_orders_id = child_channels.channel_id("orders").unwrap();
    let child_traces_id = child_channels.channel_id("traces").unwrap();

    let mut child_orders = ctx.data_mut().sender(child_orders_id);
    let poll_result = child_orders.poll_ready().into_inner(ctx)?;
    assert_matches!(poll_result, Poll::Ready(Err(SendError::Closed)));

    let mut child_traces = ctx.data_mut().receiver(child_traces_id);
    let poll_result = child_traces.poll_next().into_inner(ctx)?;
    assert_matches!(poll_result, Poll::Ready(None));

    Ok(Poll::Pending)
}

async fn test_child_workflow_channel_management(complete_child: bool) {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();

    initialize_child(&manager, workflow_id, &mut poll_fn_sx).await;

    let poll_child_traces_and_completion: MockPollFn = |ctx| {
        let _ = poll_child_traces(ctx)?;
        let mut child = ctx.data_mut().child(CHILD_ID);
        let poll_result = child.poll_completion().into_inner(ctx)?;
        assert!(poll_result.is_pending());

        Ok(Poll::Pending)
    };
    poll_fn_sx
        .send(poll_child_traces_and_completion)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();

    workflow.update().await.unwrap();
    let mut children: Vec<_> = workflow.persisted().child_workflows().collect();
    assert_eq!(children.len(), 1);
    let (child_id, child_state) = children.pop().unwrap();
    assert_eq!(child_id, CHILD_ID);
    let orders_id = child_state.channels().channel_id("orders").unwrap();
    let traces_id = child_state.channels().channel_id("traces").unwrap();

    let poll_child_workflow: MockPollFn = if complete_child {
        |_| Ok(Poll::Ready(()))
    } else {
        |ctx| {
            let channels = ctx.data().persisted.channels();
            let orders_id = channels.channel_id("orders").unwrap();
            let traces_id = channels.channel_id("traces").unwrap();

            let _wakers = ctx.data_mut().receiver(orders_id).drop();
            let _wakers = ctx.data_mut().sender(traces_id).drop();
            Ok(Poll::Pending)
        }
    };
    poll_fn_sx
        .send(poll_child_workflow)
        .async_scope(tick_workflow(&manager, child_id))
        .await
        .unwrap();

    let channel_info = manager.channel(orders_id).await.unwrap();
    assert!(channel_info.is_closed);
    let channel_info = manager.channel(traces_id).await.unwrap();
    assert!(channel_info.is_closed);

    let wakers = wakers_for_workflow(&manager, workflow_id).await;
    assert_matches!(
        wakers[0],
        WorkflowWaker::SenderClosure(id) if id == orders_id
    );
    if complete_child {
        assert_eq!(wakers.len(), 2);
        assert_matches!(wakers[1], WorkflowWaker::ChildCompletion(CHILD_ID));
    } else {
        assert_eq!(wakers.len(), 1);
    }

    test_child_resources_after_drop(&manager, poll_fn_sx, complete_child).await;
}

async fn test_child_resources_after_drop(
    manager: &LocalManager,
    mut poll_fn_sx: AnswersSender<MockPollFn>,
    complete_child: bool,
) {
    let workflow_id = 0;
    let mut workflow = manager.workflow(workflow_id).await.unwrap();

    let test_child_resources: MockPollFn = if complete_child {
        |ctx| {
            let _ = test_child_channels_after_closure(ctx)?;
            let mut child = ctx.data_mut().child(CHILD_ID);
            let poll_result = child.poll_completion().into_inner(ctx)?;
            assert_matches!(poll_result, Poll::Ready(Ok(())));

            Ok(Poll::Pending)
        }
    } else {
        test_child_channels_after_closure
    };
    let receipt = poll_fn_sx
        .send(test_child_resources)
        .async_scope(tick_workflow(manager, workflow_id))
        .await
        .unwrap();

    workflow.update().await.unwrap();
    let child_state = workflow.persisted().child_workflow(CHILD_ID).unwrap();
    if complete_child {
        assert_matches!(child_state.result(), Poll::Ready(Ok(())));
    } else {
        assert_matches!(child_state.result(), Poll::Pending);
    }
    if complete_child {
        let child_completed = receipt.events().any(|event| {
            matches!(
                event.as_resource_event(),
                Some(ResourceEvent {
                    resource_id: ResourceId::Workflow(CHILD_ID),
                    kind: ResourceEventKind::Polled(Poll::Ready(())),
                })
            )
        });
        assert!(child_completed);
    }
}

#[async_std::test]
async fn child_workflow_completion() {
    test_child_workflow_channel_management(true).await;
}

#[async_std::test]
async fn child_workflow_channel_closure() {
    test_child_workflow_channel_management(false).await;
}

fn allocate_channels_with_copied_sender(
    ctx: &mut MockInstance,
    copy_traces: bool,
) -> anyhow::Result<Poll<()>> {
    let mut local_ids = ChannelIds::new();
    local_ids.insert("orders".into(), Handle::Receiver(1));
    if !copy_traces {
        local_ids.insert("traces".into(), Handle::Sender(2));
    }
    ctx.create_channels_for_stub(CHILD_ID, local_ids)?;
    Ok(Poll::Pending)
}

fn spawn_child_with_copied_sender(
    ctx: &mut MockInstance,
    copy_traces: bool,
) -> anyhow::Result<Poll<()>> {
    let channels = ctx.data().persisted.channels();
    let events_id = channels.channel_id("events").unwrap();

    let mut handles = ctx.take_stub_channels(CHILD_ID);
    let config = Handle::Sender(events_id);
    handles.insert("events".into(), config);
    if copy_traces {
        handles.insert("traces".into(), config);
    }
    assert_eq!(handles.len(), 3);

    let args = b"child_input".to_vec();
    ctx.data_mut()
        .create_workflow_stub(CHILD_ID, DEFINITION_ID, args, handles)?;
    Ok(Poll::Pending)
}

fn poll_child_completion_with_copied_channel(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let child = ctx.data().persisted.child_workflow(CHILD_ID).unwrap();
    let err = child.channels().handle("events").unwrap_err();
    let err = err.to_string();
    assert!(err.contains("not captured"), "{err}");

    let mut child = ctx.data_mut().child(CHILD_ID);
    let poll_result = child.poll_completion().into_inner(ctx)?;
    assert!(poll_result.is_pending());
    Ok(Poll::Pending)
}

async fn init_workflow_with_copied_child_channel(
    manager: &LocalManager,
    workflow_id: WorkflowId,
    poll_fn_sx: &mut AnswersSender<MockPollFn>,
) {
    let allocate_channels: MockPollFn = |ctx| allocate_channels_with_copied_sender(ctx, false);
    let spawn_child: MockPollFn = |ctx| spawn_child_with_copied_sender(ctx, false);
    poll_fn_sx
        .send_all([
            allocate_channels,
            spawn_child,
            poll_child_completion_with_copied_channel,
        ])
        .async_scope(async {
            tick_workflow(manager, workflow_id).await.unwrap(); // allocate channels
            tick_workflow(manager, workflow_id).await.unwrap(); // spawn child
            tick_workflow(manager, workflow_id).await.unwrap(); // poll child completion
        })
        .await;
}

#[async_std::test]
async fn spawning_child_with_copied_sender() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let events_id = channel_id(workflow.ids(), "events");

    init_workflow_with_copied_child_channel(&manager, workflow_id, &mut poll_fn_sx).await;

    let events_channel = manager.channel(events_id).await.unwrap();
    assert_eq!(
        events_channel.sender_workflow_ids,
        HashSet::from_iter([workflow_id, CHILD_ID])
    );
    let child = manager.workflow(CHILD_ID).await.unwrap();
    let child_events_id = child.persisted().channels().channel_id("events").unwrap();
    assert_eq!(child_events_id, events_id);

    let write_event_and_complete_child: MockPollFn = |ctx| {
        let channels = ctx.data().persisted.channels();
        let events_id = channels.channel_id("events").unwrap();
        let mut events = ctx.data_mut().sender(events_id);
        events.start_send(b"child_event".to_vec())?;
        Ok(Poll::Ready(()))
    };
    poll_fn_sx
        .send(write_event_and_complete_child)
        .async_scope(tick_workflow(&manager, CHILD_ID))
        .await
        .unwrap();

    let events_channel = manager.channel(events_id).await.unwrap();
    assert_eq!(
        events_channel.sender_workflow_ids,
        HashSet::from_iter([workflow_id])
    );
    assert_eq!(events_channel.receiver_workflow_id, None);

    let transaction = manager.storage.readonly_transaction().await;
    let events = peek_channel(&transaction, events_id).await;
    assert_eq!(events, [b"child_event"]);
}

async fn test_child_with_copied_closed_sender(close_before_spawn: bool) {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let events_id = channel_id(workflow.ids(), "events");

    if close_before_spawn {
        manager.close_host_receiver(events_id).await;
    }
    init_workflow_with_copied_child_channel(&manager, workflow_id, &mut poll_fn_sx).await;
    if !close_before_spawn {
        manager.close_host_receiver(events_id).await;
    }

    let test_writing_event_in_child: MockPollFn = |ctx| {
        let channels = ctx.data().persisted.channels();
        let events_id = channels.channel_id("events").unwrap();
        let mut events = ctx.data_mut().sender(events_id);
        let send_result = events.start_send(b"child_event".to_vec());
        assert_matches!(send_result, Err(SendError::Closed));
        Ok(Poll::Pending)
    };
    poll_fn_sx
        .send(test_writing_event_in_child)
        .async_scope(tick_workflow(&manager, CHILD_ID))
        .await
        .unwrap();
}

#[async_std::test]
async fn spawning_child_with_copied_closed_sender() {
    test_child_with_copied_closed_sender(true).await;
}

#[async_std::test]
async fn spawning_child_with_copied_then_closed_sender() {
    test_child_with_copied_closed_sender(false).await;
}

async fn test_child_with_aliased_sender(complete_child: bool) {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let events_id = channel_id(workflow.ids(), "events");

    let allocate_channels: MockPollFn = |ctx| allocate_channels_with_copied_sender(ctx, true);
    let spawn_child: MockPollFn = |ctx| spawn_child_with_copied_sender(ctx, true);
    poll_fn_sx
        .send_all([
            allocate_channels,
            spawn_child,
            poll_child_completion_with_copied_channel,
        ])
        .async_scope(async {
            tick_workflow(&manager, workflow_id).await.unwrap(); // allocate channels
            tick_workflow(&manager, workflow_id).await.unwrap(); // spawn child workflow
            tick_workflow(&manager, workflow_id).await.unwrap(); // poll child completion
        })
        .await;

    let events_channel = manager.channel(events_id).await.unwrap();
    assert_eq!(
        events_channel.sender_workflow_ids,
        HashSet::from_iter([workflow_id, CHILD_ID])
    );
    let child = manager.workflow(CHILD_ID).await.unwrap();
    assert_eq!(channel_id(child.ids(), "events"), events_id);
    assert_eq!(channel_id(child.ids(), "traces"), events_id);

    let write_event_and_drop_events: MockPollFn = |ctx| {
        let channels = ctx.data().persisted.channels();
        let events_id = channels.channel_id("events").unwrap();
        let traces_id = channels.channel_id("traces").unwrap();

        let mut events = ctx.data_mut().sender(events_id);
        events.start_send(b"child_event".to_vec())?;
        let _wakers = events.drop();

        let mut traces = ctx.data_mut().sender(traces_id);
        traces.start_send(b"child_trace".to_vec())?;
        let poll_result = traces.poll_flush().into_inner(ctx)?;
        assert!(poll_result.is_pending());
        Ok(Poll::Pending)
    };
    poll_fn_sx
        .send(write_event_and_drop_events)
        .async_scope(tick_workflow(&manager, CHILD_ID))
        .await
        .unwrap();

    let events_channel = manager.channel(events_id).await.unwrap();
    assert_eq!(
        events_channel.sender_workflow_ids,
        HashSet::from_iter([workflow_id, CHILD_ID])
    );
    let transaction = manager.storage.readonly_transaction().await;
    let outbound_messages = peek_channel(&transaction, events_id).await;
    assert_eq!(outbound_messages, [b"child_event" as &[u8], b"child_trace"]);
    drop(transaction);

    let guard = if complete_child {
        poll_fn_sx.send(|_| Ok(Poll::Ready(())))
    } else {
        poll_fn_sx.send(|ctx| {
            let channels = ctx.data().persisted.channels();
            let traces_id = channels.channel_id("traces").unwrap();
            let _wakers = ctx.data_mut().sender(traces_id).drop();
            Ok(Poll::Pending)
        })
    };
    guard
        .async_scope(tick_workflow(&manager, CHILD_ID))
        .await
        .unwrap();

    let events_channel = manager.channel(events_id).await.unwrap();
    assert_eq!(
        events_channel.sender_workflow_ids,
        HashSet::from_iter([workflow_id])
    );
}

#[async_std::test]
async fn spawning_child_with_aliased_sender() {
    test_child_with_aliased_sender(false).await;
}

#[async_std::test]
async fn spawning_and_completing_child_with_aliased_sender() {
    test_child_with_aliased_sender(true).await;
}

fn poll_child_completion(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let mut child = ctx.data_mut().child(CHILD_ID);
    let poll_result = child.poll_completion().into_inner(ctx)?;
    assert!(poll_result.is_pending());
    Ok(Poll::Pending)
}

#[async_std::test]
async fn completing_child_with_error() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let workflow_id = create_test_workflow(&manager).await.id();

    initialize_child(&manager, workflow_id, &mut poll_fn_sx).await;
    poll_fn_sx
        .send_all([poll_child_completion, complete_task_with_error])
        .async_scope(async {
            tick_workflow(&manager, workflow_id).await.unwrap();
            tick_workflow(&manager, CHILD_ID).await.unwrap();
        })
        .await;

    let child = manager.any_workflow(CHILD_ID).await.unwrap();
    assert!(child.is_completed());
    let child = child.unwrap_completed();
    assert_matches!(
        child.result(),
        Err(JoinError::Err(err)) if err.cause().to_string() == "error message"
    );

    let check_child_completion: MockPollFn = |ctx| {
        let mut child = ctx.data_mut().child(CHILD_ID);
        let poll_result = child.poll_completion().into_inner(ctx)?;
        let err = match poll_result {
            Poll::Ready(Err(JoinError::Err(err))) => err,
            other => panic!("unexpected poll result: {other:?}"),
        };
        let err_cause = err.cause().to_string();
        assert_eq!(err_cause, "error message");
        Ok(Poll::Pending)
    };
    let receipt = poll_fn_sx
        .send(check_child_completion)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();

    let is_child_polled = receipt.events().any(|event| {
        event.as_resource_event().map_or(false, |event| {
            matches!(event.resource_id, ResourceId::Workflow(CHILD_ID))
                && matches!(event.kind, ResourceEventKind::Polled(Poll::Ready(())))
        })
    });
    assert!(is_child_polled, "{receipt:?}");
}

fn complete_task_with_panic(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let channels = ctx.data().persisted.channels();
    let events_id = channels.channel_id("events").unwrap();
    let mut events = ctx.data_mut().sender(events_id);
    events.start_send(b"panic message".to_vec())?;

    ctx.data_mut().report_error_or_panic(
        ReportedErrorKind::Panic,
        Some("panic message".to_owned()),
        None,
        42,
        1,
    );
    Err(anyhow!("oops"))
}

fn check_aborted_child_completion(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let mut child = ctx.data_mut().child(CHILD_ID);
    let poll_result = child.poll_completion().into_inner(ctx)?;
    assert_matches!(poll_result, Poll::Ready(Err(_)));

    let child = ctx.data().persisted.child_workflow(CHILD_ID);
    let child_channels = child.unwrap().channels();
    let child_events_id = child_channels.channel_id("events").unwrap();
    let mut child_events = ctx.data_mut().receiver(child_events_id);
    let poll_result = child_events.poll_next().into_inner(ctx)?;
    assert_matches!(poll_result, Poll::Ready(None));

    Ok(Poll::Pending)
}

#[async_std::test]
async fn completing_child_with_panic() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let workflow_id = create_test_workflow(&manager).await.id();

    let tick_result = poll_fn_sx
        .send_all([allocate_channels, spawn_child])
        .async_scope(async {
            manager.tick().await.unwrap().into_inner().unwrap();
            manager.tick().await.unwrap()
        })
        .await;
    assert_eq!(tick_result.workflow_id(), workflow_id);
    tick_result.into_inner().unwrap();
    let child = manager.workflow(CHILD_ID).await.unwrap();
    let child_events_id = channel_id(child.ids(), "events");

    let tick_result = poll_fn_sx
        .send_all([poll_child_completion, complete_task_with_panic])
        .async_scope(async {
            // Both workflows are ready to be polled at this point;
            // control which one is polled first.
            tick_workflow(&manager, workflow_id).await.unwrap();
            manager.tick().await.unwrap()
        })
        .await;
    assert_eq!(tick_result.workflow_id(), CHILD_ID);
    let err_handle = tick_result.into_inner().unwrap_err();
    assert_eq!(
        err_handle.error().panic_info().unwrap().message.as_deref(),
        Some("panic message")
    );
    assert_eq!(err_handle.messages().count(), 0);

    let child = manager.any_workflow(CHILD_ID).await.unwrap();
    assert!(child.is_errored());
    let other_err_handle = child.unwrap_errored();
    assert_eq!(other_err_handle.id(), err_handle.id());
    err_handle.abort().await.unwrap();
    // The aliased handle now cannot be used for any actions:
    other_err_handle.consider_repaired().await.unwrap_err();

    let child = manager.any_workflow(CHILD_ID).await.unwrap();
    assert!(child.is_completed());
    let child = child.unwrap_completed();
    assert_matches!(child.result(), Err(JoinError::Aborted));

    assert_child_abort(&manager, &mut poll_fn_sx, workflow_id, child_events_id).await;
}

async fn assert_child_abort(
    manager: &LocalManager,
    poll_fn_sx: &mut AnswersSender<MockPollFn>,
    workflow_id: WorkflowId,
    child_events_id: ChannelId,
) {
    // Check that the event emitted by the panicking workflow was not committed.
    let child_events = manager.channel(child_events_id).await.unwrap();
    assert_eq!(child_events.received_messages, 0);
    assert!(child_events.is_closed);

    let receipt = poll_fn_sx
        .send(check_aborted_child_completion)
        .async_scope(tick_workflow(manager, workflow_id))
        .await
        .unwrap();
    let is_child_polled = receipt.events().any(|event| {
        event.as_resource_event().map_or(false, |event| {
            matches!(event.resource_id, ResourceId::Workflow(CHILD_ID))
                && matches!(event.kind, ResourceEventKind::Polled(Poll::Ready(())))
        })
    });
    assert!(is_child_polled, "{receipt:?}");
}

async fn init_workflow_with_child(
    manager: &LocalManager,
    workflow_id: WorkflowId,
    poll_fn_sx: &mut AnswersSender<MockPollFn>,
) {
    initialize_child(manager, workflow_id, poll_fn_sx).await;
    poll_fn_sx
        .send(poll_child_completion)
        .async_scope(tick_workflow(manager, workflow_id))
        .await
        .unwrap();
}

async fn test_aborting_child(initialize_child: bool) {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let workflow_id = create_test_workflow(&manager).await.id();

    init_workflow_with_child(&manager, workflow_id, &mut poll_fn_sx).await;

    let child = manager.workflow(CHILD_ID).await.unwrap();
    let child_events_id = channel_id(child.ids(), "events");
    if initialize_child {
        poll_fn_sx
            .send(|_| Ok(Poll::Pending))
            .async_scope(tick_workflow(&manager, CHILD_ID))
            .await
            .unwrap();
    }
    manager.abort_workflow(CHILD_ID).await.unwrap();

    assert_child_abort(&manager, &mut poll_fn_sx, workflow_id, child_events_id).await;
}

#[async_std::test]
async fn aborting_child_before_initialization() {
    test_aborting_child(false).await;
}

#[async_std::test]
async fn aborting_child_after_initialization() {
    test_aborting_child(true).await;
}

#[async_std::test]
async fn aborting_parent() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let manager = create_test_manager(poll_fns, ()).await;
    let workflow_id = create_test_workflow(&manager).await.id();

    init_workflow_with_child(&manager, workflow_id, &mut poll_fn_sx).await;

    let child = manager.workflow(CHILD_ID).await.unwrap();
    let child_events_id = channel_id(child.ids(), "events");
    let child_orders_id = channel_id(child.ids(), "orders");
    manager.abort_workflow(workflow_id).await.unwrap();
    let child_events = manager.channel(child_events_id).await.unwrap();
    assert!(child_events.is_closed);
    let child_orders = manager.channel(child_orders_id).await.unwrap();
    assert!(child_orders.is_closed);

    let check_child_channels: MockPollFn = |ctx| {
        let channels = ctx.data().persisted.channels();
        let orders_id = channels.channel_id("orders").unwrap();
        let events_id = channels.channel_id("events").unwrap();

        let mut orders = ctx.data_mut().receiver(orders_id);
        let poll_result = orders.poll_next().into_inner(ctx)?;
        assert_matches!(poll_result, Poll::Ready(None));
        let mut events = ctx.data_mut().sender(events_id);
        let send_result = events.start_send(b"child event".to_vec());
        assert_matches!(send_result, Err(SendError::Closed));

        Ok(Poll::Ready(()))
    };
    poll_fn_sx
        .send(check_child_channels)
        .async_scope(tick_workflow(&manager, CHILD_ID))
        .await
        .unwrap();

    assert!(manager.workflow(CHILD_ID).await.is_none());
}
