//! Storage streaming.

#![allow(clippy::similar_names)] // `_sx` / `_rx` naming is acceptable

use async_trait::async_trait;
use futures::{
    channel::mpsc,
    future::{BoxFuture, Future},
    lock::Mutex,
    sink,
    stream::{self, FuturesUnordered},
    FutureExt, Sink, SinkExt, StreamExt,
};
use tracing_tunnel::PersistedMetadata;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::{
    ActiveWorkflowState, ChannelRecord, ModuleRecord, ReadonlyStorageTransaction, Storage,
    StorageTransaction, WorkflowRecord, WorkflowSelectionCriteria, WorkflowState, WorkflowWaker,
    WorkflowWakerRecord, WriteChannels, WriteModules, WriteWorkflowWakers, WriteWorkflows,
};
use tardigrade::{ChannelId, WakerId, WorkflowId};

/// Event signalling that one or more messages were added to the channel.
#[derive(Debug)]
pub struct MessageEvent {
    /// Channel ID.
    pub(crate) channel_id: ChannelId,
}

#[derive(Debug)]
pub enum MessageOrEof {
    Message(usize, Vec<u8>),
    Eof,
}

#[async_trait]
pub trait StreamingStorage<S: Sink<MessageOrEof>>: Storage {
    /// Streams messages to the provided sink.
    async fn stream_messages(&self, channel_id: ChannelId, start_index: usize, sink: S);
}

#[derive(Debug)]
struct MessageStreamRequest {
    channel_id: ChannelId,
    start_index: usize,
    message_sx: mpsc::Sender<MessageOrEof>,
}

/// Storage that streams [events](MessageEvent) about new messages when they are committed.
#[derive(Debug)]
pub struct Streaming<S> {
    inner: S,
    message_events_sx: mpsc::Sender<MessageEvent>,
    message_streams_sx: mpsc::UnboundedSender<MessageStreamRequest>,
}

impl<S: Storage + Clone> Streaming<S> {
    /// Creates a new streaming storage based on the provided storage and the sink for
    /// [`MessageEvent`]s.
    pub fn new(storage: S) -> (Self, impl Future<Output = ()>) {
        let (message_events_sx, message_events_rx) = mpsc::channel(128);
        let (message_streams_sx, message_streams_rx) = mpsc::unbounded();

        let this = Self {
            inner: storage.clone(),
            message_events_sx,
            message_streams_sx,
        };
        let router = MessageRouter::new(message_events_rx, message_streams_rx);
        let router_task = router.run(storage);
        (this, router_task)
    }
}

#[async_trait]
impl<S: Storage> Storage for Streaming<S> {
    type Transaction<'a>
    where
        S: 'a,
    = StreamingTransaction<S::Transaction<'a>>;

    type ReadonlyTransaction<'a>
    where
        S: 'a,
    = S::ReadonlyTransaction<'a>;

    async fn transaction(&self) -> Self::Transaction<'_> {
        let transaction = self.inner.transaction().await;
        StreamingTransaction {
            inner: transaction,
            events_sink: self.message_events_sx.clone(),
            new_messages: HashMap::new(),
        }
    }

    async fn readonly_transaction(&self) -> Self::ReadonlyTransaction<'_> {
        self.inner.readonly_transaction().await
    }
}

#[async_trait]
impl<S: Storage> StreamingStorage<mpsc::Sender<MessageOrEof>> for Streaming<S> {
    async fn stream_messages(
        &self,
        channel_id: ChannelId,
        start_index: usize,
        sink: mpsc::Sender<MessageOrEof>,
    ) {
        let request = MessageStreamRequest {
            channel_id,
            start_index,
            message_sx: sink,
        };
        self.message_streams_sx.unbounded_send(request).ok();
    }
}

/// Wrapper around a transaction that streams [`MessageEvent`]s on commit. Used
/// as the transaction type for [`Streaming`] storages.
///
/// Note that committing this transaction may involve dealing with backpressure.
#[derive(Debug)]
pub struct StreamingTransaction<T> {
    inner: T,
    events_sink: mpsc::Sender<MessageEvent>,
    new_messages: HashMap<ChannelId, usize>,
}

delegate_read_traits!(StreamingTransaction<T> { inner: T });

#[async_trait]
impl<T: StorageTransaction> WriteModules for StreamingTransaction<T> {
    #[inline]
    async fn insert_module(&mut self, module: ModuleRecord) {
        self.inner.insert_module(module).await;
    }

    #[inline]
    async fn update_tracing_metadata(&mut self, module_id: &str, metadata: PersistedMetadata) {
        self.inner
            .update_tracing_metadata(module_id, metadata)
            .await;
    }
}

#[async_trait]
impl<T: StorageTransaction> WriteChannels for StreamingTransaction<T> {
    #[inline]
    async fn allocate_channel_id(&mut self) -> ChannelId {
        self.inner.allocate_channel_id().await
    }

    #[inline]
    async fn insert_channel(&mut self, id: ChannelId, state: ChannelRecord) {
        self.inner.insert_channel(id, state).await;
    }

    #[inline]
    async fn manipulate_channel<F: FnOnce(&mut ChannelRecord) + Send>(
        &mut self,
        id: ChannelId,
        action: F,
    ) -> ChannelRecord {
        let mut is_closed_by_action = false;
        let wrapped_action = |record: &mut ChannelRecord| {
            let was_closed = record.is_closed;
            action(record);
            is_closed_by_action = record.is_closed && !was_closed;
        };
        let record = self.inner.manipulate_channel(id, wrapped_action).await;
        if is_closed_by_action {
            self.new_messages.entry(id).or_default();
        }
        record
    }

    async fn push_messages(&mut self, id: ChannelId, messages: Vec<Vec<u8>>) {
        let len = messages.len();
        self.inner.push_messages(id, messages).await;
        *self.new_messages.entry(id).or_default() += len;
    }

    #[inline]
    async fn truncate_channel(&mut self, id: ChannelId, min_index: usize) {
        self.inner.truncate_channel(id, min_index).await;
    }
}

#[async_trait]
impl<T: StorageTransaction> WriteWorkflows for StreamingTransaction<T> {
    #[inline]
    async fn allocate_workflow_id(&mut self) -> WorkflowId {
        self.inner.allocate_workflow_id().await
    }

    #[inline]
    async fn insert_workflow(&mut self, state: WorkflowRecord) {
        self.inner.insert_workflow(state).await;
    }

    #[inline]
    async fn workflow_for_update(&mut self, id: WorkflowId) -> Option<WorkflowRecord> {
        self.inner.workflow_for_update(id).await
    }

    #[inline]
    async fn update_workflow(&mut self, id: WorkflowId, state: WorkflowState) {
        self.inner.update_workflow(id, state).await;
    }

    #[inline]
    async fn workflow_with_wakers_for_update(
        &mut self,
    ) -> Option<WorkflowRecord<ActiveWorkflowState>> {
        self.inner.workflow_with_wakers_for_update().await
    }

    #[inline]
    async fn workflow_with_consumable_channel_for_update(
        &self,
    ) -> Option<WorkflowRecord<ActiveWorkflowState>> {
        self.inner
            .workflow_with_consumable_channel_for_update()
            .await
    }
}

#[async_trait]
impl<T: StorageTransaction> WriteWorkflowWakers for StreamingTransaction<T> {
    #[inline]
    async fn insert_waker(&mut self, workflow_id: WorkflowId, waker: WorkflowWaker) {
        self.inner.insert_waker(workflow_id, waker).await;
    }

    #[inline]
    async fn insert_waker_for_matching_workflows(
        &mut self,
        criteria: WorkflowSelectionCriteria,
        waker: WorkflowWaker,
    ) {
        self.inner
            .insert_waker_for_matching_workflows(criteria, waker)
            .await;
    }

    #[inline]
    async fn wakers_for_workflow(&self, workflow_id: WorkflowId) -> Vec<WorkflowWakerRecord> {
        self.inner.wakers_for_workflow(workflow_id).await
    }

    #[inline]
    async fn delete_wakers(&mut self, workflow_id: WorkflowId, waker_ids: &[WakerId]) {
        self.inner.delete_wakers(workflow_id, waker_ids).await;
    }
}

#[async_trait]
impl<T: StorageTransaction> StorageTransaction for StreamingTransaction<T> {
    async fn commit(mut self) {
        self.inner.commit().await;
        let events = self
            .new_messages
            .into_iter()
            .map(|(channel_id, _)| Ok(MessageEvent { channel_id }));
        self.events_sink
            .send_all(&mut stream::iter(events))
            .await
            .ok();
    }
}

#[derive(Debug)]
struct SubscriptionData {
    cursor: usize,
    sent_eof: bool,
}

#[derive(Debug)]
struct Subscription {
    message_sx: mpsc::Sender<MessageOrEof>,
    channel_id: ChannelId,
    data: Arc<Mutex<SubscriptionData>>,
}

impl Subscription {
    // For some bizarre reason, the method doesn't work if provided with `&impl Storage`;
    // it requires a static boundary on the storage impl.
    fn relay_messages<'a, T, Fut>(
        &self,
        subscription_id: usize,
        transaction_fn: impl FnOnce() -> Fut + Send + 'a,
    ) -> SubscriptionFuture<'a>
    where
        T: ReadonlyStorageTransaction + 'a,
        Fut: Future<Output = T> + Send + 'a,
    {
        let data = Arc::clone(&self.data);
        let id = self.channel_id;
        let mut message_sx = self.message_sx.clone();

        let future = async move {
            // Ensure that we don't execute tasks for the same subscription concurrently,
            // and at the same time don't forget about new tasks.
            let mut data = data.lock().await;
            // Quick check whether the message channel is still alive, and we didn't terminate
            // in another task.
            if message_sx.is_closed() || data.sent_eof {
                return true;
            }

            let transaction = transaction_fn().await;
            let Some(record) = transaction.channel(id).await else {
                return false; // Channel may be created in the future
            };
            if record.received_messages > data.cursor {
                let mut messages = transaction
                    .channel_messages(id, data.cursor..=record.received_messages)
                    .map(|(index, message)| {
                        data.cursor = index + 1;
                        Ok(MessageOrEof::Message(index, message))
                    });
                if message_sx.send_all(&mut messages).await.is_err() {
                    // The messages are no longer listened by the client.
                    return true;
                }
            }
            drop(transaction);

            if record.is_closed {
                message_sx.send(MessageOrEof::Eof).await.ok();
                // ^ we're going to finish anyway, so ignoring an error is fine
                data.sent_eof = true;
            }
            record.is_closed
        };

        future
            .map(move |is_completed| is_completed.then_some(subscription_id))
            .boxed()
    }
}

type SubscriptionFuture<'a> = BoxFuture<'a, Option<usize>>;

#[derive(Debug)]
struct MessageRouter {
    message_events_rx: mpsc::Receiver<MessageEvent>,
    message_streams_rx: mpsc::UnboundedReceiver<MessageStreamRequest>,
    subscriptions: HashMap<usize, Subscription>,
    next_subscription_id: usize,
    subscriptions_by_channel: HashMap<ChannelId, HashSet<usize>>,
}

impl MessageRouter {
    fn new(
        message_events_rx: mpsc::Receiver<MessageEvent>,
        message_streams_rx: mpsc::UnboundedReceiver<MessageStreamRequest>,
    ) -> Self {
        Self {
            message_events_rx,
            message_streams_rx,
            subscriptions: HashMap::new(),
            next_subscription_id: 0,
            subscriptions_by_channel: HashMap::new(),
        }
    }

    async fn run<S: Storage>(mut self, storage: S) {
        let mut tasks = FuturesUnordered::<SubscriptionFuture>::new();
        loop {
            let mut next_event = self.message_events_rx.next();
            let mut next_request = self.message_streams_rx.select_next_some();
            let mut next_task_completion = tasks.select_next_some();

            futures::select! {
                request = next_request => {
                    self.insert_subscription(request, &storage, &mut tasks);
                }
                completion = next_task_completion => {
                    if let Some(completed_id) = completion {
                        self.remove_subscription(completed_id);
                    }
                }
                maybe_event = next_event => {
                    if let Some(MessageEvent { channel_id }) = maybe_event {
                        self.notify_subscriptions(channel_id, &storage, &mut tasks);
                    } else {
                        drop(self.message_streams_rx);
                        // ^ Signal that we don't take new requests as soon as possible.
                        tasks.map(Ok).forward(sink::drain()).await.unwrap();
                        // ^ Wait for all existing tasks to complete.
                        break;
                    }
                }
                complete => break,
            }
        }
    }

    fn insert_subscription<'a, S: Storage>(
        &mut self,
        request: MessageStreamRequest,
        storage: &'a S,
        tasks: &mut FuturesUnordered<SubscriptionFuture<'a>>,
    ) {
        let id = self.next_subscription_id;
        self.next_subscription_id += 1;

        let subscription = Subscription {
            message_sx: request.message_sx,
            channel_id: request.channel_id,
            data: Arc::new(Mutex::new(SubscriptionData {
                cursor: request.start_index,
                sent_eof: false,
            })),
        };
        let task = subscription.relay_messages(id, || storage.readonly_transaction());
        tasks.push(task);

        self.subscriptions.insert(id, subscription);
        let subscriptions = self.subscriptions_by_channel.entry(request.channel_id);
        subscriptions.or_default().insert(id);
    }

    fn remove_subscription(&mut self, id: usize) {
        let Some(subscription) = self.subscriptions.remove(&id) else {
            return; // was already removed
        };

        let subs_for_channel = self
            .subscriptions_by_channel
            .get_mut(&subscription.channel_id)
            .unwrap();
        subs_for_channel.remove(&id);
        if subs_for_channel.is_empty() {
            self.subscriptions_by_channel
                .remove(&subscription.channel_id);
        }
    }

    fn notify_subscriptions<'a, S: Storage>(
        &self,
        channel_id: ChannelId,
        storage: &'a S,
        tasks: &mut FuturesUnordered<SubscriptionFuture<'a>>,
    ) {
        let Some(ids) = self.subscriptions_by_channel.get(&channel_id) else {
            return;
        };

        let new_tasks = ids.iter().filter_map(|&id| {
            let subscription = self.subscriptions.get(&id)?;
            let task = subscription.relay_messages(id, || storage.readonly_transaction());
            Some(task)
        });
        tasks.extend(new_tasks);
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use async_std::task;

    use super::*;
    use crate::storage::LocalStorage;

    fn create_channel_record() -> ChannelRecord {
        ChannelRecord {
            receiver_workflow_id: None,
            sender_workflow_ids: HashSet::new(),
            has_external_sender: true,
            is_closed: false,
            received_messages: 0,
        }
    }

    async fn test_streaming_storage_basics() {
        let storage = Arc::new(LocalStorage::default());
        let (storage, router_task) = Streaming::new(storage);
        task::spawn(router_task);
        let (first_sx, mut first_rx) = mpsc::channel(1);
        storage.stream_messages(1, 0, first_sx).await;
        assert!(first_rx.next().now_or_never().is_none());

        let (offset_sx, mut offset_rx) = mpsc::channel(2);
        storage.stream_messages(1, 1, offset_sx).await;
        assert!(offset_rx.next().now_or_never().is_none());

        let mut transaction = storage.transaction().await;
        let channel_state = create_channel_record();
        transaction.insert_channel(1, channel_state).await;
        transaction
            .push_messages(1, vec![b"#0".to_vec(), b"#1".to_vec()])
            .await;
        transaction.commit().await;

        let message = first_rx.next().await.unwrap();
        assert_matches!(message, MessageOrEof::Message(0, payload) if payload == b"#0");
        let message = first_rx.next().await.unwrap();
        assert_matches!(message, MessageOrEof::Message(1, payload) if payload == b"#1");
        let message = offset_rx.next().await.unwrap();
        assert_matches!(message, MessageOrEof::Message(1, payload) if payload == b"#1");

        drop(offset_rx);
        let (second_sx, mut second_rx) = mpsc::channel(1);
        storage.stream_messages(1, 0, second_sx).await;
        let messages: Vec<_> = second_rx.by_ref().take(2).collect().await;
        assert_matches!(
            messages.as_slice(),
            [MessageOrEof::Message(0, _), MessageOrEof::Message(1, _)]
        );

        let mut transaction = storage.transaction().await;
        transaction.push_messages(1, vec![b"#2".to_vec()]).await;
        transaction.commit().await;

        let message = second_rx.next().await.unwrap();
        assert_matches!(message, MessageOrEof::Message(2, payload) if payload == b"#2");

        let mut transaction = storage.transaction().await;
        transaction.push_messages(1, vec![b"#3".to_vec()]).await;
        transaction
            .manipulate_channel(1, |state| {
                state.is_closed = true;
            })
            .await;
        transaction.commit().await;

        let messages: Vec<_> = first_rx.collect().await;
        assert_matches!(
            messages.as_slice(),
            [
                MessageOrEof::Message(2, _),
                MessageOrEof::Message(3, _),
                MessageOrEof::Eof
            ]
        );
        drop(second_rx);

        let (new_sx, new_rx) = mpsc::channel(10);
        storage.stream_messages(1, 3, new_sx).await;
        let messages: Vec<_> = new_rx.collect().await;
        assert_matches!(
            messages.as_slice(),
            [MessageOrEof::Message(3, _), MessageOrEof::Eof]
        );
    }

    #[async_std::test]
    async fn streaming_storage_basics() {
        // Repeat in order to catch possible deadlocks etc.
        for _ in 0..1_000 {
            test_streaming_storage_basics().await;
        }
    }

    #[async_std::test]
    async fn isolated_channel_closure() {
        let storage = Arc::new(LocalStorage::default());
        let (storage, router_task) = Streaming::new(storage);
        task::spawn(router_task);
        let (first_sx, mut first_rx) = mpsc::channel(1);
        storage.stream_messages(1, 0, first_sx).await;

        let mut transaction = storage.transaction().await;
        let channel_state = create_channel_record();
        transaction.insert_channel(1, channel_state).await;
        transaction.commit().await;

        assert!(first_rx.next().now_or_never().is_none());

        let mut transaction = storage.transaction().await;
        transaction
            .manipulate_channel(1, |state| {
                state.is_closed = true;
            })
            .await;
        transaction.commit().await;

        let messages: Vec<_> = first_rx.collect().await;
        assert_matches!(messages.as_slice(), [MessageOrEof::Eof]);
    }

    #[async_std::test]
    async fn terminating_streams_by_dropping_storage() {
        let storage = Arc::new(LocalStorage::default());
        let (storage, router_task) = Streaming::new(storage);
        let router_task = task::spawn(router_task);
        let (first_sx, mut first_rx) = mpsc::channel(4);
        storage.stream_messages(1, 0, first_sx).await;

        drop(storage);
        router_task.await;
        assert!(first_rx.next().await.is_none());
    }

    #[async_std::test]
    async fn terminating_streams_by_aborting_router_task() {
        let storage = Arc::new(LocalStorage::default());
        let (storage, router_task) = Streaming::new(storage);
        let router_task = task::spawn(router_task);
        let (first_sx, mut first_rx) = mpsc::channel(4);
        storage.stream_messages(1, 0, first_sx).await;

        router_task.cancel().await;
        assert!(first_rx.next().await.is_none());
    }
}
