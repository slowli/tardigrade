//! Storage streaming.

use async_trait::async_trait;
use futures::{
    channel::mpsc,
    future::{self, BoxFuture, Future},
    sink,
    stream::{self, FusedStream, FuturesUnordered},
    FutureExt, SinkExt, Stream, StreamExt,
};
use pin_project_lite::pin_project;
use tracing_tunnel::PersistedMetadata;

use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    task::{Context, Poll},
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
    pub(crate) channel_id: ChannelId,
}

/// Message from a channel or the channel end marker.
#[derive(Debug)]
pub enum MessageOrEof {
    /// Message payload together with the 0-based message index.
    Message(usize, Vec<u8>),
    /// Channel end marker.
    Eof,
}

/// Allows streaming messages from a certain channel as they are written to it.
pub trait StreamMessages: Storage {
    /// Streams messages to the provided sink.
    fn stream_messages(
        &self,
        channel_id: ChannelId,
        start_index: usize,
        sink: mpsc::Sender<MessageOrEof>,
    ) -> BoxFuture<'static, ()>;
}

impl<S: StreamMessages + ?Sized> StreamMessages for &S {
    fn stream_messages(
        &self,
        channel_id: ChannelId,
        start_index: usize,
        sink: mpsc::Sender<MessageOrEof>,
    ) -> BoxFuture<'static, ()> {
        (**self).stream_messages(channel_id, start_index, sink)
    }
}

pin_project! {
    /// Stream of commits returned from [`Streaming::stream_commits()`].
    #[derive(Debug)]
    #[must_use = "should be plugged into `WorkflowManager::drive()`"]
    pub struct CommitStream {
        #[pin]
        inner: mpsc::Receiver<()>,
    }
}

impl CommitStream {
    fn new() -> (mpsc::Sender<()>, Self) {
        let (commits_sx, commits_rx) = mpsc::channel(0);
        let this = CommitStream { inner: commits_rx };
        (commits_sx, this)
    }
}

impl Stream for CommitStream {
    type Item = ();

    // Compresses a sequence of available items into a single item.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut seen_item = false;
        loop {
            let projection = self.as_mut().project();
            match projection.inner.poll_next(cx) {
                Poll::Pending => {
                    return if seen_item {
                        Poll::Ready(Some(()))
                    } else {
                        Poll::Pending
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(_)) => {
                    seen_item = true;
                }
            }
        }
    }
}

impl FusedStream for CommitStream {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[derive(Debug)]
struct MessageStreamRequest {
    channel_id: ChannelId,
    start_index: usize,
    message_sx: mpsc::Sender<MessageOrEof>,
}

#[derive(Debug)]
enum Request {
    MessageStream(MessageStreamRequest),
    Shutdown,
}

/// Storage that streams [events](MessageEvent) about new messages when they are committed.
///
/// This storage can be cheaply cloned, provided that the underlying storage is cloneable.
/// Clones will stream [message](StreamMessages) and [commit](Self::stream_commits()) events
/// to the streams configured in their parent.
#[derive(Debug, Clone)]
pub struct Streaming<S> {
    inner: S,
    message_events_sx: mpsc::Sender<MessageEvent>,
    commits_sx: mpsc::Sender<()>,
    requests_sx: mpsc::UnboundedSender<Request>,
}

impl<S: Storage + Clone> Streaming<S> {
    /// Creates a new streaming storage based on the provided storage.
    ///
    /// # Return value
    ///
    /// Returns the created storage and a routing task that handles message streaming. This task
    /// should be run in the background.
    pub fn new(storage: S) -> (Self, impl Future<Output = ()>) {
        // TODO: what is the reasonable value for channel capacity? Should it be configurable?
        let (message_events_sx, message_events_rx) = mpsc::channel(128);
        let (requests_sx, request_rx) = mpsc::unbounded();

        let this = Self {
            inner: storage.clone(),
            commits_sx: mpsc::channel(0).0,
            message_events_sx,
            requests_sx,
        };
        let router = MessageRouter::new(message_events_rx, request_rx);
        let router_task = router.run(storage);
        (this, router_task)
    }

    /// Returns a stream of commit events. Only commits that produce at least one new message
    /// or channel closure are taken into account.
    ///
    /// A previous stream returned from this method, if any,
    /// is disconnected after the call. Streaming propagates to storages cloned from this storage;
    /// if this is undesirable, call `stream_commits()` on the cloned storage and drop the output.
    pub fn stream_commits(&mut self) -> CommitStream {
        let (commits_sx, commits_rx) = CommitStream::new();
        self.commits_sx = commits_sx;
        commits_rx
    }

    /// Gracefully shuts down routing messages to streams. Existing routing tasks are allowed to
    /// complete, but no new tasks are accepted. If routing is already shut down, does nothing.
    ///
    /// This method has the same effect when called from any `Streaming` instance
    /// cloned from the original storage.
    pub fn shutdown_routing(&self) {
        self.requests_sx.unbounded_send(Request::Shutdown).ok();
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
            message_events_sx: self.message_events_sx.clone(),
            commits_sx: self.commits_sx.clone(),
            new_messages: HashMap::new(),
        }
    }

    async fn readonly_transaction(&self) -> Self::ReadonlyTransaction<'_> {
        self.inner.readonly_transaction().await
    }
}

impl<S: Storage> StreamMessages for Streaming<S> {
    fn stream_messages(
        &self,
        channel_id: ChannelId,
        start_index: usize,
        sink: mpsc::Sender<MessageOrEof>,
    ) -> BoxFuture<'static, ()> {
        let request = MessageStreamRequest {
            channel_id,
            start_index,
            message_sx: sink,
        };
        self.requests_sx
            .unbounded_send(Request::MessageStream(request))
            .ok();
        future::ready(()).boxed()
    }
}

/// Wrapper around a transaction that streams [`MessageEvent`]s on commit. Used
/// as the transaction type for [`Streaming`] storages.
///
/// Note that committing this transaction may involve dealing with backpressure.
#[derive(Debug)]
pub struct StreamingTransaction<T> {
    inner: T,
    message_events_sx: mpsc::Sender<MessageEvent>,
    commits_sx: mpsc::Sender<()>,
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
        if !self.new_messages.is_empty() {
            // If sending fails, the consumer has an unprocessed event, which is OK
            // for our purposes.
            self.commits_sx.try_send(()).ok();
        }

        let events = self
            .new_messages
            .into_keys()
            .map(|channel_id| Ok(MessageEvent { channel_id }));
        self.message_events_sx
            .send_all(&mut stream::iter(events))
            .await
            .ok();
    }
}

#[derive(Debug)]
struct Subscription {
    message_sx: mpsc::Sender<MessageOrEof>,
    channel_id: ChannelId,
    cursor: usize,
    commits_rx: CommitStream,
}

impl Subscription {
    /// Returns true if the future should be terminated.
    async fn do_send_messages<T: ReadonlyStorageTransaction>(&mut self, transaction: T) -> bool {
        let id = self.channel_id;
        let Some(record) = transaction.channel(id).await else {
            return false; // Channel may be created in the future
        };
        if record.received_messages > self.cursor {
            let mut messages = transaction
                .channel_messages(id, self.cursor..=record.received_messages)
                .map(|(index, message)| {
                    self.cursor = index + 1;
                    Ok(MessageOrEof::Message(index, message))
                });
            if self.message_sx.send_all(&mut messages).await.is_err() {
                // The messages are no longer listened by the client.
                return true;
            }
        }
        drop(transaction);

        if record.is_closed {
            self.message_sx.send(MessageOrEof::Eof).await.ok();
        }
        record.is_closed
    }

    fn send_messages<'s, T: 's + ReadonlyStorageTransaction>(
        &'s mut self,
        transaction: T,
    ) -> impl Future<Output = bool> + Send + 's {
        self.do_send_messages(transaction)
    }

    async fn subscription_task<S: Storage>(mut self, storage: &S) {
        while let Some(()) = self.commits_rx.next().await {
            // Quick check whether the message channel is still alive.
            if self.message_sx.is_closed() {
                break;
            }

            let is_completed = storage
                .readonly_transaction()
                .then(|transaction| self.send_messages(transaction));
            if is_completed.await {
                break;
            }
        }
    }
}

type SubscriptionFuture<'a> = BoxFuture<'a, (usize, ChannelId)>;

#[derive(Debug)]
struct MessageRouter {
    message_events_rx: mpsc::Receiver<MessageEvent>,
    requests_rx: mpsc::UnboundedReceiver<Request>,
    subscriptions: HashMap<usize, mpsc::Sender<()>>,
    next_subscription_id: usize,
    subscriptions_by_channel: HashMap<ChannelId, HashSet<usize>>,
}

impl MessageRouter {
    fn new(
        message_events_rx: mpsc::Receiver<MessageEvent>,
        requests_rx: mpsc::UnboundedReceiver<Request>,
    ) -> Self {
        Self {
            message_events_rx,
            requests_rx,
            subscriptions: HashMap::new(),
            next_subscription_id: 0,
            subscriptions_by_channel: HashMap::new(),
        }
    }

    async fn run<S: Storage>(mut self, storage: S) {
        let mut tasks = FuturesUnordered::<SubscriptionFuture>::new();
        loop {
            let mut next_event = self.message_events_rx.next();
            let mut next_request = self.requests_rx.select_next_some();
            let mut next_task_completion = tasks.select_next_some();

            futures::select_biased! {
                completion = next_task_completion => {
                    let (id, channel_id) = completion;
                    self.remove_subscription(id, channel_id);
                }
                maybe_event = next_event => {
                    if let Some(MessageEvent { channel_id }) = maybe_event {
                        self.notify_subscriptions(channel_id);
                    } else {
                        drop(self);
                        // ^ Signal that we don't take new requests as soon as possible
                        // and drop `self.subscriptions` in order to terminate
                        // all subscription tasks.
                        tasks.map(Ok).forward(sink::drain()).await.unwrap();
                        // ^ Wait for all existing tasks to complete.
                        break;
                    }
                }

                // Should be selected after `next_event` in order to allow events come through
                // before shutdown.
                request = next_request => match request {
                    Request::MessageStream(req) => {
                        self.insert_subscription(req, &storage, &mut tasks);
                    }
                    Request::Shutdown => {
                        self.message_events_rx.close();
                        // ^ Will trigger shutdown on the next loop iteration
                    }
                },
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
        let (mut commits_sx, commits_rx) = CommitStream::new();
        let subscription = Subscription {
            message_sx: request.message_sx,
            channel_id: request.channel_id,
            cursor: request.start_index,
            commits_rx,
        };
        let task = subscription
            .subscription_task(storage)
            .map(move |()| (id, request.channel_id))
            .boxed();
        tasks.push(task);

        self.subscriptions.insert(id, commits_sx.clone());
        commits_sx.try_send(()).ok(); // Perform the initial scan
        let subscriptions = self.subscriptions_by_channel.entry(request.channel_id);
        subscriptions.or_default().insert(id);
    }

    fn remove_subscription(&mut self, id: usize, channel_id: ChannelId) {
        self.subscriptions.remove(&id);

        let subs_for_channel = self.subscriptions_by_channel.get_mut(&channel_id).unwrap();
        subs_for_channel.remove(&id);
        if subs_for_channel.is_empty() {
            self.subscriptions_by_channel.remove(&channel_id);
        }
    }

    fn notify_subscriptions(&self, channel_id: ChannelId) {
        let Some(ids) = self.subscriptions_by_channel.get(&channel_id) else {
            return;
        };
        for &id in ids {
            if let Some(subscription) = self.subscriptions.get(&id) {
                subscription.clone().try_send(()).ok();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use async_std::task;
    use futures::future;

    use std::sync::Arc;

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

    #[async_std::test]
    async fn commit_stream_basics() {
        let (mut commits_sx, commits_rx) = mpsc::channel(2);
        let mut commits_rx = CommitStream { inner: commits_rx };
        for _ in 0..3 {
            commits_sx.try_send(()).unwrap();
        }

        commits_rx.next().await.unwrap();
        assert!(commits_rx.next().now_or_never().is_none());

        future::join(commits_rx.next(), commits_sx.send(()).map(Result::unwrap)).await;
        drop(commits_sx);
        assert!(commits_rx.next().await.is_none());
    }

    #[async_std::test]
    async fn events_are_streamed_from_storage_clones() {
        let storage = Arc::new(LocalStorage::default());
        let (mut storage, router_task) = Streaming::new(storage);
        task::spawn(router_task);
        let mut commits_rx = storage.stream_commits();
        let (messages_sx, mut messages_rx) = mpsc::channel(4);
        storage.stream_messages(1, 0, messages_sx).await;

        let storage_clone = storage.clone();
        let mut transaction = storage_clone.transaction().await;
        let channel_state = create_channel_record();
        transaction.insert_channel(1, channel_state).await;
        transaction.push_messages(1, vec![b"test".to_vec()]).await;
        transaction.commit().await;

        commits_rx.next().await.unwrap();
        assert_matches!(
            messages_rx.next().await.unwrap(),
            MessageOrEof::Message(0, message) if message == b"test"
        );
    }
}
