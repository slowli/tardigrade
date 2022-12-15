//! Channel handles for sending / receiving messages.

use futures::{channel::mpsc, stream::BoxStream, FutureExt, Stream, StreamExt};

use std::{
    convert::Infallible,
    fmt,
    marker::PhantomData,
    ops,
    pin::Pin,
    task::{ready, Context, Poll},
};

use crate::{
    storage::{
        helper, ChannelRecord, MessageError, MessageOrEof, ReadChannels, Storage,
        StorageTransaction, StreamMessages, WriteChannels,
    },
    utils::RefStream,
};
use tardigrade::{
    channel::SendError,
    workflow::{IntoRaw, TryFromRaw},
    ChannelId, Codec, Raw,
};

/// Handle for a workflow channel [`Receiver`] that allows sending messages via the channel.
///
/// [`Receiver`]: tardigrade::channel::Receiver
#[derive(Debug)]
pub struct MessageSender<T, C, S> {
    storage: S,
    channel_id: ChannelId,
    record: ChannelRecord,
    _ty: PhantomData<(fn(T), C)>,
}

/// [`MessageSender`] for raw bytes.
pub type RawMessageSender<S> = MessageSender<Vec<u8>, Raw, S>;

impl<T, C: Codec<T>, S: Storage + Clone> Clone for MessageSender<T, C, S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            channel_id: self.channel_id,
            record: self.record.clone(),
            _ty: PhantomData,
        }
    }
}

impl<'a, T, C: Codec<T>, S: Storage> MessageSender<T, C, S> {
    pub(super) fn closed(storage: S) -> Self {
        Self {
            storage,
            channel_id: 0,
            record: ChannelRecord::closed(),
            _ty: PhantomData,
        }
    }

    pub(super) fn new(storage: S, id: ChannelId, record: ChannelRecord) -> Self {
        Self {
            storage,
            channel_id: id,
            record,
            _ty: PhantomData,
        }
    }

    /// Returns the ID of the channel this sender is connected to.
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    /// Returns the snapshot of the state of this channel.
    ///
    /// The snapshot is taken when the handle is created and can be updated
    /// using [`Self::update()`].
    pub fn channel_info(&self) -> &ChannelRecord {
        &self.record
    }

    /// Updates the snapshot of the state of this channel.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub async fn update(&mut self) {
        let transaction = self.storage.readonly_transaction().await;
        self.record = transaction.channel(self.channel_id).await.unwrap();
    }

    /// Checks whether this sender can be used to manipulate the channel (e.g., close it)
    /// [`Self::channel_info()`] (i.e., this check can be outdated).
    /// This is possible if the channel sender is held by the host.
    pub fn can_manipulate(&self) -> bool {
        self.record.has_external_sender
    }

    /// Sends a message over the channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel is full or closed.
    pub async fn send(&self, message: T) -> Result<(), SendError> {
        self.send_all([message]).await
    }

    /// Sends the provided messages over the channel in a single transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel is full or closed.
    pub async fn send_all(&self, messages: impl IntoIterator<Item = T>) -> Result<(), SendError> {
        let raw_messages: Vec<_> = messages.into_iter().map(C::encode_value).collect();
        helper::send_messages(&self.storage, self.channel_id, raw_messages).await
    }

    /// Closes this channel from the host side.
    pub async fn close(self) {
        helper::close_host_sender(&self.storage, self.channel_id).await;
    }
}

impl<T, C: Codec<T>, S: Storage + Clone> MessageSender<T, C, &S> {
    /// Converts the storage referenced by this sender to an owned form.
    pub fn into_owned(self) -> MessageSender<T, C, S> {
        MessageSender {
            storage: self.storage.clone(),
            channel_id: self.channel_id,
            record: self.record,
            _ty: PhantomData,
        }
    }
}

impl<T, C, S> TryFromRaw<RawMessageSender<S>> for MessageSender<T, C, S>
where
    C: Codec<T>,
    S: Storage,
{
    type Error = Infallible;

    fn try_from_raw(raw: RawMessageSender<S>) -> Result<Self, Self::Error> {
        Ok(Self {
            storage: raw.storage,
            channel_id: raw.channel_id,
            record: raw.record,
            _ty: PhantomData,
        })
    }
}

impl<T, C, S> IntoRaw<RawMessageSender<S>> for MessageSender<T, C, S>
where
    C: Codec<T>,
    S: Storage,
{
    fn into_raw(self) -> RawMessageSender<S> {
        RawMessageSender {
            storage: self.storage,
            channel_id: self.channel_id,
            record: self.record,
            _ty: PhantomData,
        }
    }
}

/// Handle for an workflow channel [`Sender`] that allows taking messages from the channel.
///
/// [`Sender`]: tardigrade::channel::Sender
#[derive(Debug)]
pub struct MessageReceiver<T, C, S> {
    storage: S,
    channel_id: ChannelId,
    record: ChannelRecord,
    _ty: PhantomData<(C, fn() -> T)>,
}

/// [`MessageReceiver`] for raw bytes.
pub type RawMessageReceiver<S> = MessageReceiver<Vec<u8>, Raw, S>;

impl<T, C: Codec<T>, S: Storage> MessageReceiver<T, C, S> {
    pub(super) fn closed(storage: S) -> Self {
        Self {
            storage,
            channel_id: 0,
            record: ChannelRecord::closed(),
            _ty: PhantomData,
        }
    }

    pub(super) fn new(storage: S, id: ChannelId, record: ChannelRecord) -> Self {
        Self {
            storage,
            channel_id: id,
            record,
            _ty: PhantomData,
        }
    }

    /// Returns the ID of the channel this receiver is connected to.
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    /// Returns the snapshot of the state of this channel.
    ///
    /// The snapshot is taken when the handle is created and can be updated
    /// using [`Self::update()`].
    pub fn channel_info(&self) -> &ChannelRecord {
        &self.record
    }

    /// Updates the snapshot of the state of this channel.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub async fn update(&mut self) {
        let transaction = self.storage.readonly_transaction().await;
        self.record = transaction.channel(self.channel_id).await.unwrap();
    }

    /// Takes a message with the specified index from the channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the message is not available.
    pub async fn receive_message(
        &self,
        index: usize,
    ) -> Result<ReceivedMessage<T, C>, MessageError> {
        let transaction = self.storage.readonly_transaction().await;
        let raw_message = transaction.channel_message(self.channel_id, index).await?;

        Ok(ReceivedMessage {
            index,
            raw_message,
            _ty: PhantomData,
        })
    }

    /// Receives the messages with the specified indices.
    ///
    /// Since the messages can be truncated or not exist, it is not guaranteed that
    /// the range of indices for returned messages is precisely the requested one. It *is*
    /// guaranteed that indices of returned messages are sequential and are in the requested
    /// range.
    pub fn receive_messages(
        &self,
        indices: impl ops::RangeBounds<usize>,
    ) -> impl Stream<Item = ReceivedMessage<T, C>> + '_ {
        let start_idx = match indices.start_bound() {
            ops::Bound::Unbounded => 0,
            ops::Bound::Included(i) => *i,
            ops::Bound::Excluded(i) => *i + 1,
        };
        let end_idx = match indices.end_bound() {
            ops::Bound::Unbounded => usize::MAX,
            ops::Bound::Included(i) => *i,
            ops::Bound::Excluded(i) => i.saturating_sub(1),
        };
        let indices = start_idx..=end_idx;

        let messages_future = async {
            let tx = self.storage.readonly_transaction().await;
            RefStream::from_source(tx, |tx| tx.channel_messages(self.channel_id, indices))
        };
        messages_future
            .flatten_stream()
            .map(|(index, raw_message)| ReceivedMessage {
                index,
                raw_message,
                _ty: PhantomData,
            })
    }

    /// Checks whether this receiver can be used to manipulate the channel (e.g., close it).
    /// This is possible if the channel receiver is not held by a workflow.
    ///
    /// This check is based on the local [snapshot](Self::channel_info()) of the channel state
    /// and thus can be outdated.
    pub fn can_manipulate(&self) -> bool {
        self.record.receiver_workflow_id.is_none()
    }

    /// Truncates this channel so that its minimum message index is no less than `min_index`.
    /// If [`Self::can_manipulate()`] returns `false`, this is a no-op.
    pub async fn truncate(&self, min_index: usize) {
        if self.can_manipulate() {
            let mut transaction = self.storage.transaction().await;
            transaction
                .truncate_channel(self.channel_id, min_index)
                .await;
            transaction.commit().await;
        }
    }

    /// Closes this channel from the host side. If the receiver is not held by the host,
    /// this is a no-op.
    pub async fn close(self) {
        if self.can_manipulate() {
            helper::close_host_receiver(&self.storage, self.channel_id).await;
        }
    }
}

impl<T, C: Codec<T>, S: Storage + Clone> MessageReceiver<T, C, &S> {
    /// Converts the storage referenced by this receiver to an owned form.
    pub fn into_owned(self) -> MessageReceiver<T, C, S> {
        MessageReceiver {
            storage: self.storage.clone(),
            channel_id: self.channel_id,
            record: self.record,
            _ty: PhantomData,
        }
    }
}

impl<T: 'static, C: Codec<T>, S: StreamMessages> MessageReceiver<T, C, S> {
    /// Streams messages from this channel starting from the specified index.
    pub fn stream_messages(&self, start_index: usize) -> MessageStream<T, C> {
        let (sx, rx) = mpsc::channel(16);
        let stream_registration = self
            .storage
            .stream_messages(self.channel_id, start_index, sx);

        let messages = stream_registration.map(|()| rx).flatten_stream().boxed();
        MessageStream::new(messages)
    }
}

impl<T, C, S> TryFromRaw<RawMessageReceiver<S>> for MessageReceiver<T, C, S>
where
    C: Codec<T>,
    S: Storage,
{
    type Error = Infallible;

    fn try_from_raw(raw: RawMessageReceiver<S>) -> Result<Self, Self::Error> {
        Ok(Self {
            storage: raw.storage,
            channel_id: raw.channel_id,
            record: raw.record,
            _ty: PhantomData,
        })
    }
}

impl<T, C, S> IntoRaw<RawMessageReceiver<S>> for MessageReceiver<T, C, S>
where
    C: Codec<T>,
    S: Storage,
{
    fn into_raw(self) -> RawMessageReceiver<S> {
        RawMessageReceiver {
            storage: self.storage,
            channel_id: self.channel_id,
            record: self.record,
            _ty: PhantomData,
        }
    }
}

/// Message received from a workflow channel.
#[derive(Debug)]
pub struct ReceivedMessage<T, C> {
    index: usize,
    raw_message: Vec<u8>,
    _ty: PhantomData<(C, fn() -> T)>,
}

impl<T, C: Codec<T>> ReceivedMessage<T, C> {
    pub(super) fn new(index: usize, raw_message: Vec<u8>) -> Self {
        Self {
            index,
            raw_message,
            _ty: PhantomData,
        }
    }

    /// Returns the zero-based message index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Tries to decode the message.
    ///
    /// # Errors
    ///
    /// Returns a decoding error, if any.
    pub fn decode(self) -> Result<T, C::Error> {
        C::try_decode_bytes(self.raw_message)
    }
}

impl ReceivedMessage<Vec<u8>, Raw> {
    /// Returns the raw message bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.raw_message
    }

    /// Downcasts the message to have the specified payload type and codec. No checks are performed
    /// that this downcast is actually correct; it is caller's responsibility to ensure
    /// correctness.
    pub fn downcast<T, C: Codec<T>>(self) -> ReceivedMessage<T, C> {
        ReceivedMessage {
            index: self.index,
            raw_message: self.raw_message,
            _ty: PhantomData,
        }
    }
}

/// Stream of messages returned from [`MessageReceiver::stream_messages()`] that allows
/// checking whether the underlying channel was closed.
#[must_use = "streams do nothing unless polled"]
pub struct MessageStream<T, C> {
    inner: BoxStream<'static, MessageOrEof>,
    is_closed: bool,
    _ty: PhantomData<(C, fn() -> T)>,
}

impl<T, C: Codec<T>> Unpin for MessageStream<T, C> {}

impl<T, C: Codec<T>> fmt::Debug for MessageStream<T, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MessageStream")
            .field("is_closed", &self.is_closed)
            .finish()
    }
}

impl<T, C: Codec<T>> MessageStream<T, C> {
    fn new(inner: BoxStream<'static, MessageOrEof>) -> Self {
        Self {
            inner,
            is_closed: false,
            _ty: PhantomData,
        }
    }

    /// Checks if the channel this stream takes messages from has closed. This only
    /// makes sense to check after the channel was completely consumed; before that,
    /// the method will return `false`.
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }
}

impl<T, C: Codec<T>> Stream for MessageStream<T, C> {
    type Item = ReceivedMessage<T, C>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.is_closed {
            return Poll::Ready(None);
        }

        let Some(message) = ready!(this.inner.poll_next_unpin(cx)) else {
            return Poll::Ready(None);
        };
        Poll::Ready(match message {
            MessageOrEof::Message(index, raw_message) => {
                Some(ReceivedMessage::new(index, raw_message))
            }
            MessageOrEof::Eof => {
                this.is_closed = true;
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use async_std::task;

    use std::{collections::HashSet, sync::Arc};

    use super::*;
    use crate::storage::{LocalStorage, Streaming};

    fn create_channel_record() -> ChannelRecord {
        ChannelRecord {
            receiver_workflow_id: None,
            sender_workflow_ids: HashSet::new(),
            has_external_sender: true,
            is_closed: false,
            received_messages: 0,
        }
    }

    async fn test_message_stream_basics(close_channel: bool) {
        let storage = Arc::new(LocalStorage::default());
        let (storage, router_task) = Streaming::new(storage);
        let router_task = task::spawn(router_task);

        let mut transaction = storage.transaction().await;
        let record = create_channel_record();
        transaction.insert_channel(1, record.clone()).await;
        transaction.commit().await;

        let sender = RawMessageSender::new(&storage, 1, record.clone());
        let receiver = RawMessageReceiver::new(&storage, 1, record);
        let mut messages = receiver.stream_messages(0);

        sender.send(vec![1]).await.unwrap();
        let received = messages.next().await.unwrap();
        assert_eq!(received.index(), 0);
        assert_eq!(received.decode().unwrap().as_slice(), [1]);
        sender.send_all([vec![2], vec![3]]).await.unwrap();
        if close_channel {
            sender.close().await;
        } else {
            storage.shutdown_routing();
            router_task.await;
        }

        assert_eq!(messages.by_ref().count().await, 2);
        assert_eq!(messages.is_closed(), close_channel);
    }

    #[async_std::test]
    async fn message_stream_with_channel_closure() {
        test_message_stream_basics(true).await;
    }

    #[async_std::test]
    async fn message_stream_with_halted_routing() {
        // Repeat in order to catch possible deadlocks etc.
        for _ in 0..1_000 {
            test_message_stream_basics(false).await;
        }
    }
}
