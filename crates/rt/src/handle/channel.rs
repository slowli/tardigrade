//! Channel handles for sending / receiving messages.

use futures::{channel::mpsc, future, FutureExt, Stream, StreamExt};

use std::{convert::Infallible, marker::PhantomData, ops};

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

impl<'s, T: 'static, C: Codec<T>, S: 's + StreamMessages> MessageReceiver<T, C, S> {
    /// Streams messages from this channel starting from the specified index.
    pub fn stream_messages(
        &self,
        start_index: usize,
    ) -> impl Stream<Item = ReceivedMessage<T, C>> + 's {
        let (sx, rx) = mpsc::channel(16);
        let stream_registration = self
            .storage
            .stream_messages(self.channel_id, start_index, sx);

        let messages = stream_registration.map(|()| rx).flatten_stream();
        messages.filter_map(|message_or_eof| {
            future::ready(match message_or_eof {
                MessageOrEof::Message(index, raw_message) => Some(ReceivedMessage {
                    index,
                    raw_message,
                    _ty: PhantomData,
                }),
                MessageOrEof::Eof => None, // TODO: signal the termination somehow
            })
        })
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
