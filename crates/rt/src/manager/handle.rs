//! Handles for workflows in a [`WorkflowManager`] and their components (e.g., channels).

use futures::future;

use std::{collections::HashSet, error, fmt, marker::PhantomData};

use crate::{
    manager::{persistence::in_transaction, AsManager, WorkflowAndChannelIds},
    storage::{ChannelRecord, MessageError, ReadChannels, ReadWorkflows, Storage, WorkflowRecord},
    workflow::ChannelIds,
    PersistedWorkflow,
};
use tardigrade::{
    channel::{Receiver, SendError, Sender},
    interface::{AccessError, AccessErrorKind, InboundChannel, Interface, OutboundChannel},
    workflow::{GetInterface, TakeHandle, UntypedHandle},
    ChannelId, Decode, Encode, WorkflowId,
};

/// Error updating a [`WorkflowHandle`].
#[derive(Debug)]
#[non_exhaustive]
pub enum HandleUpdateError {
    /// The workflow that the handle is associated with was terminated.
    Terminated,
}

impl fmt::Display for HandleUpdateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Terminated => {
                formatter.write_str("workflow that the handle is associated with was terminated")
            }
        }
    }
}

impl error::Error for HandleUpdateError {}

/// Handle to a workflow in a [`WorkflowManager`].
///
/// This type is used as a type param for the [`TakeHandle`] trait. The returned handles
/// allow interacting with the workflow (e.g., [send messages](MessageSender) via inbound channels
/// and [take messages](MessageReceiver) from outbound channels).
///
/// See [`AsyncEnv`] for a more high-level, future-based alternative.
///
/// [`AsyncEnv`]: crate::manager::future::AsyncEnv
///
/// # Examples
///
/// ```
/// use tardigrade::interface::{InboundChannel, OutboundChannel};
/// use tardigrade_rt::manager::WorkflowHandle;
///
/// # fn test_wrapper(workflow: WorkflowHandle<'_, ()>) -> anyhow::Result<()> {
/// // Assume we have a dynamically typed workflow:
/// let mut workflow: WorkflowHandle<()> = // ...
/// #   workflow;
/// // We can create a handle to manipulate the workflow.
/// let mut handle = workflow.handle();
///
/// // Let's send a message via an inbound channel.
/// let message = b"hello".to_vec();
/// handle[InboundChannel("commands")].send(message)?;
///
/// // Let's then take outbound messages from a certain channel:
/// let messages = handle[OutboundChannel("events")]
///     .take_messages()
///     .unwrap();
/// let messages: Vec<Vec<u8>> = messages.decode().unwrap();
/// // ^ `decode().unwrap()` always succeeds because the codec
/// // for untyped workflows is just an identity.
///
/// // It is possible to access the underlying workflow state:
/// let persisted = workflow.persisted();
/// println!("{:?}", persisted.tasks().collect::<Vec<_>>());
/// let now = persisted.current_time();
/// # Ok(())
/// # }
/// ```
pub struct WorkflowHandle<'a, W, M> {
    manager: &'a M,
    ids: WorkflowAndChannelIds,
    host_receiver_channels: HashSet<ChannelId>,
    interface: &'a Interface,
    persisted: PersistedWorkflow,
    _ty: PhantomData<fn(W)>,
}

impl<W, M: fmt::Debug> fmt::Debug for WorkflowHandle<'_, W, M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowEnv")
            .field("manager", &self.manager)
            .field("ids", &self.ids)
            .finish()
    }
}

#[allow(clippy::mismatching_type_param_order)] // false positive
impl<'a, M: AsManager> WorkflowHandle<'a, (), M> {
    pub(super) async fn new(
        manager: &'a M,
        transaction: &impl ReadChannels,
        interface: &'a Interface,
        record: WorkflowRecord,
    ) -> WorkflowHandle<'a, (), M> {
        let ids = WorkflowAndChannelIds {
            workflow_id: record.id,
            channel_ids: record.persisted.channel_ids(),
        };
        let host_receiver_channels =
            Self::find_host_receiver_channels(transaction, &ids.channel_ids).await;

        Self {
            manager,
            ids,
            host_receiver_channels,
            interface,
            persisted: record.persisted,
            _ty: PhantomData,
        }
    }

    async fn find_host_receiver_channels(
        transaction: &impl ReadChannels,
        channel_ids: &ChannelIds,
    ) -> HashSet<ChannelId> {
        let outbound_channel_ids = channel_ids.outbound.values();
        let channel_tasks = outbound_channel_ids.map(|&id| async move {
            let channel = transaction.channel(id).await.unwrap();
            Some(id).filter(|_| channel.state.receiver_workflow_id.is_none())
        });
        let host_receiver_channels = future::join_all(channel_tasks).await;
        host_receiver_channels.into_iter().flatten().collect()
    }

    #[cfg(test)]
    pub(super) fn ids(&self) -> &WorkflowAndChannelIds {
        &self.ids
    }

    /// Attempts to downcast this handle to a specific workflow interface.
    ///
    /// # Errors
    ///
    /// Returns an error on workflow interface mismatch.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn downcast<W: GetInterface>(self) -> Result<WorkflowHandle<'a, W, M>, AccessError> {
        W::interface().check_compatibility(self.interface)?;
        Ok(self.downcast_unchecked())
    }

    pub(crate) fn downcast_unchecked<W>(self) -> WorkflowHandle<'a, W, M> {
        WorkflowHandle {
            manager: self.manager,
            ids: self.ids,
            host_receiver_channels: self.host_receiver_channels,
            interface: self.interface,
            persisted: self.persisted,
            _ty: PhantomData,
        }
    }
}

impl<W: TakeHandle<Self, Id = ()>, M: AsManager> WorkflowHandle<'_, W, M> {
    /// Returns the ID of this workflow.
    pub fn id(&self) -> WorkflowId {
        self.ids.workflow_id
    }

    /// Returns the current persisted state of the workflow.
    pub fn persisted(&self) -> &PersistedWorkflow {
        &self.persisted
    }

    /// Updates the snapshot contained in this handle.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow was terminated.
    pub async fn update(&mut self) -> Result<(), HandleUpdateError> {
        let manager = self.manager.as_manager();
        let transaction = manager.storage.readonly_transaction().await;
        let record = transaction.workflow(self.ids.workflow_id).await;
        let record = record.ok_or(HandleUpdateError::Terminated)?;
        self.persisted = record.persisted;
        Ok(())
    }

    /// Returns a handle for the workflow that allows interacting with its channels.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn handle(&mut self) -> <W as TakeHandle<Self>>::Handle {
        W::take_handle(self, &()).unwrap()
    }
}

/// Handle for an [inbound workflow channel](Receiver) that allows sending messages
/// via the channel.
#[derive(Debug)]
pub struct MessageSender<'a, T, C, M> {
    manager: &'a M,
    channel_id: ChannelId,
    pub(super) codec: C,
    _item: PhantomData<fn(T)>,
}

impl<T, C: Encode<T>, M: AsManager> MessageSender<'_, T, C, M> {
    /// Returns the ID of the channel this sender is connected to.
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    /// Returns the current state of the channel.
    #[allow(clippy::missing_panics_doc)] // false positive: channels are never removed
    pub async fn channel_info(&self) -> ChannelRecord {
        let manager = self.manager.as_manager();
        manager.channel(self.channel_id).await.unwrap()
    }

    /// Sends a message over the channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel is full or closed.
    pub async fn send(&mut self, message: T) -> Result<(), SendError> {
        let raw_message = self.codec.encode_value(message);
        let manager = self.manager.as_manager();
        manager.send_message(self.channel_id, raw_message).await
    }

    /// Closes this channel from the host side.
    pub async fn close(self) {
        let manager = self.manager.as_manager();
        manager.close_host_sender(self.channel_id).await;
    }
}

impl<'a, T, C, W, M> TakeHandle<WorkflowHandle<'a, W, M>> for Receiver<T, C>
where
    C: Encode<T> + Default,
    M: AsManager,
{
    type Id = str;
    type Handle = MessageSender<'a, T, C, M>;

    fn take_handle(
        env: &mut WorkflowHandle<'a, W, M>,
        id: &str,
    ) -> Result<Self::Handle, AccessError> {
        if let Some(channel_id) = env.ids.channel_ids.inbound.get(id).copied() {
            Ok(MessageSender {
                manager: env.manager,
                channel_id,
                codec: C::default(),
                _item: PhantomData,
            })
        } else {
            Err(AccessErrorKind::Unknown.with_location(InboundChannel(id)))
        }
    }
}

/// Handle for an [outbound workflow channel](Sender) that allows taking messages
/// from the channel.
#[derive(Debug)]
pub struct MessageReceiver<'a, T, C, M> {
    manager: &'a M,
    channel_id: ChannelId,
    pub(super) can_manipulate: bool,
    pub(super) codec: C,
    _item: PhantomData<fn() -> T>,
}

impl<T, C: Decode<T> + Default, M: AsManager> MessageReceiver<'_, T, C, M> {
    /// Returns the ID of the channel this receiver is connected to.
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    /// Returns the current state of the channel.
    #[allow(clippy::missing_panics_doc)] // false positive: channels are never removed
    pub async fn channel_info(&self) -> ChannelRecord {
        let manager = self.manager.as_manager();
        manager.channel(self.channel_id).await.unwrap()
    }

    /// Checks whether this receiver can be used to manipulate the channel (e.g., close it).
    /// This is possible if the channel receiver is not held by a workflow.
    pub fn can_manipulate(&self) -> bool {
        self.can_manipulate
    }

    /// Takes a message with the specified index from the channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the message is not available.
    pub async fn receive_message(
        &mut self,
        index: usize,
    ) -> Result<ReceivedMessage<T, C>, MessageError> {
        let raw_message = in_transaction(self.manager, |transaction| {
            transaction.channel_message(self.channel_id, index)
        })
        .await?;

        Ok(ReceivedMessage {
            index,
            raw_message,
            codec: C::default(),
            _item: PhantomData,
        })
    }

    /// Closes this channel from the host side. If [`Self::can_manipulate()`] returns `false`,
    /// this is a no-op.
    pub async fn close(self) {
        if self.can_manipulate {
            let manager = self.manager.as_manager();
            manager.close_host_receiver(self.channel_id).await;
        }
    }
}

/// Message or end of stream received from an outbound workflow channel.
#[derive(Debug)]
pub struct ReceivedMessage<T, C> {
    index: usize,
    raw_message: Vec<u8>,
    codec: C,
    _item: PhantomData<fn() -> T>,
}

impl<T, C: Decode<T>> ReceivedMessage<T, C> {
    /// Returns zero-based message index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Tries to decode the message. Returns `None` if this is the end of stream marker.
    ///
    /// # Errors
    ///
    /// Returns a decoding error, if any.
    pub fn decode(&mut self) -> Result<T, C::Error> {
        self.codec.try_decode_bytes(self.raw_message.clone())
    }
}

impl<'a, T, C, W, M> TakeHandle<WorkflowHandle<'a, W, M>> for Sender<T, C>
where
    C: Decode<T> + Default,
    M: AsManager,
{
    type Id = str;
    type Handle = MessageReceiver<'a, T, C, M>;

    fn take_handle(
        env: &mut WorkflowHandle<'a, W, M>,
        id: &str,
    ) -> Result<Self::Handle, AccessError> {
        if let Some(channel_id) = env.ids.channel_ids.outbound.get(id).copied() {
            Ok(MessageReceiver {
                manager: env.manager,
                channel_id,
                can_manipulate: env.host_receiver_channels.contains(&channel_id),
                codec: C::default(),
                _item: PhantomData,
            })
        } else {
            Err(AccessErrorKind::Unknown.with_location(OutboundChannel(id)))
        }
    }
}

impl<M: AsManager> TakeHandle<WorkflowHandle<'_, (), M>> for Interface {
    type Id = ();
    type Handle = Self;

    fn take_handle(
        env: &mut WorkflowHandle<'_, (), M>,
        _id: &Self::Id,
    ) -> Result<Self::Handle, AccessError> {
        Ok(env.interface.clone())
    }
}

impl<'a, M: AsManager> TakeHandle<WorkflowHandle<'a, (), M>> for () {
    type Id = ();
    type Handle = UntypedHandle<WorkflowHandle<'a, (), M>>;

    fn take_handle(
        env: &mut WorkflowHandle<'a, (), M>,
        _id: &Self::Id,
    ) -> Result<Self::Handle, AccessError> {
        UntypedHandle::take_handle(env, &())
    }
}
