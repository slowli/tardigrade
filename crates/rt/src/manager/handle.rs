//! Handles for workflows in a [`WorkflowManager`] and their components (e.g., channels).

use futures::future;

use std::{collections::HashSet, error, fmt, marker::PhantomData};

use crate::{
    manager::{WorkflowAndChannelIds, WorkflowManager},
    storage::{ChannelRecord, ReadChannels, ReadWorkflows, Storage, WorkflowRecord},
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
pub struct WorkflowHandle<'a, W, S> {
    manager: &'a WorkflowManager<S>,
    ids: WorkflowAndChannelIds,
    valid_receiver_ids: HashSet<ChannelId>,
    interface: &'a Interface,
    persisted: PersistedWorkflow,
    _ty: PhantomData<fn(W)>,
}

impl<W, S> fmt::Debug for WorkflowHandle<'_, W, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowEnv")
            .field("manager", &self.manager)
            .field("ids", &self.ids)
            .finish()
    }
}

#[allow(clippy::mismatching_type_param_order)] // false positive
impl<'a, S: Storage<'a>> WorkflowHandle<'a, (), S> {
    pub(super) async fn new(
        manager: &'a WorkflowManager<S>,
        transaction: &S::ReadonlyTransaction,
        interface: &'a Interface,
        record: WorkflowRecord,
    ) -> WorkflowHandle<'a, (), S> {
        let ids = WorkflowAndChannelIds {
            workflow_id: record.id,
            channel_ids: record.persisted.channel_ids(),
        };
        let valid_receiver_ids = Self::find_valid_receiver_ids(transaction, &ids.channel_ids).await;

        Self {
            manager,
            ids,
            valid_receiver_ids,
            interface,
            persisted: record.persisted,
            _ty: PhantomData,
        }
    }

    async fn find_valid_receiver_ids(
        transaction: &S::ReadonlyTransaction,
        channel_ids: &ChannelIds,
    ) -> HashSet<ChannelId> {
        let outbound_channel_ids = channel_ids.outbound.values();
        let channel_tasks = outbound_channel_ids.map(|&id| async move {
            let channel = transaction.channel(id).await.unwrap();
            Some(id).filter(|_| channel.state.receiver_workflow_id.is_none())
        });
        let receivable_channels = future::join_all(channel_tasks).await;
        receivable_channels.into_iter().flatten().collect()
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
    pub fn downcast<W: GetInterface>(self) -> Result<WorkflowHandle<'a, W, S>, AccessError> {
        W::interface().check_compatibility(self.interface)?;
        Ok(self.downcast_unchecked())
    }

    pub(crate) fn downcast_unchecked<W>(self) -> WorkflowHandle<'a, W, S> {
        WorkflowHandle {
            manager: self.manager,
            ids: self.ids,
            valid_receiver_ids: self.valid_receiver_ids,
            interface: self.interface,
            persisted: self.persisted,
            _ty: PhantomData,
        }
    }
}

impl<'a, W: TakeHandle<Self, Id = ()>, S: Storage<'a>> WorkflowHandle<'a, W, S> {
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
        let transaction = self.manager.storage.readonly_transaction().await;
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
pub struct MessageSender<'a, T, C, S> {
    manager: &'a WorkflowManager<S>,
    channel_id: ChannelId,
    pub(super) codec: C,
    _item: PhantomData<fn(T)>,
}

impl<'a, T, C: Encode<T>, S: Storage<'a>> MessageSender<'a, T, C, S> {
    /// Returns the ID of the channel this sender is connected to.
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    /// Returns the current state of the channel.
    #[allow(clippy::missing_panics_doc)] // false positive: channels are never removed
    pub async fn channel_info(&self) -> ChannelRecord {
        self.manager.channel(self.channel_id).await.unwrap()
    }

    /// Sends a message over the channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel is full or closed.
    pub async fn send(&mut self, message: T) -> Result<(), SendError> {
        let raw_message = self.codec.encode_value(message);
        self.manager
            .send_message(self.channel_id, raw_message)
            .await
    }

    /// Closes this channel from the host side.
    pub async fn close(self) {
        self.manager.close_host_sender(self.channel_id).await;
    }
}

impl<'a, T, C, W, S> TakeHandle<WorkflowHandle<'a, W, S>> for Receiver<T, C>
where
    C: Encode<T> + Default,
    S: Storage<'a>,
{
    type Id = str;
    type Handle = MessageSender<'a, T, C, S>;

    fn take_handle(
        env: &mut WorkflowHandle<'a, W, S>,
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
pub struct MessageReceiver<'a, T, C, S> {
    manager: &'a WorkflowManager<S>,
    channel_id: ChannelId,
    pub(super) can_receive_messages: bool,
    pub(super) codec: C,
    _item: PhantomData<fn() -> T>,
}

impl<'a, T, C: Decode<T>, S: Storage<'a>> MessageReceiver<'a, T, C, S> {
    /// Returns the ID of the channel this receiver is connected to.
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    /// Returns the current state of the channel.
    #[allow(clippy::missing_panics_doc)] // false positive: channels are never removed
    pub async fn channel_info(&self) -> ChannelRecord {
        self.manager.channel(self.channel_id).await.unwrap()
    }

    /// Checks whether this receiver can be used to receive messages from the channel.
    /// This is possible if the channel receiver is not held by a workflow.
    pub fn can_receive_messages(&self) -> bool {
        self.can_receive_messages
    }

    /// Takes messages from the channel, or `None` if messages are not received by host
    /// (i.e., [`Self::can_receive_messages()`] returns `false`).
    pub async fn take_messages(&mut self) -> Option<TakenMessages<'_, T, C>> {
        if !self.can_receive_messages {
            return None;
        }

        Some(TakenMessages {
            raw_messages: self.manager.take_outbound_messages(self.channel_id).await,
            codec: &mut self.codec,
            _item: PhantomData,
        })
    }

    /// Closes this channel from the host side. If [`Self::can_receive_messages()`] returns `false`,
    /// this is a no-op.
    pub async fn close(self) {
        if self.can_receive_messages {
            self.manager.close_host_receiver(self.channel_id).await;
        }
    }
}

/// Result of taking messages from an outbound workflow channel.
#[derive(Debug)]
pub struct TakenMessages<'a, T, C> {
    raw_messages: Vec<Vec<u8>>,
    codec: &'a mut C,
    _item: PhantomData<fn() -> T>,
}

impl<T, C: Decode<T>> TakenMessages<'_, T, C> {
    /// Tries to decode the taken messages.
    ///
    /// # Errors
    ///
    /// Returns a decoding error, if any.
    pub fn decode(self) -> Result<Vec<T>, C::Error> {
        self.raw_messages
            .into_iter()
            .map(|bytes| self.codec.try_decode_bytes(bytes))
            .collect()
    }
}

impl<'a, T, C, W, S> TakeHandle<WorkflowHandle<'a, W, S>> for Sender<T, C>
where
    C: Decode<T> + Default,
    S: Storage<'a>,
{
    type Id = str;
    type Handle = MessageReceiver<'a, T, C, S>;

    fn take_handle(
        env: &mut WorkflowHandle<'a, W, S>,
        id: &str,
    ) -> Result<Self::Handle, AccessError> {
        if let Some(channel_id) = env.ids.channel_ids.outbound.get(id).copied() {
            Ok(MessageReceiver {
                manager: env.manager,
                channel_id,
                can_receive_messages: env.valid_receiver_ids.contains(&channel_id),
                codec: C::default(),
                _item: PhantomData,
            })
        } else {
            Err(AccessErrorKind::Unknown.with_location(OutboundChannel(id)))
        }
    }
}

impl<'a, S: Storage<'a>> TakeHandle<WorkflowHandle<'a, (), S>> for Interface {
    type Id = ();
    type Handle = Self;

    fn take_handle(
        env: &mut WorkflowHandle<'a, (), S>,
        _id: &Self::Id,
    ) -> Result<Self::Handle, AccessError> {
        Ok(env.interface.clone())
    }
}

impl<'a, S: Storage<'a>> TakeHandle<WorkflowHandle<'a, (), S>> for () {
    type Id = ();
    type Handle = UntypedHandle<WorkflowHandle<'a, (), S>>;

    fn take_handle(
        env: &mut WorkflowHandle<'a, (), S>,
        _id: &Self::Id,
    ) -> Result<Self::Handle, AccessError> {
        UntypedHandle::take_handle(env, &())
    }
}
