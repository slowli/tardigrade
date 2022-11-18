//! Handles for workflows in a `WorkflowManager` and their components (e.g., channels).

use futures::future;

use std::{collections::HashSet, error, fmt, marker::PhantomData};

use crate::storage::{CompletedWorkflowState, WorkflowState};
use crate::{
    manager::{AsManager, WorkflowAndChannelIds},
    receipt::ExecutionError,
    storage::{
        ActiveWorkflowState, ChannelRecord, ErroneousMessageRef, MessageError, ReadChannels,
        ReadWorkflows, Storage, StorageTransaction, WriteChannels,
    },
    workflow::ChannelIds,
    PersistedWorkflow,
};
use tardigrade::{
    channel::{Receiver, SendError, Sender},
    interface::{AccessError, AccessErrorKind, InboundChannel, Interface, OutboundChannel},
    task::JoinError,
    workflow::{GetInterface, TakeHandle, UntypedHandle},
    ChannelId, Decode, Encode, Raw, WorkflowId,
};

/// Error updating a [`WorkflowHandle`].
#[derive(Debug)]
#[non_exhaustive]
pub enum HandleUpdateError {
    /// The workflow that the handle is associated with has completed.
    Completed,
    /// The workflow that the handle is associated with has errored.
    Errored,
}

impl fmt::Display for HandleUpdateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed => {
                formatter.write_str("workflow that the handle is associated with has completed")
            }
            Self::Errored => {
                formatter.write_str("workflow that the handle is associated with has errored")
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
/// See [`Driver`] for a more high-level alternative.
///
/// [`WorkflowManager`]: crate::manager::WorkflowManager
/// [`Driver`]: crate::driver::Driver
///
/// # Examples
///
/// ```
/// use tardigrade::interface::{InboundChannel, OutboundChannel};
/// use tardigrade_rt::manager::WorkflowHandle;
/// # use tardigrade_rt::manager::AsManager;
///
/// # async fn test_wrapper<M: AsManager>(
/// #     workflow: WorkflowHandle<'_, (), M>,
/// # ) -> anyhow::Result<()> {
/// // Assume we have a dynamically typed workflow:
/// let mut workflow: WorkflowHandle<(), _> = // ...
/// #   workflow;
/// // We can create a handle to manipulate the workflow.
/// let mut handle = workflow.handle();
///
/// // Let's send a message via an inbound channel.
/// let message = b"hello".to_vec();
/// handle[InboundChannel("commands")].send(message).await?;
///
/// // Let's then take outbound messages from a certain channel:
/// let message = handle[OutboundChannel("events")]
///     .receive_message(0)
///     .await?;
/// let message: Vec<u8> = message.decode()?;
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
            .debug_struct("WorkflowHandle")
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
        id: WorkflowId,
        interface: &'a Interface,
        state: ActiveWorkflowState,
    ) -> WorkflowHandle<'a, (), M> {
        let ids = WorkflowAndChannelIds {
            workflow_id: id,
            channel_ids: state.persisted.channel_ids(),
        };
        let host_receiver_channels =
            Self::find_host_receiver_channels(transaction, &ids.channel_ids).await;

        Self {
            manager,
            ids,
            host_receiver_channels,
            interface,
            persisted: state.persisted,
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
            Some(id).filter(|_| channel.receiver_workflow_id.is_none())
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

    /// Returns the snapshot of the persisted state of this workflow.
    ///
    /// The snapshot is taken when the handle is created and can be updated
    /// using [`Self::update()`].
    pub fn persisted(&self) -> &PersistedWorkflow {
        &self.persisted
    }

    /// Updates the [snapshot](Self::persisted()) contained in this handle.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow was terminated.
    pub async fn update(&mut self) -> Result<(), HandleUpdateError> {
        let manager = self.manager.as_manager();
        let transaction = manager.storage.readonly_transaction().await;
        let record = transaction.workflow(self.ids.workflow_id).await;
        let record = record.ok_or(HandleUpdateError::Completed)?;
        self.persisted = match record.state {
            WorkflowState::Active(state) => state.persisted,
            WorkflowState::Completed(_) => return Err(HandleUpdateError::Completed),
            WorkflowState::Errored(_) => return Err(HandleUpdateError::Errored),
        };
        Ok(())
    }

    /// Aborts this workflow.
    pub async fn abort(self) {
        self.manager
            .as_manager()
            .abort_workflow(self.ids.workflow_id)
            .await;
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
    pub(crate) codec: C,
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
    can_manipulate: bool,
    pub(crate) codec: C,
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
        &self,
        index: usize,
    ) -> Result<ReceivedMessage<T, C>, MessageError> {
        let manager = self.manager.as_manager();
        let transaction = manager.storage.readonly_transaction().await;
        let raw_message = transaction.channel_message(self.channel_id, index).await?;

        Ok(ReceivedMessage {
            index,
            raw_message,
            codec: C::default(),
            _item: PhantomData,
        })
    }

    /// Truncates this channel so that its minimum message index is no less than `min_index`.
    /// If [`Self::can_manipulate()`] returns `false`, this is a no-op.
    pub async fn truncate(&self, min_index: usize) {
        if self.can_manipulate {
            let manager = self.manager.as_manager();
            let mut transaction = manager.storage.transaction().await;
            transaction
                .truncate_channel(self.channel_id, min_index)
                .await;
            transaction.commit().await;
        }
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
    pub fn decode(mut self) -> Result<T, C::Error> {
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

/// Handle for an errored workflow.
#[derive(Debug)]
pub struct ErroredWorkflowHandle<'a, M> {
    manager: &'a M,
    id: WorkflowId,
    pub(super) error: ExecutionError,
    erroneous_messages: Vec<ErroneousMessageRef>,
}

impl<'a, M: AsManager> ErroredWorkflowHandle<'a, M> {
    pub(super) fn new(
        manager: &'a M,
        id: WorkflowId,
        error: ExecutionError,
        erroneous_messages: Vec<ErroneousMessageRef>,
    ) -> Self {
        Self {
            manager,
            id,
            error,
            erroneous_messages,
        }
    }

    /// Returns the ID of this workflow.
    pub fn id(&self) -> WorkflowId {
        self.id
    }

    /// Returns the workflow execution error.
    pub fn error(&self) -> &ExecutionError {
        &self.error
    }

    /// Aborts this workflow.
    pub async fn abort(self) {
        self.manager.as_manager().abort_workflow(self.id).await;
    }

    /// Iterates over messages the ingestion of which may have led to the execution error.
    pub fn messages(&self) -> impl Iterator<Item = ErroneousMessage<'a, M>> + '_ {
        self.erroneous_messages
            .iter()
            .map(|message_ref| ErroneousMessage {
                manager: self.manager,
                workflow_id: self.id,
                message_ref: message_ref.clone(),
            })
    }
}

/// Handle for a potentially erroneous message consumed by an errored workflow.
///
/// The handle allows inspecting the message and dropping it from the workflow perspective.
#[derive(Debug)]
pub struct ErroneousMessage<'a, M> {
    manager: &'a M,
    workflow_id: WorkflowId,
    message_ref: ErroneousMessageRef,
}

impl<M: AsManager> ErroneousMessage<'_, M> {
    /// Receives this message.
    pub async fn receive(&self) -> Result<ReceivedMessage<Vec<u8>, Raw>, MessageError> {
        let manager = self.manager.as_manager();
        let transaction = manager.storage.readonly_transaction().await;
        let raw_message = transaction
            .channel_message(self.message_ref.channel_id, self.message_ref.index)
            .await?;

        Ok(ReceivedMessage {
            index: self.message_ref.index,
            raw_message,
            codec: Raw,
            _item: PhantomData,
        })
    }

    /// Drops the message from the workflow perspective.
    pub async fn drop_for_workflow(self) {
        self.manager
            .as_manager()
            .drop_message(self.workflow_id, &self.message_ref)
            .await;
    }
}

/// Handle to a completed workflow.
#[derive(Debug)]
pub struct CompletedWorkflowHandle {
    id: WorkflowId,
    result: Result<(), JoinError>,
}

impl CompletedWorkflowHandle {
    pub(super) fn new(id: WorkflowId, state: CompletedWorkflowState) -> Self {
        Self {
            id,
            result: state.result,
        }
    }

    /// Returns the ID of this workflow.
    pub fn id(&self) -> WorkflowId {
        self.id
    }

    /// Returns the execution result of the workflow.
    #[allow(clippy::missing_errors_doc)] // doesn't make sense semantically
    pub fn result(&self) -> Result<(), &JoinError> {
        self.result.as_ref().copied()
    }
}

/// Handle to a workflow in an unknown state.
#[derive(Debug)]
#[non_exhaustive]
pub enum AnyWorkflowHandle<'a, M> {
    /// The workflow is active.
    Active(Box<WorkflowHandle<'a, (), M>>),
    /// The workflow is errored.
    Errored(ErroredWorkflowHandle<'a, M>),
    /// The workflow is completed.
    Completed(CompletedWorkflowHandle),
}

impl<'a, M: AsManager> AnyWorkflowHandle<'a, M> {
    /// Returns the ID of this workflow.
    pub fn id(&self) -> WorkflowId {
        match self {
            Self::Active(handle) => handle.id(),
            Self::Errored(handle) => handle.id(),
            Self::Completed(handle) => handle.id(),
        }
    }

    /// Checks whether this workflow is active.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active(_))
    }

    /// Checks whether this workflow is errored.
    pub fn is_errored(&self) -> bool {
        matches!(self, Self::Errored(_))
    }

    /// Checks whether this workflow is completed.
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed(_))
    }

    /// Unwraps the handle to the active workflow.
    ///
    /// # Panics
    ///
    /// Panics if the workflow is not active.
    pub fn unwrap_active(self) -> WorkflowHandle<'a, (), M> {
        match self {
            Self::Active(handle) => *handle,
            _ => panic!("workflow is not active"),
        }
    }

    /// Unwraps the handle to the errored workflow.
    ///
    /// # Panics
    ///
    /// Panics if the workflow is not errored.
    pub fn unwrap_errored(self) -> ErroredWorkflowHandle<'a, M> {
        match self {
            Self::Errored(handle) => handle,
            _ => panic!("workflow is not errored"),
        }
    }

    /// Unwraps the handle to the completed workflow.
    ///
    /// # Panics
    ///
    /// Panics if the workflow is not completed.
    pub fn unwrap_completed(self) -> CompletedWorkflowHandle {
        match self {
            Self::Completed(handle) => handle,
            _ => panic!("workflow is not completed"),
        }
    }
}

impl<'a, M: AsManager> From<WorkflowHandle<'a, (), M>> for AnyWorkflowHandle<'a, M> {
    fn from(handle: WorkflowHandle<'a, (), M>) -> Self {
        Self::Active(Box::new(handle))
    }
}

impl<'a, M: AsManager> From<ErroredWorkflowHandle<'a, M>> for AnyWorkflowHandle<'a, M> {
    fn from(handle: ErroredWorkflowHandle<'a, M>) -> Self {
        Self::Errored(handle)
    }
}

impl<'a, M: AsManager> From<CompletedWorkflowHandle> for AnyWorkflowHandle<'a, M> {
    fn from(handle: CompletedWorkflowHandle) -> Self {
        Self::Completed(handle)
    }
}
