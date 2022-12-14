//! Handles to workflows.

use std::{fmt, marker::PhantomData};

use super::{ConcurrencyError, HostHandles, MessageReceiver, MessageSender, ReceivedMessage};
use crate::{
    receipt::ExecutionError,
    storage::{
        helper, ActiveWorkflowState, CompletedWorkflowState, ErroneousMessageRef, MessageError,
        ReadChannels, ReadWorkflows, Storage, WorkflowState,
    },
    workflow::WorkflowAndChannelIds,
    PersistedWorkflow,
};
use tardigrade::{
    handle::{AccessError, Handle, HandleMap},
    interface::Interface,
    task::JoinError,
    workflow::{GetInterface, WithHandle},
    Raw, WorkflowId,
};

/// Handle to an active workflow in a [`WorkflowManager`].
///
/// A workflow handle allows [getting handles](Self::handle()) for the channels specified
/// in the workflow interface. The returned handles
/// allow interacting with the workflow by [sending messages](MessageSender)
/// and/or [receiving messages](MessageReceiver).
///
/// [`WorkflowManager`]: crate::manager::WorkflowManager
///
/// # Examples
///
/// ```
/// use tardigrade::handle::{ReceiverAt, SenderAt, WithIndexing};
/// use tardigrade_rt::handle::WorkflowHandle;
/// # use tardigrade_rt::storage::Storage;
///
/// # async fn test_wrapper<S: Storage>(
/// #     workflow: WorkflowHandle<(), &S>,
/// # ) -> anyhow::Result<()> {
/// // Assume we have a dynamically typed workflow:
/// let workflow: WorkflowHandle<(), _> = // ...
/// #   workflow;
/// // We can create a handle to manipulate the workflow.
/// let handle = workflow.handle().await.with_indexing();
///
/// // Let's send a message via a channel.
/// let message = b"hello".to_vec();
/// handle[ReceiverAt("commands")].send(message).await?;
///
/// // Let's then take outbound messages from a certain channel:
/// let message = handle[SenderAt("events")]
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
pub struct WorkflowHandle<W, S> {
    storage: S,
    ids: WorkflowAndChannelIds,
    interface: Interface,
    persisted: PersistedWorkflow,
    _ty: PhantomData<fn(W)>,
}

impl<W, S: fmt::Debug> fmt::Debug for WorkflowHandle<W, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowHandle")
            .field("storage", &self.storage)
            .field("ids", &self.ids)
            .finish()
    }
}

impl<S: Storage> WorkflowHandle<(), S> {
    pub(super) fn new(
        storage: S,
        id: WorkflowId,
        interface: Interface,
        state: ActiveWorkflowState,
    ) -> Self {
        let ids = WorkflowAndChannelIds {
            workflow_id: id,
            channel_ids: state.persisted.channels().to_ids(),
        };

        Self {
            storage,
            ids,
            interface,
            persisted: state.persisted,
            _ty: PhantomData,
        }
    }

    #[cfg(test)]
    pub(crate) fn ids(&self) -> &WorkflowAndChannelIds {
        &self.ids
    }

    /// Attempts to downcast this handle to a specific workflow interface.
    ///
    /// # Errors
    ///
    /// Returns an error on workflow interface mismatch.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn downcast<W: GetInterface>(self) -> Result<WorkflowHandle<W, S>, AccessError> {
        W::interface().check_compatibility(&self.interface)?;
        Ok(self.downcast_unchecked())
    }

    pub(crate) fn downcast_unchecked<W>(self) -> WorkflowHandle<W, S> {
        WorkflowHandle {
            storage: self.storage,
            ids: self.ids,
            interface: self.interface,
            persisted: self.persisted,
            _ty: PhantomData,
        }
    }
}

impl<W: WithHandle, S: Storage> WorkflowHandle<W, S> {
    /// Returns the ID of this workflow.
    pub fn id(&self) -> WorkflowId {
        self.ids.workflow_id
    }

    /// Returns the workflow interface.
    pub fn interface(&self) -> &Interface {
        &self.interface
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
    pub async fn update(&mut self) -> Result<(), ConcurrencyError> {
        let transaction = self.storage.readonly_transaction().await;
        let record = transaction.workflow(self.ids.workflow_id).await;
        let record = record.ok_or_else(ConcurrencyError::new)?;
        self.persisted = match record.state {
            WorkflowState::Active(state) => state.persisted,
            WorkflowState::Completed(_) | WorkflowState::Errored(_) => {
                return Err(ConcurrencyError::new());
            }
        };
        Ok(())
    }

    /// Aborts this workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if abort fails because of a concurrent edit (e.g., the workflow is
    /// already aborted).
    pub async fn abort(self) -> Result<(), ConcurrencyError> {
        helper::abort_workflow(&self.storage, self.ids.workflow_id).await
    }
}

impl<'a, W: WithHandle, S: Storage> WorkflowHandle<W, &'a S> {
    /// Returns a handle for the workflow that allows interacting with its channels.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub async fn handle(&self) -> HostHandles<'a, W, S> {
        let storage = self.storage;
        let transaction = storage.readonly_transaction().await;
        let mut untyped = HandleMap::with_capacity(self.ids.channel_ids.len());

        for (path, &id_handle) in &self.ids.channel_ids {
            let handle = match id_handle {
                Handle::Receiver(id) => {
                    let record = transaction.channel(id).await.unwrap();
                    let sender = MessageSender::new(storage, id, record);
                    Handle::Receiver(sender)
                }
                Handle::Sender(id) => {
                    let record = transaction.channel(id).await.unwrap();
                    let receiver = MessageReceiver::new(storage, id, record);
                    Handle::Sender(receiver)
                }
            };
            untyped.insert(path.clone(), handle);
        }
        W::try_from_untyped(untyped).unwrap()
    }
}

/// Handle for an errored workflow in a [`WorkflowManager`].
///
/// The handle can be used to get information about the error, inspect potentially bogus
/// inbound messages, and repair the workflow. See the [manager docs] for more information
/// about workflow lifecycle.
///
/// [`WorkflowManager`]: crate::manager::WorkflowManager
/// [manager docs]: crate::manager::WorkflowManager#workflow-lifecycle
///
/// # Examples
///
/// ```
/// # use tardigrade_rt::{engine::Wasmtime, handle::StorageRef, storage::LocalStorage};
/// # use tardigrade::WorkflowId;
/// #
/// # fn is_bogus(bytes: &[u8]) -> bool { bytes.is_empty() }
/// #
/// # async fn test_wrapper(
/// #     storage: StorageRef<'_, LocalStorage>,
/// # ) -> anyhow::Result<()> {
/// let storage: StorageRef<'_, LocalStorage> = // ...
/// #   storage;
/// let workflow_id: WorkflowId = // ...
/// #   0;
/// let workflow = storage.any_workflow(workflow_id).await.unwrap();
/// let workflow = workflow.unwrap_errored();
/// // Let's inspect the execution error.
/// let panic_info = workflow.error().panic_info().unwrap();
/// println!("{panic_info}");
///
/// // Let's inspect potentially bogus inbound messages:
/// let mut dropped_messages = false;
/// for message in workflow.messages() {
///     let payload = message.receive().await?.decode()?;
///     if is_bogus(&payload) {
///         message.drop_for_workflow().await?;
///         dropped_messages = true;
///     }
/// }
///
/// if dropped_messages {
///     // Consider that the workflow is repaired:
///     workflow.consider_repaired().await?;
/// } else {
///     // We didn't drop any messages. Assume that we can't do anything
///     // and terminate the workflow.
///     workflow.abort().await?;
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ErroredWorkflowHandle<S> {
    storage: S,
    id: WorkflowId,
    error: ExecutionError,
    erroneous_messages: Vec<ErroneousMessageRef>,
}

impl<S: Storage> ErroredWorkflowHandle<S> {
    pub(super) fn new(
        storage: S,
        id: WorkflowId,
        error: ExecutionError,
        erroneous_messages: Vec<ErroneousMessageRef>,
    ) -> Self {
        Self {
            storage,
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
    ///
    /// # Errors
    ///
    /// Returns an error if abort fails because of a concurrent edit (e.g., the workflow is
    /// already aborted).
    pub async fn abort(self) -> Result<(), ConcurrencyError> {
        helper::abort_workflow(&self.storage, self.id).await
    }

    /// Considers this workflow repaired, usually after [dropping bogus messages] for the workflow
    /// or otherwise eliminating the error cause.
    ///
    /// # Errors
    ///
    /// Returns an error if repair fails because of a concurrent edit (e.g., the workflow is
    /// already repaired or aborted).
    ///
    /// [dropping bogus messages]: ErroneousMessage::drop_for_workflow()
    pub async fn consider_repaired(self) -> Result<(), ConcurrencyError> {
        helper::repair_workflow(&self.storage, self.id).await
    }

    /// Iterates over messages the ingestion of which may have led to the execution error.
    pub fn messages(&self) -> impl Iterator<Item = ErroneousMessage<&S>> + '_ {
        self.erroneous_messages
            .iter()
            .map(|message_ref| ErroneousMessage {
                storage: &self.storage,
                workflow_id: self.id,
                message_ref: message_ref.clone(),
            })
    }
}

/// Handle for a potentially erroneous message consumed by an errored workflow.
///
/// The handle allows inspecting the message and dropping it from the workflow perspective.
#[derive(Debug)]
pub struct ErroneousMessage<S> {
    storage: S,
    workflow_id: WorkflowId,
    message_ref: ErroneousMessageRef,
}

impl<S: Storage> ErroneousMessage<S> {
    /// Receives this message.
    ///
    /// # Errors
    ///
    /// Returns an error if the message is not available.
    pub async fn receive(&self) -> Result<ReceivedMessage<Vec<u8>, Raw>, MessageError> {
        let transaction = self.storage.readonly_transaction().await;
        let raw_message = transaction
            .channel_message(self.message_ref.channel_id, self.message_ref.index)
            .await?;

        Ok(ReceivedMessage::new(self.message_ref.index, raw_message))
    }

    /// Drops the message from the workflow perspective.
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be dropped because of a concurrent edit;
    /// e.g., if the workflow was aborted, repaired, or the message was already dropped.
    pub async fn drop_for_workflow(self) -> Result<(), ConcurrencyError> {
        helper::drop_message(&self.storage, self.workflow_id, &self.message_ref).await
    }
}

/// Handle to a completed workflow in a [`WorkflowManager`].
///
/// There isn't much to do with a completed workflow, other than retrieving
/// the execution [result](Self::result()). See the [manager docs] for more information
/// about workflow lifecycle.
///
/// [`WorkflowManager`]: crate::manager::WorkflowManager
/// [manager docs]: crate::manager::WorkflowManager#workflow-lifecycle
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
pub enum AnyWorkflowHandle<S> {
    /// The workflow is active.
    Active(Box<WorkflowHandle<(), S>>),
    /// The workflow is errored.
    Errored(ErroredWorkflowHandle<S>),
    /// The workflow is completed.
    Completed(CompletedWorkflowHandle),
}

impl<S: Storage> AnyWorkflowHandle<S> {
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
    pub fn unwrap_active(self) -> WorkflowHandle<(), S> {
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
    pub fn unwrap_errored(self) -> ErroredWorkflowHandle<S> {
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

impl<S: Storage> From<WorkflowHandle<(), S>> for AnyWorkflowHandle<S> {
    fn from(handle: WorkflowHandle<(), S>) -> Self {
        Self::Active(Box::new(handle))
    }
}

impl<S: Storage> From<ErroredWorkflowHandle<S>> for AnyWorkflowHandle<S> {
    fn from(handle: ErroredWorkflowHandle<S>) -> Self {
        Self::Errored(handle)
    }
}

impl<S: Storage> From<CompletedWorkflowHandle> for AnyWorkflowHandle<S> {
    fn from(handle: CompletedWorkflowHandle) -> Self {
        Self::Completed(handle)
    }
}
