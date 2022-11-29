//! Async, transactional storage abstraction for storing workflows and channel state.
//!
//! # Overview
//!
//! The core trait of this module is [`Storage`], which produces storage transactions:
//!
//! - [`ReadonlyStorageTransaction`] for reading data
//! - [`StorageTransaction`] for reading and writing data
//!
//! Transactions must have at least snapshot isolation.
//!
//! The data operated by transactions is as follows:
//!
//! | Entity type | Read trait | Write trait |
//! |-------------|------------|-------------|
//! | [`ModuleRecord`] | [`ReadModules`] | [`WriteModules`] |
//! | [`ChannelRecord`] | [`ReadChannels`] | [`WriteChannels`] |
//! | [`WorkflowRecord`] | [`ReadWorkflows`] | [`WriteWorkflows`] |
//! | [`WorkflowWakerRecord`] | n/a | [`WriteWorkflowWakers`] |
//!
//! The `ReadonlyStorageTransaction` trait encompasses all read traits, and `StorageTransaction`
//! encompasses both read and write traits. To simplify readonly transaction implementation without
//! sacrificing type safety, there is [`Readonly`] wrapper.
//!
//! In terms of domain-driven development, modules, channels and workflows are independent
//! aggregate roots, while workflow wakers are tied to a workflow.
//! `_Record` types do not precisely correspond to the relational data model, but can be
//! straightforwardly mapped onto one. In particular, [`WorkflowRecord`] has the workflow state
//! as a type param. The state is modelled via enum dispatch on the [`WorkflowState`] enum and
//! its variants (e.g., [`ActiveWorkflowState`] for active workflows).
//!
//! Besides the [`Storage`] trait, the module provides its local in-memory implementation:
//! [`LocalStorage`]. It has [`LocalTransaction`] transactions and provides
//! [`LocalStorageSnapshot`] as a way to get (de)serializable snapshot of the storage.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tracing_tunnel::{PersistedMetadata, PersistedSpans};

use std::{collections::HashSet, error, fmt, ops, sync::Arc};

mod local;

pub use self::local::{LocalStorage, LocalStorageSnapshot, LocalTransaction, ModuleRecordMut};

use crate::{
    receipt::ExecutionError,
    utils::{clone_join_error, serde_b64},
    PersistedWorkflow,
};
use tardigrade::{task::JoinError, ChannelId, WakerId, WorkflowId};

/// Async, transactional storage for workflows and workflow channels.
///
/// A storage is required to instantiate a [`WorkflowManager`](crate::manager::WorkflowManager).
#[async_trait]
pub trait Storage: 'static + Send + Sync {
    /// Read/write transaction for the storage. See [`StorageTransaction`] for required
    /// transaction semantics.
    type Transaction<'a>: 'a + StorageTransaction;
    /// Readonly transaction for the storage.
    type ReadonlyTransaction<'a>: 'a + ReadonlyStorageTransaction;

    /// Creates a new read/write transaction.
    async fn transaction(&self) -> Self::Transaction<'_>;
    /// Creates a new readonly transaction.
    async fn readonly_transaction(&self) -> Self::ReadonlyTransaction<'_>;
}

/// [`Storage`] transaction with readonly access to the storage.
pub trait ReadonlyStorageTransaction:
    Send + Sync + ReadModules + ReadChannels + ReadWorkflows
{
}

impl<T> ReadonlyStorageTransaction for T where
    T: Send + Sync + ReadModules + ReadChannels + ReadWorkflows
{
}

/// [`Storage`] transaction with read/write access to the storage.
///
/// Transactions must satisfy the ACID semantics. In particular, they must apply atomically
/// and be isolated (i.e., not visible to other transactions until committed).
///
/// [`Self::commit()`] must be called for before the transaction is dropped.
/// Explicit transaction rollback support is not required; all transactions instantiated
/// by a [`WorkflowManager`] are guaranteed to eventually be committed (save for corner cases,
/// e.g., panicking when the transaction is active).
/// If rollback *is* supported by the storage, it is assumed to be the default behavior
/// on transaction drop. It would be an error to commit transactions on drop,
/// since this would produce errors in the aforementioned corner cases.
///
/// [`WorkflowManager`]: crate::manager::WorkflowManager
#[must_use = "transactions must be committed to take effect"]
#[async_trait]
pub trait StorageTransaction:
    Send + Sync + WriteModules + WriteChannels + WriteWorkflows + WriteWorkflowWakers
{
    /// Commits this transaction to the storage. This method must be called
    /// to (atomically) apply transaction changes.
    async fn commit(self);
}

/// Allows reading stored information about [`WorkflowModule`]s.
///
/// [`WorkflowModule`]: crate::engine::WorkflowModule
#[async_trait]
pub trait ReadModules {
    /// Retrieves a module with the specified ID.
    async fn module(&self, id: &str) -> Option<ModuleRecord>;

    /// Streams all modules.
    fn modules(&self) -> BoxStream<'_, ModuleRecord>;
}

/// Allows modifying stored information about [`WorkflowModule`]s.
///
/// [`WorkflowModule`]: crate::engine::WorkflowModule
#[async_trait]
pub trait WriteModules: ReadModules {
    /// Inserts the module into the storage.
    async fn insert_module(&mut self, module: ModuleRecord);

    /// Updates tracing metadata for the module.
    ///
    /// It is acceptable to merge the provided metadata JSON to the current one to prevent edit
    /// conflicts.
    async fn update_tracing_metadata(&mut self, module_id: &str, metadata: PersistedMetadata);
}

/// Storage record for a [`WorkflowModule`].
///
/// [`WorkflowModule`]: crate::engine::WorkflowModule
#[derive(Clone, Serialize, Deserialize)]
pub struct ModuleRecord {
    /// ID of the module. This should be the primary key of the module.
    pub id: String,
    /// WASM module bytes.
    #[serde(with = "serde_b64")]
    pub bytes: Arc<[u8]>,
    /// Persisted metadata.
    pub tracing_metadata: PersistedMetadata,
}

impl fmt::Debug for ModuleRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModuleRecord")
            .field("id", &self.id)
            .field("bytes_len", &self.bytes.len())
            .field("tracing_metadata", &self.tracing_metadata)
            .finish()
    }
}

/// Allows reading stored information about workflow channels.
#[async_trait]
pub trait ReadChannels {
    /// Retrieves information about the channel with the specified ID.
    async fn channel(&self, id: ChannelId) -> Option<ChannelRecord>;

    /// Gets a message with the specified 0-based index from the specified channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be retrieved.
    async fn channel_message(&self, id: ChannelId, index: usize) -> Result<Vec<u8>, MessageError>;

    /// Gets messages with indices in the specified range from the specified channel.
    fn channel_messages(
        &self,
        id: ChannelId,
        indices: ops::RangeInclusive<usize>,
    ) -> BoxStream<'_, (usize, Vec<u8>)>;
}

/// Allows modifying stored information about channels.
#[async_trait]
pub trait WriteChannels: ReadChannels {
    /// Allocates a new unique ID for a channel.
    async fn allocate_channel_id(&mut self) -> ChannelId;

    /// Creates a new channel with the provided `state`. If the channel with the specified ID
    /// already exists, does nothing (i.e., the channel state is not updated in any way).
    async fn get_or_insert_channel(&mut self, id: ChannelId, state: ChannelRecord)
        -> ChannelRecord;

    /// Changes the channel state and selects the updated state.
    async fn manipulate_channel<F: FnOnce(&mut ChannelRecord) + Send>(
        &mut self,
        id: ChannelId,
        action: F,
    ) -> ChannelRecord;

    /// Pushes one or more messages into the channel.
    async fn push_messages(&mut self, id: ChannelId, messages: Vec<Vec<u8>>);

    /// Truncates the channel so that `min_index` is the minimum retained index.
    async fn truncate_channel(&mut self, id: ChannelId, min_index: usize);
}

/// Error retrieving a message from a workflow channel. Returned by
/// [`ReadChannels::channel_message()`].
#[derive(Debug)]
#[non_exhaustive]
pub enum MessageError {
    /// A channel with the specified channel ID does not exist.
    UnknownChannelId,
    /// Requested index with an index larger than the maximum stored index.
    NonExistingIndex {
        /// Is the channel closed?
        is_closed: bool,
    },
    /// Requested index with an index lesser than the minimum stored index.
    Truncated,
}

impl fmt::Display for MessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownChannelId => formatter.write_str("unknown channel ID"),
            Self::NonExistingIndex { is_closed } => {
                formatter.write_str("non-existing message index")?;
                if *is_closed {
                    formatter.write_str(" for closed channel")?;
                }
                Ok(())
            }
            Self::Truncated => formatter.write_str("message was truncated"),
        }
    }
}

impl error::Error for MessageError {}

/// State of a workflow channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelRecord {
    /// ID of the receiver workflow, or `None` if the receiver is external.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receiver_workflow_id: Option<WorkflowId>,
    /// IDs of sender workflows.
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub sender_workflow_ids: HashSet<WorkflowId>,
    /// `true` if the channel has an external sender.
    #[serde(default, skip_serializing_if = "is_false")]
    pub has_external_sender: bool,
    /// `true` if the channel is closed (i.e., no more messages can be written to it).
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_closed: bool,
    /// Number of messages written to the channel.
    pub received_messages: usize,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // required by serde
fn is_false(&flag: &bool) -> bool {
    !flag
}

/// Allows reading information about workflows.
#[async_trait]
pub trait ReadWorkflows {
    /// Returns the number of active workflows.
    async fn count_active_workflows(&self) -> usize;
    /// Retrieves a snapshot of the workflow with the specified ID.
    async fn workflow(&self, id: WorkflowId) -> Option<WorkflowRecord>;

    /// Finds the nearest timer expiration in all active workflows.
    async fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>>;
}

/// Allows modifying stored information about workflows.
#[async_trait]
pub trait WriteWorkflows: ReadWorkflows {
    /// Allocates a new unique ID for a workflow.
    async fn allocate_workflow_id(&mut self) -> WorkflowId;
    /// Inserts a new workflow into the storage.
    async fn insert_workflow(&mut self, state: WorkflowRecord);
    /// Returns a workflow record for the specified ID for update.
    async fn workflow_for_update(&mut self, id: WorkflowId) -> Option<WorkflowRecord>;

    /// Updates the state of a workflow with the specified ID.
    // TODO: concurrency edit protection (via execution counter?)
    async fn update_workflow(&mut self, id: WorkflowId, state: WorkflowState);

    /// Finds an active workflow with wakers and selects it for update.
    async fn workflow_with_wakers_for_update(
        &mut self,
    ) -> Option<WorkflowRecord<ActiveWorkflowState>>;

    /// Finds an active workflow with an inbound channel that has a message or EOF marker
    /// consumable by the workflow, and selects the workflow for update.
    async fn workflow_with_consumable_channel_for_update(
        &self,
    ) -> Option<WorkflowRecord<ActiveWorkflowState>>;
}

/// State of a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRecord<T = WorkflowState> {
    /// ID of the workflow.
    pub id: WorkflowId,
    /// ID of the parent workflow, or `None` if this is a root workflow.
    pub parent_id: Option<WorkflowId>,
    /// ID of the module in which the workflow is defined.
    pub module_id: String,
    /// Name of the workflow in the module.
    pub name_in_module: String,
    /// Current state of the workflow.
    pub state: T,
}

impl WorkflowRecord {
    pub(crate) fn into_active(self) -> Option<WorkflowRecord<ActiveWorkflowState>> {
        match self.state {
            WorkflowState::Active(state) => Some(WorkflowRecord {
                id: self.id,
                parent_id: self.parent_id,
                module_id: self.module_id,
                name_in_module: self.name_in_module,
                state: *state,
            }),
            WorkflowState::Completed(_) | WorkflowState::Errored(_) => None,
        }
    }

    pub(crate) fn into_errored(self) -> Option<WorkflowRecord<ErroredWorkflowState>> {
        match self.state {
            WorkflowState::Errored(state) => Some(WorkflowRecord {
                id: self.id,
                parent_id: self.parent_id,
                module_id: self.module_id,
                name_in_module: self.name_in_module,
                state,
            }),
            WorkflowState::Completed(_) | WorkflowState::Active(_) => None,
        }
    }
}

/// State of a [`WorkflowRecord`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    /// Workflow is currently active.
    Active(Box<ActiveWorkflowState>),
    /// Workflow has errored.
    Errored(ErroredWorkflowState),
    /// Workflow is completed.
    Completed(CompletedWorkflowState),
}

impl WorkflowState {
    pub(crate) fn into_result(self) -> Option<Result<(), JoinError>> {
        match self {
            Self::Completed(CompletedWorkflowState { result }) => Some(result),
            Self::Active(_) | Self::Errored(_) => None,
        }
    }
}

impl From<ActiveWorkflowState> for WorkflowState {
    fn from(state: ActiveWorkflowState) -> Self {
        Self::Active(Box::new(state))
    }
}

impl From<ErroredWorkflowState> for WorkflowState {
    fn from(state: ErroredWorkflowState) -> Self {
        Self::Errored(state)
    }
}

impl From<CompletedWorkflowState> for WorkflowState {
    fn from(state: CompletedWorkflowState) -> Self {
        Self::Completed(state)
    }
}

/// State of an active workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveWorkflowState {
    /// Persisted workflow state.
    pub persisted: PersistedWorkflow,
    /// Tracing spans associated with the workflow.
    pub tracing_spans: PersistedSpans,
}

impl ActiveWorkflowState {
    pub(crate) fn with_error(
        self,
        error: ExecutionError,
        erroneous_messages: Vec<ErroneousMessageRef>,
    ) -> ErroredWorkflowState {
        ErroredWorkflowState {
            persisted: self.persisted,
            tracing_spans: self.tracing_spans,
            error,
            erroneous_messages,
        }
    }
}

/// State of a completed workflow.
#[derive(Debug, Serialize, Deserialize)]
pub struct CompletedWorkflowState {
    /// Workflow completion result.
    pub result: Result<(), JoinError>,
}

impl Clone for CompletedWorkflowState {
    fn clone(&self) -> Self {
        Self {
            result: self.result.as_ref().copied().map_err(clone_join_error),
        }
    }
}

/// State of an errored workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErroredWorkflowState {
    /// Persisted workflow state.
    pub persisted: PersistedWorkflow,
    /// Tracing spans associated with the workflow.
    pub tracing_spans: PersistedSpans,
    /// Workflow execution error.
    pub error: ExecutionError,
    /// Messages the ingestion of which may have led to the execution error.
    pub erroneous_messages: Vec<ErroneousMessageRef>,
}

impl ErroredWorkflowState {
    pub(crate) fn repair(self) -> ActiveWorkflowState {
        ActiveWorkflowState {
            persisted: self.persisted,
            tracing_spans: self.tracing_spans,
        }
    }
}

/// Reference for a potentially erroneous message in [`ErroredWorkflowState`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErroneousMessageRef {
    /// ID of the channel the message was received from.
    pub channel_id: ChannelId,
    /// 0-based index of the message in the channel.
    pub index: usize,
}

/// Workflow selection criteria used in [`insert_waker_for_matching_workflows()`].
///
/// [`insert_waker_for_matching_workflows()`]: WriteWorkflowWakers::insert_waker_for_matching_workflows()
#[derive(Debug)]
#[non_exhaustive]
pub enum WorkflowSelectionCriteria {
    /// Workflow has an active timer before the specified timestamp.
    HasTimerBefore(DateTime<Utc>),
}

impl WorkflowSelectionCriteria {
    fn matches(&self, workflow: &PersistedWorkflow) -> bool {
        match self {
            Self::HasTimerBefore(time) => workflow
                .timers()
                .any(|(_, timer)| timer.definition().expires_at <= *time),
        }
    }
}

/// Waker for a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkflowWaker {
    /// Waker produced by the workflow execution, e.g., flushing outbound messages
    /// or initializing a child workflow.
    Internal,
    /// Waker produced by a timer.
    Timer(DateTime<Utc>),
    /// Waker produced by a sender closure.
    SenderClosure(ChannelId),
    /// Waker produced by a completed child workflow.
    ChildCompletion(WorkflowId),
}

/// Record associating a [`WorkflowWaker`] with a particular workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowWakerRecord {
    /// ID of the workflow that the waker belongs to.
    pub workflow_id: WorkflowId,
    /// ID of the waker. Must be unique at least within the workflow.
    pub waker_id: WakerId,
    /// The waker information.
    pub waker: WorkflowWaker,
}

/// Allows modifying stored information about [`WorkflowWaker`].
#[async_trait]
pub trait WriteWorkflowWakers {
    /// Inserts a workflow waker into the queue.
    async fn insert_waker(&mut self, workflow_id: WorkflowId, waker: WorkflowWaker);

    /// Inserts a workflow waker for all non-completed workflows matching the specified criteria.
    async fn insert_waker_for_matching_workflows(
        &mut self,
        criteria: WorkflowSelectionCriteria,
        waker: WorkflowWaker,
    );

    /// Selects all wakers for the specified workflow.
    async fn wakers_for_workflow(&self, workflow_id: WorkflowId) -> Vec<WorkflowWakerRecord>;

    /// Deletes wakers with the specified IDs for the given workflow ID.
    async fn delete_wakers(&mut self, workflow_id: WorkflowId, waker_ids: &[WakerId]);
}

/// Wrapper for [`StorageTransaction`] implementations that allows safely using them
/// as readonly transactions.
#[derive(Debug)]
pub struct Readonly<T>(T);

impl<T: ReadonlyStorageTransaction> From<T> for Readonly<T> {
    fn from(inner: T) -> Self {
        Self(inner)
    }
}

impl<T: ReadonlyStorageTransaction> AsRef<T> for Readonly<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

#[async_trait]
impl<T: ReadonlyStorageTransaction> ReadModules for Readonly<T> {
    async fn module(&self, id: &str) -> Option<ModuleRecord> {
        self.0.module(id).await
    }

    fn modules(&self) -> BoxStream<'_, ModuleRecord> {
        self.0.modules()
    }
}

#[async_trait]
impl<T: ReadonlyStorageTransaction> ReadChannels for Readonly<T> {
    async fn channel(&self, id: ChannelId) -> Option<ChannelRecord> {
        self.0.channel(id).await
    }

    async fn channel_message(&self, id: ChannelId, index: usize) -> Result<Vec<u8>, MessageError> {
        self.0.channel_message(id, index).await
    }

    fn channel_messages(
        &self,
        id: ChannelId,
        indices: ops::RangeInclusive<usize>,
    ) -> BoxStream<'_, (usize, Vec<u8>)> {
        self.0.channel_messages(id, indices)
    }
}

#[async_trait]
impl<T: ReadonlyStorageTransaction> ReadWorkflows for Readonly<T> {
    async fn count_active_workflows(&self) -> usize {
        self.0.count_active_workflows().await
    }

    async fn workflow(&self, id: WorkflowId) -> Option<WorkflowRecord> {
        self.0.workflow(id).await
    }

    async fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>> {
        self.0.nearest_timer_expiration().await
    }
}
