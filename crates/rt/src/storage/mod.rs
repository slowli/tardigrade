//! Async, transactional storage abstraction for storing workflows and channel state.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tracing_tunnel::{PersistedMetadata, PersistedSpans};

use std::{collections::HashSet, error, fmt, sync::Arc};

mod local;

pub use self::local::{LocalStorage, LocalStorageSnapshot, LocalTransaction, ModuleRecordMut};

use crate::{
    utils::{clone_join_error, serde_b64},
    PersistedWorkflow,
};
use tardigrade::{task::JoinError, ChannelId, WorkflowId};

/// Async, transactional storage for workflows and workflow channels.
///
/// A storage is required to instantiate a [`WorkflowManager`](crate::manager::WorkflowManager).
#[async_trait]
pub trait Storage<'a>: 'static + Send + Sync {
    /// Read/write transaction for the storage. See [`StorageTransaction`] for required
    /// transaction semantics.
    type Transaction: 'a + StorageTransaction;
    /// Readonly transaction for the storage.
    type ReadonlyTransaction: 'a + StorageReadonlyTransaction;

    /// Creates a new read/write transaction.
    async fn transaction(&'a self) -> Self::Transaction;
    /// Creates a new readonly transaction.
    async fn readonly_transaction(&'a self) -> Self::ReadonlyTransaction;
}

/// [`Storage`] transaction with readonly access to the storage.
pub trait StorageReadonlyTransaction:
    Send + Sync + ReadModules + ReadChannels + ReadWorkflows
{
}

impl<T> StorageReadonlyTransaction for T where
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

/// Allows reading stored information about [`WorkflowModule`](crate::WorkflowModule)s.
#[async_trait]
pub trait ReadModules {
    /// Retrieves a module with the specified ID.
    async fn module(&self, id: &str) -> Option<ModuleRecord>;

    /// Streams all modules.
    fn modules(&self) -> BoxStream<'_, ModuleRecord>;
}

/// Allows modifying stored information about [`WorkflowModule`](crate::WorkflowModule)s.
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

/// Storage record for a [`WorkflowModule`](crate::WorkflowModule).
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

    /// Receives a message with the specified 0-based index from the specified channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be retrieved.
    async fn channel_message(&self, id: ChannelId, index: usize) -> Result<Vec<u8>, MessageError>;

    /// Selects a channel with a message or EOF marker consumable by the receiver workflow.
    async fn find_consumable_channel(&self) -> Option<(ChannelId, ChannelRecord)>;
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
    pub receiver_workflow_id: Option<WorkflowId>,
    /// IDs of sender workflows.
    pub sender_workflow_ids: HashSet<WorkflowId>,
    /// `true` if the channel has an external sender.
    pub has_external_sender: bool,
    /// `true` if the channel is closed (i.e., no more messages can be written to it).
    pub is_closed: bool,
    /// Number of messages written to the channel.
    pub received_messages: usize,
}

/// Allows reading information about workflows.
#[async_trait]
pub trait ReadWorkflows {
    /// Returns the number of active workflows.
    async fn count_workflows(&self) -> usize;
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

    /// Persists a workflow with the specified ID.
    async fn persist_workflow(
        &mut self,
        id: WorkflowId,
        workflow: PersistedWorkflow,
        tracing_spans: PersistedSpans,
    );

    /// Manipulates the persisted part of a workflow.
    async fn manipulate_workflow<F: FnOnce(&mut PersistedWorkflow) + Send>(
        &mut self,
        id: WorkflowId,
        action: F,
    ) -> Option<WorkflowRecord>;

    /// Deletes a workflow with the specified ID.
    async fn delete_workflow(&mut self, id: WorkflowId);
}

/// State of a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRecord {
    /// ID of the workflow.
    pub id: WorkflowId,
    /// ID of the parent workflow, or `None` if this is a root workflow.
    pub parent_id: Option<WorkflowId>,
    /// ID of the module in which the workflow is defined.
    pub module_id: String,
    /// Name of the workflow in the module.
    pub name_in_module: String,
    /// Persisted workflow state.
    pub persisted: PersistedWorkflow,
    /// Tracing spans associated with the workflow.
    pub tracing_spans: PersistedSpans,
}

/// Workflow selection criteria used in [`WriteWorkflows::manipulate_all_workflows()`].
#[derive(Debug)]
#[non_exhaustive]
pub enum WorkflowSelectionCriteria {
    /// Workflow has an active timer before the specified timestamp.
    HasTimerBefore(DateTime<Utc>),
}

impl WorkflowSelectionCriteria {
    fn matches(&self, record: &WorkflowRecord) -> bool {
        match self {
            Self::HasTimerBefore(time) => record
                .persisted
                .timers()
                .any(|(_, timer)| timer.definition().expires_at <= *time),
        }
    }
}

/// Waker for a workflow.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkflowWaker {
    /// Waker produced by the workflow execution, e.g., flushing outbound messages
    /// or initializing a child workflow.
    Internal,
    /// Waker produced by a timer.
    Timer(DateTime<Utc>),
    /// Waker produced by an outbound channel closure.
    OutboundChannelClosure(ChannelId),
    /// Waker produced by a completed child workflow.
    // FIXME: store result in workflow entity
    ChildCompletion(WorkflowId, Result<(), JoinError>),
}

impl Clone for WorkflowWaker {
    fn clone(&self) -> Self {
        match self {
            Self::Internal => Self::Internal,
            Self::Timer(timer) => Self::Timer(*timer),
            Self::OutboundChannelClosure(channel_id) => Self::OutboundChannelClosure(*channel_id),
            Self::ChildCompletion(id, result) => {
                let result = result.as_ref().copied().map_err(clone_join_error);
                Self::ChildCompletion(*id, result)
            }
        }
    }
}

impl WorkflowWaker {
    pub(crate) fn for_workflow(self, workflow_id: WorkflowId) -> WorkflowWakerRecord {
        WorkflowWakerRecord {
            workflow_id,
            waker: self,
        }
    }
}

/// Record associating a [`WorkflowWaker`] with a particular workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowWakerRecord {
    /// ID of the workflow that the waker belongs to.
    pub workflow_id: WorkflowId,
    /// The waker.
    pub waker: WorkflowWaker,
}

/// Allows modifying stored information about [`WorkflowWaker`].
#[async_trait]
pub trait WriteWorkflowWakers {
    /// Inserts a workflow waker into the queue.
    async fn insert_waker(&mut self, waker: WorkflowWakerRecord);
    /// Inserts a workflow waker for all workflows matching the specified criteria.
    async fn insert_waker_for_matching_workflows(
        &mut self,
        criteria: WorkflowSelectionCriteria,
        waker: WorkflowWaker,
    );

    /// Removes and returns all workflow wakers for a single workflow. The selection of the workflow
    /// is up to the implementation. If there are no workflows with a waker,
    /// returns an empty `Vec`.
    async fn delete_wakers_for_single_workflow(&mut self) -> Vec<WorkflowWakerRecord>;
}

/// Wrapper for [`StorageTransaction`] implementations that allows safely using them
/// as readonly transactions.
#[derive(Debug)]
pub struct Readonly<T>(T);

impl<T: StorageReadonlyTransaction> From<T> for Readonly<T> {
    fn from(inner: T) -> Self {
        Self(inner)
    }
}

impl<T: StorageReadonlyTransaction> AsRef<T> for Readonly<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

#[async_trait]
impl<T: StorageReadonlyTransaction> ReadModules for Readonly<T> {
    async fn module(&self, id: &str) -> Option<ModuleRecord> {
        self.0.module(id).await
    }

    fn modules(&self) -> BoxStream<'_, ModuleRecord> {
        self.0.modules()
    }
}

#[async_trait]
impl<T: StorageReadonlyTransaction> ReadChannels for Readonly<T> {
    async fn channel(&self, id: ChannelId) -> Option<ChannelRecord> {
        self.0.channel(id).await
    }

    // TODO: add retrieving messages by an index range?
    async fn channel_message(&self, id: ChannelId, index: usize) -> Result<Vec<u8>, MessageError> {
        self.0.channel_message(id, index).await
    }

    async fn find_consumable_channel(&self) -> Option<(ChannelId, ChannelRecord)> {
        self.0.find_consumable_channel().await
    }
}

#[async_trait]
impl<T: StorageReadonlyTransaction> ReadWorkflows for Readonly<T> {
    async fn count_workflows(&self) -> usize {
        self.0.count_workflows().await
    }

    async fn workflow(&self, id: WorkflowId) -> Option<WorkflowRecord> {
        self.0.workflow(id).await
    }

    async fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>> {
        self.0.nearest_timer_expiration().await
    }
}
