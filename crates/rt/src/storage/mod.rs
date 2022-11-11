//! Storage abstraction.

#![allow(missing_docs)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tracing_tunnel::{PersistedMetadata, PersistedSpans};

use std::{borrow::Cow, collections::HashSet, error, fmt};

mod local;

pub use self::local::{LocalStorage, LocalStorageSnapshot, LocalTransaction};

use crate::{utils::serde_b64, PersistedWorkflow};
use tardigrade::{channel::SendError, ChannelId, WorkflowId};

#[async_trait]
pub trait Storage<'a>: 'a + Send + Sync {
    type Transaction: 'a + StorageTransaction;
    type ReadonlyTransaction: 'a + Send + Sync + ReadModules + ReadChannels + ReadWorkflows;

    // FIXME: specify drop behavior etc.
    async fn transaction(&'a self) -> Self::Transaction;

    async fn readonly_transaction(&'a self) -> Self::ReadonlyTransaction;
}

#[must_use = "transactions should be committed to take effect"]
#[async_trait]
pub trait StorageTransaction: Send + Sync + WriteModules + WriteChannels + WriteWorkflows {
    // TODO: errors?
    async fn commit(self);
}

#[async_trait]
pub trait ReadModules {
    /// Retrieves a module with the specified ID.
    async fn module(&self, id: &str) -> Option<ModuleRecord<'static>>;
    /// Streams all modules.
    fn modules(&self) -> BoxStream<'_, ModuleRecord<'static>>;
}

#[async_trait]
pub trait WriteModules: ReadModules {
    /// Inserts the module into the storage.
    async fn insert_module(&mut self, module: ModuleRecord<'_>);

    /// Updates tracing metadata for the module.
    ///
    /// It is acceptable to merge the provided metadata JSON to the current one to prevent edit
    /// conflicts.
    async fn update_tracing_metadata(&mut self, module_id: &str, metadata: PersistedMetadata);
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ModuleRecord<'a> {
    /// ID of the module.
    pub id: String,
    /// WASM module bytes.
    #[serde(with = "serde_b64")]
    pub bytes: Cow<'a, [u8]>,
    /// Persisted metadata.
    pub tracing_metadata: PersistedMetadata,
}

impl fmt::Debug for ModuleRecord<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModuleRecord")
            .field("id", &self.id)
            .field("bytes_len", &self.bytes.len())
            .field("tracing_metadata", &self.tracing_metadata)
            .finish()
    }
}

#[async_trait]
pub trait ReadChannels {
    async fn channel(&self, id: ChannelId) -> Option<ChannelRecord>;
    /// Checks whether the specified channel has messages unconsumed by the receiver workflow.
    /// If there is no receiver workflow for the channel, returns `false`.
    async fn has_messages_for_receiver_workflow(&self, id: ChannelId) -> bool;
    /// Receives a message from the channel.
    async fn channel_message(&self, id: ChannelId, index: usize) -> Result<Vec<u8>, MessageError>;
}

#[async_trait]
pub trait WriteChannels: ReadChannels {
    /// Allocates a new unique ID for a channel.
    async fn allocate_channel_id(&mut self) -> ChannelId;
    /// Creates a new channel with the provided `state`.
    async fn get_or_insert_channel(&mut self, id: ChannelId, state: ChannelState) -> ChannelState;
    /// Changes the channel state and selects it for further updates.
    async fn manipulate_channel<F: FnOnce(&mut ChannelState) + Send>(
        &mut self,
        id: ChannelId,
        action: F,
    ) -> ChannelState;

    /// Pushes one or more messages into the channel.
    async fn push_messages(
        &mut self,
        id: ChannelId,
        messages: Vec<Vec<u8>>,
    ) -> Result<(), SendError>;

    /// Truncates the channel so that `min_index` is the minimum retained index.
    async fn truncate_channel(&mut self, id: ChannelId, min_index: usize);
}

#[derive(Debug)]
#[non_exhaustive]
pub enum MessageError {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelState {
    pub(crate) receiver_workflow_id: Option<WorkflowId>,
    pub(crate) sender_workflow_ids: HashSet<WorkflowId>,
    pub(crate) has_external_sender: bool,
    pub(crate) is_closed: bool,
}

#[derive(Debug)]
pub struct ChannelRecord {
    pub state: ChannelState,
    pub next_message_idx: usize,
}

impl ChannelRecord {
    /// Checks if the channel is closed (i.e., no new messages can be written into it).
    pub fn is_closed(&self) -> bool {
        self.state.is_closed
    }

    /// Returns the number of messages written to the channel.
    pub fn received_messages(&self) -> usize {
        self.next_message_idx
    }

    /// Returns the ID of a workflow that holds the receiver end of this channel, or `None`
    /// if it is held by the host.
    pub fn receiver_workflow_id(&self) -> Option<WorkflowId> {
        self.state.receiver_workflow_id
    }
}

#[async_trait]
pub trait ReadWorkflows {
    /// Returns the number of active workflows.
    async fn count_workflows(&self) -> usize;
    /// Retrieves a snapshot of the workflow with the specified ID.
    async fn workflow(&self, id: WorkflowId) -> Option<WorkflowRecord>;

    /// Selects a workflow with pending tasks for execution.
    async fn find_workflow_with_pending_tasks(&self) -> Option<WorkflowRecord>;
    /// Selects a workflow to which a message can be sent for execution.
    async fn find_consumable_channel(&self) -> Option<(ChannelId, usize, WorkflowRecord)>;
    /// Finds the nearest timer expiration in all active workflows.
    async fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>>;
}

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

    /// Manipulates the persisted part of a workflow without restoring it.
    async fn manipulate_workflow<F: FnOnce(&mut PersistedWorkflow) + Send>(
        &mut self,
        id: WorkflowId,
        action: F,
    ) -> Option<WorkflowRecord>;

    async fn manipulate_all_workflows<F: FnMut(&mut PersistedWorkflow) + Send>(
        &mut self,
        criteria: WorkflowSelectionCriteria,
        action: F,
    );

    /// Deletes a workflow with the specified ID.
    async fn delete_workflow(&mut self, id: WorkflowId);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRecord {
    pub id: WorkflowId,
    pub parent_id: Option<WorkflowId>,
    pub module_id: String,
    pub name_in_module: String,
    pub persisted: PersistedWorkflow,
    pub tracing_spans: PersistedSpans,
}

#[derive(Debug)]
pub enum WorkflowSelectionCriteria {
    HasTimerBefore(DateTime<Utc>),
}

impl WorkflowSelectionCriteria {
    pub(crate) fn matches(&self, record: &WorkflowRecord) -> bool {
        match self {
            Self::HasTimerBefore(time) => record
                .persisted
                .timers()
                .any(|(_, timer)| timer.definition().expires_at <= *time),
        }
    }
}
