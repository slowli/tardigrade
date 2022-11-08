//! Storage abstraction.

#![allow(missing_docs)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use tracing_tunnel::{PersistedMetadata, PersistedSpans};

use std::{borrow::Cow, collections::HashSet};

mod local;

pub use self::local::{LocalStorage, LocalToken, LocalTransaction};

use crate::PersistedWorkflow;
use tardigrade::{channel::SendError, ChannelId, WorkflowId};

#[async_trait]
pub trait Storage<'a>: 'a + Send + Sync {
    type Transaction: StorageTransaction;
    type ReadonlyTransaction: Send + Sync + ReadModules + ReadChannels + ReadWorkflows;

    // FIXME: specify drop behavior etc.
    async fn transaction(&'a self) -> Self::Transaction;

    async fn readonly_transaction(&'a self) -> Self::ReadonlyTransaction;
}

#[async_trait]
#[must_use = "transactions should be committed to take effect"]
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

#[derive(Debug, Clone)]
pub struct ModuleRecord<'a> {
    /// ID of the module.
    pub id: String,
    /// WASM module bytes.
    pub bytes: Cow<'a, [u8]>,
    /// Persisted metadata.
    pub tracing_metadata: PersistedMetadata,
}

#[async_trait]
pub trait ReadChannels {
    async fn channel(&self, id: ChannelId) -> Option<ChannelRecord>;
    /// Checks whether a channel with the specified ID has unconsumed messages.
    async fn has_messages(&self, id: ChannelId) -> bool;
}

#[async_trait]
pub trait WriteChannels: ReadChannels {
    type Token: Send + Sync + 'static;

    /// Allocates a new unique ID for a channel.
    async fn allocate_channel_id(&mut self) -> ChannelId;
    /// Creates a new channel with the provided `spec`.
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

    /// Pops a message from the channel.
    // TODO: lease timeout?
    async fn pop_message(&mut self, id: ChannelId) -> Option<(MessageOrEof, Self::Token)>;

    /// Removes a previously received message from the channel.
    async fn confirm_message_removal(
        &mut self,
        token: Self::Token,
    ) -> Result<(), MessageOperationError>;

    /// Places a previously received message back to the channel.
    async fn revert_message_removal(
        &mut self,
        token: Self::Token,
    ) -> Result<(), MessageOperationError>;
}

#[derive(Debug, Clone)]
pub enum MessageOrEof {
    Message(Vec<u8>),
    Eof,
}

#[derive(Debug, Clone)]
pub struct ChannelState {
    pub(crate) receiver_workflow_id: Option<WorkflowId>,
    pub(crate) sender_workflow_ids: HashSet<WorkflowId>,
    pub(crate) has_external_sender: bool,
    pub(crate) is_closed: bool,
}

#[derive(Debug)]
pub struct ChannelRecord {
    pub state: ChannelState,
    pub message_count: usize,
    pub next_message_idx: usize,
}

impl ChannelRecord {
    /// Checks if the channel is closed (i.e., no new messages can be written into it).
    pub fn is_closed(&self) -> bool {
        self.state.is_closed
    }

    /// Checks if this channel has no pending messages.
    pub fn is_empty(&self) -> bool {
        self.message_count == 0
    }

    /// Returns the number of messages written to the channel.
    pub fn received_messages(&self) -> usize {
        self.next_message_idx
    }

    /// Returns the number of messages consumed by the channel receiver.
    pub fn flushed_messages(&self) -> usize {
        self.next_message_idx - self.message_count
    }

    /// Returns the ID of a workflow that holds the receiver end of this channel, or `None`
    /// if it is held by the host.
    pub fn receiver_workflow_id(&self) -> Option<WorkflowId> {
        self.state.receiver_workflow_id
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum MessageOperationError {
    InvalidToken,
    AlreadyRemoved,
    ExpiredToken,
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
    async fn find_consumable_channel(&self) -> Option<(ChannelId, WorkflowRecord)>;
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

#[derive(Debug, Clone)]
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
                .any(|(_, timer)| timer.definition().expires_at < *time),
        }
    }
}
