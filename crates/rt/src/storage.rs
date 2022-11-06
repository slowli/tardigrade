//! Storage abstraction.

#![allow(missing_docs)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use tracing_tunnel::{PersistedMetadata, PersistedSpans};

use std::{collections::HashSet, pin::Pin};

use crate::PersistedWorkflow;
use tardigrade::{channel::SendError, ChannelId, WorkflowId};

pub type BoxedStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

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
pub trait StorageTransaction: Send + Sync + ReadModules + WriteChannels + WriteWorkflows {
    // TODO: errors?
    async fn commit(self);
}

#[async_trait]
pub trait ReadModules {
    /// Retrieves a module with the specified ID.
    async fn module(&self, id: &str) -> Option<ModuleRecord>;
    /// Streams all modules.
    fn modules(&self) -> BoxedStream<'_, ModuleRecord>;
}

#[derive(Debug)]
pub struct ModuleRecord {
    /// WASM module bytes.
    pub bytes: Vec<u8>,
    /// Tracing metadata associated with the module.
    pub metadata: PersistedMetadata,
}

#[async_trait]
pub trait ReadChannels {
    async fn channel(&self, id: ChannelId) -> Option<ChannelRecord>;
}

#[async_trait]
pub trait WriteChannels: ReadChannels {
    type Token: Send + Sync + 'static;

    /// Allocates a new unique ID for a channel.
    async fn allocate_channel_id(&mut self) -> ChannelId;
    /// Creates a new channel with the provided `spec`.
    async fn get_or_insert_channel(&mut self, id: ChannelId, spec: ChannelState) -> ChannelState;
    /// Changes the channel state and selects it for further updates.
    async fn manipulate_channel(
        &mut self,
        id: ChannelId,
        action: impl FnOnce(&mut ChannelState),
    ) -> ChannelState;

    /// Pushes one or more messages into the channel.
    async fn push_messages(
        &mut self,
        channel_id: ChannelId,
        messages: Vec<Vec<u8>>,
    ) -> Result<(), SendError>;

    /// Receives a message from the channel.
    // TODO: lease timeout?
    async fn receive_message(&mut self, id: ChannelId) -> Option<(MessageOrEof, Self::Token)>;

    /// Removes a previously received message from the channel.
    async fn remove_message(&mut self, token: Self::Token) -> Result<(), RemoveError>;

    /// Places a previously received message back to the channel.
    async fn revert_message(&mut self, token: Self::Token) -> Result<(), RemoveError>;
}

#[derive(Debug)]
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
pub enum RemoveError {
    ExpiredToken,
}

#[async_trait]
pub trait ReadWorkflows {
    /// Retrieves a snapshot of the workflow with the specified ID.
    async fn workflow(&self, id: WorkflowId) -> Option<WorkflowRecord>;

    /// Selects a workflow with pending tasks for execution.
    async fn find_workflow_with_pending_tasks(&self) -> Option<WorkflowRecord>;
    /// Selects a workflow to which a message can be sent for execution.
    async fn find_consumable_channel(&self) -> Option<(ChannelId, WorkflowRecord)>;
}

#[async_trait]
pub trait WriteWorkflows: ReadWorkflows {
    /// Allocates a new unique ID for a workflow.
    async fn allocate_workflow_id(&mut self) -> WorkflowId;
    /// Creates a new workflow.
    async fn create_workflow(&mut self, state: WorkflowRecord);
    /// Persists a workflow with the specified ID.
    async fn persist_workflow(
        &mut self,
        id: WorkflowId,
        workflow: PersistedWorkflow,
        tracing_spans: PersistedSpans,
    );
    /// Manipulates the persisted part of a workflow without restoring it.
    async fn manipulate_workflow(
        &mut self,
        id: WorkflowId,
        action: impl FnOnce(&mut PersistedWorkflow),
    ) -> WorkflowRecord;

    async fn manipulate_all_workflows(
        &mut self,
        criteria: WorkflowSelectionCriteria,
        action: impl FnMut(&mut PersistedWorkflow),
    );

    /// Removes a workflow with the specified ID.
    async fn remove_workflow(&mut self, id: WorkflowId);
}

#[derive(Debug)]
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
