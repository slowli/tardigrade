//! Data types used in storage traits.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing_tunnel::{PersistedMetadata, PersistedSpans};

use std::{
    collections::{HashMap, HashSet},
    error, fmt,
    sync::Arc,
};

use crate::{
    receipt::ExecutionError,
    utils::{clone_join_error, serde_b64},
    workflow::PersistedWorkflow,
};
use tardigrade::{interface::Interface, task::JoinError, ChannelId, WakerId, WorkflowId};

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
    /// Workflow definitions from this module keyed by the definition ID.
    pub definitions: HashMap<String, DefinitionRecord>,
}

impl fmt::Debug for ModuleRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModuleRecord")
            .field("id", &self.id)
            .field("bytes_len", &self.bytes.len())
            .field("tracing_metadata", &self.tracing_metadata)
            .field("definitions", &self.definitions)
            .finish()
    }
}

/// Storage record for a workflow definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefinitionRecord {
    /// Interface associated with the definition.
    pub interface: Interface,
}

/// Error retrieving a message from a workflow channel. Returned by
/// [`ReadChannels::channel_message()`].
///
/// [`ReadChannels::channel_message()`]: crate::storage::ReadChannels::channel_message()
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

impl ChannelRecord {
    /// Returns a record for a channel with both sides closed.
    pub fn closed() -> Self {
        Self {
            receiver_workflow_id: None,
            sender_workflow_ids: HashSet::new(),
            has_external_sender: false,
            is_closed: true,
            received_messages: 0,
        }
    }

    /// Returns a record for a channel with both sides owned by the host.
    pub fn owned_by_host() -> Self {
        Self {
            receiver_workflow_id: None,
            sender_workflow_ids: HashSet::new(),
            has_external_sender: true,
            is_closed: false,
            received_messages: 0,
        }
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)] // required by serde
fn is_false(&flag: &bool) -> bool {
    !flag
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
    /// Number of the workflow executions.
    pub execution_count: usize,
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
                execution_count: self.execution_count,
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
                execution_count: self.execution_count,
                state,
            }),
            WorkflowState::Completed(_) | WorkflowState::Active(_) => None,
        }
    }
}

impl<T> WorkflowRecord<T> {
    pub(crate) fn split(self) -> (WorkflowRecord<()>, T) {
        let state = self.state;
        let record = WorkflowRecord {
            id: self.id,
            parent_id: self.parent_id,
            module_id: self.module_id,
            name_in_module: self.name_in_module,
            execution_count: self.execution_count,
            state: (),
        };
        (record, state)
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
/// [`insert_waker_for_matching_workflows()`]: crate::storage::WriteWorkflowWakers::insert_waker_for_matching_workflows()
#[derive(Debug)]
#[non_exhaustive]
pub enum WorkflowSelectionCriteria {
    /// Workflow has an active timer before the specified timestamp.
    HasTimerBefore(DateTime<Utc>),
}

impl WorkflowSelectionCriteria {
    pub(super) fn matches(&self, workflow: &PersistedWorkflow) -> bool {
        match self {
            Self::HasTimerBefore(time) => workflow
                .common()
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
