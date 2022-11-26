//! [`Receipt`]s for workflows and associated types.
//!
//! The root type is [`Receipt`] that records zero or more [`Execution`]s. Each `Execution`
//! records [`Event`]s that have occurred when executing a specific [function](ExecutedFunction)
//! from the WASM definition of the [`WorkflowModule`]. `Event`s can relate either to a message
//! channel connected to the workflow ([`ChannelEvent`]), or to a resource managed by the runtime
//! ([`ResourceEvent`]), such as a task or a timer.
//!
//! [`WorkflowModule`]: crate::WorkflowModule

use serde::{Deserialize, Serialize};

use std::{error, fmt, ops::Range, sync::Arc, task::Poll};

use crate::utils::{serde_poll, serde_poll_res, serde_trap};
use tardigrade::{
    channel::SendError,
    task::{ErrorLocation, TaskError, TaskResult},
    ChannelId, TaskId, TimerId, WakerId, WorkflowId,
};

/// Cause of waking up a workflow task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WakeUpCause {
    /// Woken up by an inbound message.
    InboundMessage {
        /// ID of the channel.
        channel_id: ChannelId,
        /// 0-based message index.
        message_index: usize,
    },
    /// Woken up by a channel getting closed.
    ChannelClosed {
        /// ID of the channel.
        channel_id: ChannelId,
    },
    /// Woken up by flushing a channel.
    Flush {
        /// ID of the channel.
        channel_id: ChannelId,
        /// Indexes of flushed messages.
        message_indexes: Range<usize>,
    },

    /// Initial task enqueuing after it was spawned.
    Spawned,
    /// Woken up by an executed function, such as another task (e.g., due to internal channels
    /// or other sync primitives).
    Function {
        /// ID of the task if executed function is a task; `None` otherwise.
        task_id: Option<TaskId>,
    },
    /// Woken up by task completion.
    CompletedTask(TaskId),
    /// Woken up by a timer.
    Timer {
        /// Timer ID.
        id: TimerId,
    },
    /// Woken up by workflow initialization.
    InitWorkflow {
        /// ID of the stub that got initialized.
        stub_id: WorkflowId,
    },
    /// Woken up by completion of a child workflow.
    CompletedWorkflow(WorkflowId),
}

/// Executed top-level WASM function.
///
/// These functions are exported from the workflow WASM module and are called during different
/// stages of the workflow lifecycle (e.g., after receiving an inbound message or completing
/// a timer).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ExecutedFunction {
    /// Entry point of the workflow.
    #[non_exhaustive]
    Entry,
    /// Polling a task.
    #[non_exhaustive]
    Task {
        /// ID of the task.
        task_id: TaskId,
        /// Cause of the task waking up.
        wake_up_cause: WakeUpCause,
    },
    /// Waking up a [`Waker`](std::task::Waker).
    #[non_exhaustive]
    Waker {
        /// ID of the waker.
        waker_id: WakerId,
        /// Cause of waking up the waker.
        wake_up_cause: WakeUpCause,
    },
    /// Dropping a completed task.
    #[non_exhaustive]
    TaskDrop {
        /// ID of the task getting dropped.
        task_id: TaskId,
    },
}

impl ExecutedFunction {
    pub(crate) fn task_id(&self) -> Option<TaskId> {
        match self {
            Self::Task { task_id, .. } => Some(*task_id),
            _ => None,
        }
    }

    fn wake_up_cause(&self) -> Option<&WakeUpCause> {
        match self {
            Self::Task { wake_up_cause, .. } | Self::Waker { wake_up_cause, .. } => {
                Some(wake_up_cause)
            }
            _ => None,
        }
    }

    fn write_summary(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Entry { .. } => formatter.write_str("spawning workflow"),
            Self::Task { task_id, .. } => {
                write!(formatter, "polling task {}", task_id)
            }
            Self::Waker { waker_id, .. } => {
                write!(formatter, "waking up waker {}", waker_id)
            }
            Self::TaskDrop { task_id } => {
                write!(formatter, "dropping task {}", task_id)
            }
        }
    }
}

/// ID of a host-managed resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResourceId {
    /// Timer ID.
    Timer(TimerId),
    /// Task ID.
    Task(TaskId),
    /// Workflow ID.
    Workflow(WorkflowId),
    /// Workflow stub ID.
    WorkflowStub(WorkflowId),
}

/// Kind of a [`ResourceEvent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResourceEventKind {
    /// The resource was created.
    Created,
    /// The resource was dropped.
    Dropped,
    /// The resource was polled for completion.
    Polled(#[serde(with = "serde_poll")] Poll<()>),
}

/// Event related to a host-managed resource (a task or a timer).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ResourceEvent {
    /// Resource ID.
    pub resource_id: ResourceId,
    /// Event kind.
    pub kind: ResourceEventKind,
}

/// Kind of a [`ChannelEvent`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ChannelEventKind {
    /// Channel receiver was polled for messages.
    ReceiverPolled {
        /// Result of a poll, with the message replaced with its byte length.
        #[serde(with = "serde_poll")]
        result: Poll<Option<usize>>,
    },
    /// Channel receiver closed by the workflow logic.
    ReceiverClosed,

    /// Channel sender was polled for readiness.
    SenderReady {
        /// Result of a poll.
        #[serde(with = "serde_poll_res")]
        result: Poll<Result<(), SendError>>,
    },
    /// Message was sent via a channel sender.
    OutboundMessageSent {
        /// Byte length of the sent message.
        message_len: usize,
    },
    /// Channel sender was polled for flush.
    SenderFlushed {
        /// Result of a poll.
        #[serde(with = "serde_poll_res")]
        result: Poll<Result<(), SendError>>,
    },
    /// Channel sender closed by the workflow logic.
    SenderClosed,
}

/// Event related to a workflow channel receiver or sender.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ChannelEvent {
    /// Event kind.
    pub kind: ChannelEventKind,
    /// ID of the channel.
    pub channel_id: ChannelId,
}

/// Event included into a [`Receipt`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Event {
    /// Event related to a host-managed resource (a task or a timer).
    Resource(ResourceEvent),
    /// Event related to a channel sender / receiver.
    Channel(ChannelEvent),
}

impl From<ResourceEvent> for Event {
    fn from(event: ResourceEvent) -> Self {
        Self::Resource(event)
    }
}

impl From<ChannelEvent> for Event {
    fn from(event: ChannelEvent) -> Self {
        Self::Channel(event)
    }
}

impl Event {
    pub(crate) fn as_resource_event(&self) -> Option<&ResourceEvent> {
        if let Event::Resource(res_event) = self {
            Some(res_event)
        } else {
            None
        }
    }

    pub(crate) fn as_channel_event(&self) -> Option<&ChannelEvent> {
        if let Event::Channel(chan_event) = self {
            Some(chan_event)
        } else {
            None
        }
    }
}

/// Execution of a top-level WASM function with a list of events that have occurred during
/// the execution.
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Execution {
    /// The top-level WASM function getting executed.
    pub function: ExecutedFunction,
    /// Events that have occurred during the execution (in the order of their appearance).
    pub events: Vec<Event>,
    /// Result of executing a task. This field can only be set for
    /// [task executions](ExecutedFunction::Task), and it is `None` if the task is
    /// not completed as a result of this execution.
    pub task_result: Option<TaskResult>,
}

impl Clone for Execution {
    fn clone(&self) -> Self {
        Self {
            function: self.function.clone(),
            events: self.events.clone(),
            task_result: self
                .task_result
                .as_ref()
                .map(|result| result.as_ref().copied().map_err(TaskError::clone_boxed)),
        }
    }
}

impl Execution {
    /// Returns the cause of this execution.
    pub fn cause(&self) -> Option<&WakeUpCause> {
        self.function.wake_up_cause()
    }
}

/// Receipt for executing tasks in a workflow.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Receipt {
    pub(crate) executions: Vec<Execution>,
}

impl Receipt {
    pub(crate) fn extend(&mut self, other: Self) {
        self.executions.extend(other.executions);
    }

    /// Returns the list of executed top-level WASM functions together with [`Event`]s that
    /// have occurred during their execution.
    pub fn executions(&self) -> &[Execution] {
        &self.executions
    }

    /// Iterates over events in all executions in order.
    pub fn events(&self) -> impl Iterator<Item = &Event> + '_ {
        self.executions
            .iter()
            .flat_map(|execution| &execution.events)
    }
}

/// Error occurring during workflow execution.
///
/// An error is caused by the executed WASM code [`Trap`]ping, which can be caused by a panic
/// in the workflow logic, or misuse of Tardigrade runtime APIs. (The latter should not happen
/// if properly using the Tardigrade client library.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionError {
    #[serde(with = "serde_trap")]
    trap: Arc<anyhow::Error>,
    panic_info: Option<PanicInfo>,
    receipt: Receipt,
}

impl ExecutionError {
    pub(crate) fn new(
        trap: anyhow::Error,
        panic_info: Option<PanicInfo>,
        receipt: Receipt,
    ) -> Self {
        Self {
            trap: Arc::new(trap),
            panic_info,
            receipt,
        }
    }

    /// Returns an [`Error`](anyhow::Error) that has led to this execution error.
    pub fn trap(&self) -> &anyhow::Error {
        &self.trap
    }

    /// Returns information about a panic, if any. The panic info may be absent depending
    /// on the workflow config.
    pub fn panic_info(&self) -> Option<&PanicInfo> {
        self.panic_info.as_ref()
    }

    /// Returns a [`Receipt`] for the execution. The last [`Execution`] in the receipt
    /// is the one that trapped.
    pub fn receipt(&self) -> &Receipt {
        &self.receipt
    }
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "workflow execution failed while ")?;
        let execution = self.receipt.executions.last().unwrap();
        execution.function.write_summary(formatter)?;
        if let Some(panic_info) = &self.panic_info {
            write!(formatter, ": {panic_info}")
        } else {
            write!(formatter, ": {}", self.trap)
        }
    }
}

impl error::Error for ExecutionError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(anyhow::Error::as_ref(&self.trap))
    }
}

/// Information about a panic in the workflow code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PanicInfo {
    /// Human-readable panic message.
    pub message: Option<String>,
    /// Location where the panic has occurred.
    pub location: Option<ErrorLocation>,
}

impl fmt::Display for PanicInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.message, &self.location) {
            (Some(message), Some(location)) => {
                write!(formatter, "panic at {}: {}", location, message)
            }
            (Some(message), None) => formatter.write_str(message),
            (None, Some(location)) => write!(formatter, "panic at {}", location),
            (None, None) => formatter.write_str("panic at unknown location"),
        }
    }
}

impl PanicInfo {
    pub(crate) fn into_parts(self) -> (String, ErrorLocation) {
        let message = self
            .message
            .unwrap_or_else(|| "task execution failed".to_owned());
        let location = self.location.unwrap_or(ErrorLocation::UNKNOWN);
        (message, location)
    }
}

impl From<PanicInfo> for TaskError {
    fn from(info: PanicInfo) -> Self {
        let (message, location) = info.into_parts();
        Self::from_parts(message, location)
    }
}
