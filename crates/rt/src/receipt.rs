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
use wasmtime::Trap;

use std::{error, fmt, ops::Range, task::Poll};

use crate::{ChannelId, TaskId, TimerId, WakerId, WorkflowId};
use tardigrade_shared::SendError;

/// Cause of waking up a workflow task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WakeUpCause {
    /// Woken up by an inbound message.
    InboundMessage {
        /// ID of the remote workflow that the channel is attached to, or `None` if the channel
        /// is local.
        workflow_id: Option<WorkflowId>,
        /// Name of the inbound channel that has received a message.
        channel_name: String,
        /// 0-based message index.
        message_index: usize,
    },
    /// Woken up by an inbound channel getting closed.
    ChannelClosed {
        /// ID of the remote workflow that the channel is attached to, or `None` if the channel
        /// is local.
        workflow_id: Option<WorkflowId>,
        /// Name of the inbound channel that was closed.
        channel_name: String,
    },
    /// Woken up by flushing an outbound channel.
    Flush {
        /// ID of the remote workflow that the channel is attached to, or `None` if the channel
        /// is local.
        workflow_id: Option<WorkflowId>,
        /// Name of the outbound channel that was flushed.
        channel_name: String,
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
    /// Woken up by completion of a child workflow.
    CompletedWorkflow(WorkflowId),
}

/// Executed top-level WASM function.
///
/// These functions are exported from the workflow WASM module and are called during different
/// stages of the workflow lifecycle (e.g., after receiving an inbound message or completing
/// a timer).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ExecutedFunction {
    /// Entry point of the workflow.
    #[non_exhaustive]
    Entry {
        /// ID of the created main task.
        // TODO: kind of awkward; ideally, this field should be removed
        task_id: TaskId,
    },
    /// Polling a task.
    #[non_exhaustive]
    Task {
        /// ID of the task.
        task_id: TaskId,
        /// Cause of the task waking up.
        wake_up_cause: WakeUpCause,
        /// Result of polling a task.
        poll_result: Poll<()>,
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
            Self::Entry { task_id } | Self::Task { task_id, .. } => Some(*task_id),
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ResourceId {
    /// Timer ID.
    Timer(TimerId),
    /// Task ID.
    Task(TaskId),
    /// Workflow ID.
    Workflow(WorkflowId),
}

/// Kind of a [`ResourceEvent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ResourceEventKind {
    /// The resource was created.
    Created,
    /// The resource was dropped.
    Dropped,
    /// The resource was polled for completion.
    Polled(Poll<()>),
}

/// Event related to a host-managed resource (a task or a timer).
#[derive(Debug)]
#[non_exhaustive]
pub struct ResourceEvent {
    /// Resource ID.
    pub resource_id: ResourceId,
    /// Event kind.
    pub kind: ResourceEventKind,
}

/// Kind of a [`ChannelEvent`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChannelEventKind {
    /// Inbound channel was polled for messages.
    InboundChannelPolled {
        /// Result of a poll, with the message replaced with its byte length.
        result: Poll<Option<usize>>,
    },
    /// Inbound channel closed by the workflow logic.
    InboundChannelClosed(ChannelId),

    /// Outbound channel was polled for readiness.
    OutboundChannelReady {
        /// Result of a poll.
        result: Poll<Result<(), SendError>>,
    },
    /// Message was sent via an outbound channel.
    OutboundMessageSent {
        /// Byte length of the sent message.
        message_len: usize,
    },
    /// Outbound channel was polled for flush.
    OutboundChannelFlushed {
        /// Result of a poll.
        result: Poll<Result<(), SendError>>,
    },
    /// Outbound channel closed by the workflow logic.
    OutboundChannelClosed(ChannelId),
}

/// Event related to an inbound or outbound workflow channel.
#[derive(Debug)]
#[non_exhaustive]
pub struct ChannelEvent {
    /// Event kind.
    pub kind: ChannelEventKind,
    /// Name of the channel.
    pub channel_name: String,
    /// ID of the remote workflow that the channel is attached to, or `None` if the channel
    /// is local.
    pub workflow_id: Option<WorkflowId>,
}

/// Event included into a [`Receipt`].
#[derive(Debug)]
#[non_exhaustive]
pub enum Event {
    /// Event related to a host-managed resource (a task or a timer).
    Resource(ResourceEvent),
    /// Event related to an inbound or outbound channel.
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
#[derive(Debug)]
#[non_exhaustive]
pub struct Execution {
    /// The top-level WASM function getting executed.
    pub function: ExecutedFunction,
    /// Events that have occurred during the execution (in the order of their appearance).
    pub events: Vec<Event>,
}

/// Receipt for executing tasks in a workflow.
#[derive(Debug)]
pub struct Receipt {
    pub(crate) executions: Vec<Execution>,
}

impl Receipt {
    pub(crate) fn new() -> Self {
        Self {
            executions: Vec::new(),
        }
    }

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

    /// Returns the root cause of the workflow execution that corresponds to this receipt, if any.
    pub fn root_cause(&self) -> Option<&WakeUpCause> {
        self.executions.first()?.function.wake_up_cause()
    }
}

#[derive(Debug)]
pub(crate) struct ExtendedTrap {
    trap: Trap,
    panic_info: Option<PanicInfo>,
}

impl ExtendedTrap {
    pub fn new(trap: Trap, panic_info: Option<PanicInfo>) -> Self {
        Self { trap, panic_info }
    }
}

impl fmt::Display for ExtendedTrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(panic_info) = &self.panic_info {
            write!(formatter, "{}; trap info: {}", panic_info, self.trap)
        } else {
            fmt::Display::fmt(&self.trap, formatter)
        }
    }
}

/// Error occurring during workflow execution.
///
/// An error is caused by the executed WASM code [`Trap`]ping, which can be caused by a panic
/// in the workflow logic, or misuse of Tardigrade runtime APIs. (The latter should not happen
/// if properly using the Tardigrade client library.)
#[derive(Debug)]
pub struct ExecutionError {
    trap: Trap,
    panic_info: Option<PanicInfo>,
    receipt: Receipt,
}

impl ExecutionError {
    pub(crate) fn new(trap: ExtendedTrap, receipt: Receipt) -> Self {
        Self {
            trap: trap.trap,
            panic_info: trap.panic_info,
            receipt,
        }
    }

    /// Returns a [`Trap`] that has led to an error.
    pub fn trap(&self) -> &Trap {
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
            write!(formatter, ": {}", panic_info)
        } else {
            write!(formatter, ": {}", self.trap.display_reason())
        }
    }
}

impl error::Error for ExecutionError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.trap)
    }
}

/// Information about a panic in the workflow code.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PanicInfo {
    /// Human-readable panic message.
    pub message: Option<String>,
    /// Location where the panic has occurred.
    pub location: Option<PanicLocation>,
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

/// Location of a panic in the workflow code.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PanicLocation {
    /// Name of the file where a panic has occurred.
    pub filename: String,
    /// Line number in the file.
    pub line: u32,
    /// Column number on the line.
    pub column: u32,
}

impl fmt::Display for PanicLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}:{}", self.filename, self.line, self.column)
    }
}
