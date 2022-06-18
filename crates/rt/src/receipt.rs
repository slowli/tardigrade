//! `Receipt`s for workflows.

use wasmtime::Trap;

use std::{error, fmt, ops::Range, task::Poll};

use crate::{TaskId, TimerId, WakerId};

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WakeUpCause {
    /// Woken up by an inbound message.
    InboundMessage {
        channel_name: String,
        message_index: usize,
    },
    /// Woken up by flushing an outbound channel.
    Flush {
        channel_name: String,
        /// Indexes of flushed messages.
        message_indexes: Range<usize>,
    },

    /// Initial task enqueuing after it was spawned.
    Spawned(Box<ExecutedFunction>),
    /// Woken up by an executed function, such as another task (e.g., due to internal channels
    /// or other sync primitives).
    Function(Box<ExecutedFunction>),
    /// Woken up by task completion.
    CompletedTask(TaskId),
    /// Woken up by a timer.
    Timer {
        /// Timer ID.
        id: TimerId,
    },
}

/// Executed WASM function.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ExecutedFunction {
    /// Entry point.
    Entry,
    /// Polling a task.
    #[non_exhaustive]
    Task {
        task_id: TaskId,
        wake_up_cause: WakeUpCause,
        poll_result: Poll<()>,
    },
    /// Waking up a waker.
    #[non_exhaustive]
    Waker {
        waker_id: WakerId,
        wake_up_cause: WakeUpCause,
    },
    /// Dropping a completed task.
    #[non_exhaustive]
    TaskDrop { task_id: TaskId },
}

impl ExecutedFunction {
    fn write_summary(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Entry => formatter.write_str("spawning workflow"),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ResourceId {
    Timer(TimerId),
    Task(TaskId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ResourceEventKind {
    Created,
    Dropped,
    /// The resource was polled for completion.
    Polled(Poll<()>),
}

#[derive(Debug)]
#[non_exhaustive]
pub struct ResourceEvent {
    /// Resource ID.
    pub resource_id: ResourceId,
    /// Event kind.
    pub kind: ResourceEventKind,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ChannelEventKind {
    InboundChannelPolled,
    OutboundChannelReady,
    OutboundChannelFlushed,
}

#[derive(Debug)]
#[non_exhaustive]
pub struct ChannelEvent {
    pub kind: ChannelEventKind,
    pub channel_name: String,
    pub result: Poll<()>,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum Event {
    Resource(ResourceEvent),
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
}

#[derive(Debug)]
#[non_exhaustive]
pub struct Execution {
    pub function: ExecutedFunction,
    pub events: Vec<Event>,
}

#[derive(Debug)]
pub struct Receipt<T = ()> {
    pub(crate) executions: Vec<Execution>,
    output: T,
}

impl Receipt<()> {
    pub(crate) fn new() -> Self {
        Self {
            executions: Vec::new(),
            output: (),
        }
    }
}

impl<T> Receipt<T> {
    pub(crate) fn map<U>(self, map_fn: impl FnOnce(T) -> U) -> Receipt<U> {
        Receipt {
            executions: self.executions,
            output: map_fn(self.output),
        }
    }

    pub fn executions(&self) -> &[Execution] {
        &self.executions
    }

    pub fn output(&self) -> &T {
        &self.output
    }

    pub fn into_inner(self) -> T {
        self.output
    }
}

/// Error occurring during [`Workflow`] execution.
#[derive(Debug)]
pub struct ExecutionError {
    trap: Trap,
    receipt: Receipt,
}

impl ExecutionError {
    pub(crate) fn new(trap: Trap, receipt: Receipt) -> Self {
        Self { trap, receipt }
    }
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "workflow execution failed while ")?;
        let execution = self.receipt.executions.last().unwrap();
        execution.function.write_summary(formatter)?;
        write!(formatter, ": {}", self.trap)
    }
}

impl error::Error for ExecutionError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.trap)
    }
}
