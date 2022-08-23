//! Types shared between host and client envs.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use std::{error, fmt, task::Poll};

/// Result of polling a receiver end of a channel.
pub type PollMessage = Poll<Option<Vec<u8>>>;
/// Result of polling a workflow task.
pub type PollTask = Poll<Result<(), JoinError>>;

/// ID of a [`Waker`](std::task::Waker) defined by a workflow.
pub type WakerId = u64;
/// ID of a workflow task.
pub type TaskId = u64;
/// ID of a workflow timer.
pub type TimerId = u64;
/// ID of a (traced) future defined by a workflow.
pub type FutureId = u64;

/// Errors that can occur when joining a task (i.e., waiting for its completion).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinError {
    /// The task was aborted.
    Aborted,
    /// A trap has occurred during task execution.
    Trapped,
}

impl fmt::Display for JoinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Aborted => formatter.write_str("task was aborted"),
            Self::Trapped => formatter.write_str("trap has occurred during task execution"),
        }
    }
}

impl error::Error for JoinError {}

/// Definition of a timer used by a workflow.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimerDefinition {
    /// Expiration timestamp of the timer.
    pub expires_at: DateTime<Utc>,
}

/// Errors that can occur when sending a message over a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SendError {
    /// The channel is full.
    Full,
    /// The channel is closed.
    Closed,
}

impl fmt::Display for SendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => formatter.write_str("channel is full"),
            Self::Closed => formatter.write_str("channel is closed"),
        }
    }
}

impl error::Error for SendError {}

/// Errors that can occur when spawning a workflow.
// TODO: generalize as a trap?
#[derive(Debug)]
pub struct SpawnError {}

impl fmt::Display for SpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("trap has occurred during workflow instantiation")
    }
}

impl error::Error for SpawnError {}
