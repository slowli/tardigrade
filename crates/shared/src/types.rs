//! Types shared between host and client envs.

use chrono::{DateTime, Utc};

use std::{error, fmt, task::Poll};

use crate::abi::{FromWasmError, TryFromWasm};

/// Result of polling a receiver end of a channel.
pub type PollMessage = Poll<Option<Vec<u8>>>;
/// Result of polling a task.
pub type PollTask = Poll<Result<(), JoinError>>;

/// ID of a waker.
pub type WakerId = u64;
/// ID of a task.
pub type TaskId = u64;
/// ID of a timer.
pub type TimerId = u64;
/// ID of a (traced) future.
pub type FutureId = u64;

/// Errors that can occur when joining a task.
#[derive(Debug, Clone)]
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

/// Kind of a timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum TimerKind {
    /// Relative timer: one that expires after the specified duration.
    Duration = 0,
    /// Absolute timer: one that expires at the specified instant.
    Instant = 1,
}

impl TryFromWasm for TimerKind {
    type Abi = i32;

    fn into_abi_in_wasm(self) -> Self::Abi {
        self as i32
    }

    fn try_from_wasm(value: i32) -> Result<Self, FromWasmError> {
        match value {
            0 => Ok(Self::Duration),
            1 => Ok(Self::Instant),
            _ => Err(FromWasmError::new("invalid `TimerKind` value")),
        }
    }
}

/// Timer definition.
#[derive(Debug, Clone, Copy)]
pub struct TimerDefinition {
    /// Expiration timestamp.
    pub expires_at: DateTime<Utc>,
}
