//! Types shared between host and client envs.

use chrono::{DateTime, Utc};

use std::{error, fmt, task::Poll};

use crate::{AllocateBytes, FromWasmError, IntoWasm, TryFromWasm};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelKind {
    Inbound,
    Outbound,
}

impl fmt::Display for ChannelKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        })
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ChannelErrorKind {
    /// Channel was not registered in the workflow interface.
    Unknown,
    /// An inbound channel was already acquired by the workflow.
    AlreadyAcquired,
}

impl fmt::Display for ChannelErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unknown => "channel was not registered in the workflow interface",
            Self::AlreadyAcquired => "inbound channel was already acquired",
        })
    }
}

impl ChannelErrorKind {
    #[doc(hidden)]
    pub fn for_channel(
        self,
        channel_kind: ChannelKind,
        channel_name: impl Into<String>,
    ) -> ChannelError {
        ChannelError {
            kind: self,
            channel_name: channel_name.into(),
            channel_kind,
        }
    }
}

#[derive(Debug)]
pub struct ChannelError {
    kind: ChannelErrorKind,
    channel_name: String,
    channel_kind: ChannelKind,
}

impl ChannelError {
    fn fmt_channel(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} channel `{}`",
            self.channel_kind, self.channel_name
        )
    }
}

impl fmt::Display for ChannelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            ChannelErrorKind::Unknown => {
                formatter.write_str("unknown ")?;
                self.fmt_channel(formatter)
            }
            ChannelErrorKind::AlreadyAcquired => {
                self.fmt_channel(formatter)?;
                formatter.write_str(" already acquired")
            }
        }
    }
}

impl error::Error for ChannelError {}

impl ChannelError {
    pub fn kind(&self) -> &ChannelErrorKind {
        &self.kind
    }

    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }

    pub fn channel_kind(&self) -> ChannelKind {
        self.channel_kind
    }
}

pub type RawChannelResult = Result<(), ChannelErrorKind>;

impl IntoWasm for RawChannelResult {
    type Abi = i32;

    fn into_wasm<A: AllocateBytes>(self, _: &mut A) -> Result<Self::Abi, A::Error> {
        Ok(match self {
            Ok(()) => 0,
            Err(ChannelErrorKind::Unknown) => -1,
            Err(ChannelErrorKind::AlreadyAcquired) => -2,
        })
    }

    unsafe fn from_abi_in_wasm(abi: i32) -> Self {
        Err(match abi {
            0 => return Ok(()),
            -1 => ChannelErrorKind::Unknown,
            -2 => ChannelErrorKind::AlreadyAcquired,
            _ => panic!("Unexpected ABI value"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum TimerKind {
    Duration = 0,
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

#[derive(Debug, Clone, Copy)]
pub struct TimerDefinition {
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub enum TracedFutureUpdate {
    Polling,
    Polled(Poll<()>),
    Dropped,
}

impl TryFromWasm for TracedFutureUpdate {
    type Abi = i32;

    fn into_abi_in_wasm(self) -> Self::Abi {
        match self {
            Self::Polling => 0,
            Self::Polled(Poll::Ready(())) => 1,
            Self::Dropped => 2,
            Self::Polled(Poll::Pending) => -1,
        }
    }

    fn try_from_wasm(abi: i32) -> Result<Self, FromWasmError> {
        Ok(match abi {
            0 => Self::Polling,
            1 => Self::Polled(Poll::Ready(())),
            2 => Self::Dropped,
            -1 => Self::Polled(Poll::Pending),
            _ => return Err(FromWasmError::new("invalid value for traced future event")),
        })
    }
}
