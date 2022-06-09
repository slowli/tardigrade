//! Types shared between host and client envs.

use chrono::{DateTime, Utc};

use std::{error, fmt, task::Poll};

use crate::{FromAbi, IntoAbiOnStack};

/// Result of polling a receiver end of a channel.
pub type PollMessage = Poll<Option<Vec<u8>>>;
/// Result of polling a task.
pub type PollTask = Poll<Result<(), JoinError>>;

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

impl IntoAbiOnStack for RawChannelResult {
    type Output = i32;

    fn into_abi(self) -> Self::Output {
        match self {
            Ok(()) => 0,
            Err(ChannelErrorKind::Unknown) => -1,
            Err(ChannelErrorKind::AlreadyAcquired) => -2,
        }
    }
}

impl FromAbi for RawChannelResult {
    unsafe fn from_abi(abi: i32) -> Self {
        Err(match abi {
            0 => return Ok(()),
            -1 => ChannelErrorKind::Unknown,
            -2 => ChannelErrorKind::AlreadyAcquired,
            _ => panic!("Unexpected ABI value"),
        })
    }
}

#[derive(Debug)]
pub struct TimeoutKindError {
    invalid_value: i32,
}

impl fmt::Display for TimeoutKindError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid value for timer kind: {}",
            self.invalid_value
        )
    }
}

impl error::Error for TimeoutKindError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum TimerKind {
    Duration = 0,
    Instant = 1,
}

impl TryFrom<i32> for TimerKind {
    type Error = TimeoutKindError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Duration),
            1 => Ok(Self::Instant),
            _ => Err(TimeoutKindError {
                invalid_value: value,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TimerDefinition {
    pub expires_at: DateTime<Utc>,
}
