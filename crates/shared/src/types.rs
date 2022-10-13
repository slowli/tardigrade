//! Types shared between host and client envs.

use chrono::{DateTime, Utc};
use futures::future::Aborted;
use serde::{Deserialize, Serialize};

use std::task::Poll;

/// Result of polling a receiver end of a channel.
pub type PollMessage = Poll<Option<Vec<u8>>>;
/// Result of polling a workflow task.
pub type PollTask = Poll<Result<(), Aborted>>;

/// ID of a [`Waker`](std::task::Waker) defined by a workflow.
pub type WakerId = u64;
/// ID of a workflow task.
pub type TaskId = u64;
/// ID of a workflow timer.
pub type TimerId = u64;
/// ID of a (traced) future defined by a workflow.
pub type FutureId = u64;
/// ID of a workflow.
pub type WorkflowId = u64;
/// ID of a channel.
pub type ChannelId = u128;

/// Definition of a timer used by a workflow.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimerDefinition {
    /// Expiration timestamp of the timer.
    pub expires_at: DateTime<Utc>,
}
