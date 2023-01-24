//! Common types used by multiple crates.

use serde::{Deserialize, Serialize};

/// ID of a [`Waker`](std::task::Waker) defined by a workflow.
pub type WakerId = u64;
/// ID of a workflow task.
pub type TaskId = u64;
/// ID of a workflow timer.
pub type TimerId = u64;
/// ID of a workflow.
pub type WorkflowId = u64;
/// ID of a channel.
pub type ChannelId = u64;

/// Container for a [request](Requests) that can also be used to cancel the pending request
/// from the client side.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Request<T> {
    /// New request.
    New {
        /// ID of the response channel.
        #[serde(rename = "rx")]
        response_channel_id: ChannelId,
        /// Identifier of the request unique within the [`Requests`] instance generating
        /// requests.
        #[serde(rename = "@id")]
        id: u64,
        /// Payload of the request.
        data: T,
    },
    /// Request cancellation.
    Cancel {
        /// ID of the response channel.
        #[serde(rename = "rx")]
        response_channel_id: ChannelId,
        /// Identifier of a [previously created](Self::New) request.
        #[serde(rename = "@id")]
        id: u64,
    },
}

/// Container for the response to a [`Request`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Response<T> {
    /// Identifier of the request that this response corresponds to.
    #[serde(rename = "@id")]
    pub id: u64,
    /// Wrapped response payload.
    pub data: T,
}
