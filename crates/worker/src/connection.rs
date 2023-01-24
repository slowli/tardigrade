//! Connection traits.

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

use std::error;

use tardigrade_shared::ChannelId;

/// Stream of messages together with their indices.
pub type MessageStream<Err> = BoxStream<'static, Result<(usize, Vec<u8>), Err>>;

/// Connection to the worker storage.
#[async_trait]
pub trait WorkerConnection: Send + Sync {
    /// Errors that can occur when applying changes to the storage.
    type Error: error::Error + Send + Sync + 'static;
    /// Read-write view of the storage.
    type View<'a>: WorkerStorageView<Error = Self::Error>
    where
        Self: 'a;

    /// Creates a new storage view.
    async fn view(&self) -> Self::View<'_>;

    /// Streams messages from a specific channel starting with the specified index.
    fn stream_messages(
        &self,
        channel_id: ChannelId,
        start_idx: usize,
    ) -> MessageStream<Self::Error>;
}

/// Read-write view of the worker storage.
///
/// This view is not required to be transactional, although it can be if the underlying storage
/// supports this.
#[async_trait]
pub trait WorkerStorageView: Send {
    /// Errors that can occur when applying changes to the storage.
    type Error: error::Error + Send + Sync + 'static;

    /// Gets or creates a [`WorkerRecord`] by the (globally unique) worker name.
    async fn get_or_create_worker(&mut self, name: &str) -> Result<WorkerRecord, Self::Error>;

    /// Updates the cursor value of a worker with the specified ID.
    async fn update_worker_cursor(
        &mut self,
        worker_id: u64,
        cursor: usize,
    ) -> Result<(), Self::Error>;

    /// Sends the message over a channel.
    async fn push_message(
        &mut self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), Self::Error>;

    /// Commits changes in this view to the storage.
    ///
    /// If this view is not transactional, the changes in the view are expected to take
    /// effect immediately, and this method can be no-op.
    async fn commit(self);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRecord {
    /// Primary key of the worker record.
    pub id: u64,
    /// Globally unique worker name, such as `tardigrade.v0.Timer`.
    pub name: String,
    /// Index of the inbound channel used by the worker.
    pub inbound_channel_id: ChannelId,
    /// The current cursor position of the worker.
    pub cursor: usize,
}
