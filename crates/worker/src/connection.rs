//! Connection traits.

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

use std::error;

use tardigrade_shared::ChannelId;

/// Stream of messages together with their indices.
pub type MessageStream<Err> = BoxStream<'static, Result<(usize, Vec<u8>), Err>>;

/// Connection pool for the worker storage.
#[async_trait]
pub trait WorkerStoragePool: Send + Sync + 'static {
    /// Errors that can occur when applying changes to the storage.
    type Error: error::Error + Send + Sync + 'static;
    /// Read-write connection to the storage.
    type Connection<'a>: WorkerStorageConnection<Error = Self::Error>
    where
        Self: 'a;

    /// Connects to the worker storage.
    async fn connect(&self) -> Self::Connection<'_>;

    /// Streams messages from a specific channel starting with the specified index.
    fn stream_messages(
        &self,
        channel_id: ChannelId,
        start_idx: usize,
    ) -> MessageStream<Self::Error>;
}

/// Read-write connection to the worker storage.
///
/// This view is not required to be transactional, although it can be if the underlying storage
/// supports this.
#[async_trait]
pub trait WorkerStorageConnection: Send {
    /// Errors that can occur when applying changes to the storage.
    type Error: error::Error + Send + Sync + 'static;

    /// Gets a [`WorkerRecord`] by the (globally unique) worker name.
    async fn worker(&mut self, name: &str) -> Result<Option<WorkerRecord>, Self::Error>;

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

    /// Releases the connection back to the pool.
    ///
    /// - If this connection is transactional, commits changes to the storage.
    /// - If this connection is not transactional, the changes are expected to take
    ///   effect immediately, and this method can be no-op.
    async fn release(self);
}

/// Storage record for a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRecord {
    /// Primary key of this worker record.
    pub id: u64,
    /// Globally unique worker name, such as `tardigrade.v0.Timer`.
    pub name: String,
    /// Index of the inbound channel used by the worker.
    pub inbound_channel_id: ChannelId,
    /// The current cursor position of the worker.
    pub cursor: usize,
}
