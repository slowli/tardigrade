//! Async, transactional storage abstraction for storing workflows and channel state.
//!
//! # Overview
//!
//! The core trait of this module is [`Storage`], which produces storage transactions:
//!
//! - [`ReadonlyStorageTransaction`] for reading data
//! - [`StorageTransaction`] for reading and writing data
//!
//! Transactions must have at least snapshot isolation.
//!
//! The data operated by transactions is as follows:
//!
//! | Entity type | Read trait | Write trait |
//! |-------------|------------|-------------|
//! | [`ModuleRecord`] | [`ReadModules`] | [`WriteModules`] |
//! | [`ChannelRecord`] | [`ReadChannels`] | [`WriteChannels`] |
//! | [`WorkflowRecord`] | [`ReadWorkflows`] | [`WriteWorkflows`] |
//! | [`WorkflowWakerRecord`] | n/a | [`WriteWorkflowWakers`] |
//!
//! The `ReadonlyStorageTransaction` trait encompasses all read traits, and `StorageTransaction`
//! encompasses both read and write traits. To simplify readonly transaction implementation without
//! sacrificing type safety, there is [`Readonly`] wrapper.
//!
//! In terms of domain-driven development, modules, channels and workflows are independent
//! aggregate roots, while workflow wakers are tied to a workflow.
//! `_Record` types do not precisely correspond to the relational data model, but can be
//! straightforwardly mapped onto one. In particular, [`WorkflowRecord`] has the workflow state
//! as a type param. The state is modelled via enum dispatch on the [`WorkflowState`] enum and
//! its variants (e.g., [`ActiveWorkflowState`] for active workflows).
//!
//! Besides the [`Storage`] trait, the module provides its local in-memory implementation:
//! [`LocalStorage`]. It has [`LocalTransaction`] transactions and provides
//! [`LocalStorageSnapshot`] as a way to get (de)serializable snapshot of the storage.
//!
//! # See also
//!
//! - [`StorageRef`](crate::handle::StorageRef), a wrapper around a `Storage` that allows
//!   accessing [workflow](crate::handle::WorkflowHandle) and channel [sender] / [receiver]
//!   handles
//!
//! [sender]: crate::handle::MessageSender
//! [receiver]: crate::handle::MessageReceiver

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{channel::mpsc, future, stream::BoxStream, FutureExt, StreamExt};
use tracing_tunnel::PersistedMetadata;

use std::{convert::Infallible, ops, sync::Arc};

#[macro_use]
mod macros;
pub(crate) mod helper;
mod local;
mod records;
mod stream;
mod transaction;

pub use self::{
    local::{
        LocalReadonlyTransaction, LocalStorage, LocalStorageSnapshot, LocalTransaction,
        ModuleRecordMut,
    },
    records::{
        ActiveWorkflowState, ChannelRecord, CompletedWorkflowState, DefinitionRecord,
        ErroneousMessageRef, ErroredWorkflowState, MessageError, ModuleRecord, WorkflowRecord,
        WorkflowSelectionCriteria, WorkflowState, WorkflowWaker, WorkflowWakerRecord,
    },
    stream::{
        CommitStream, MessageEvent, MessageOrEof, StreamMessages, Streaming, StreamingTransaction,
    },
    transaction::{TransactionAsStorage, TransactionLock, TransactionReadLock},
};

use tardigrade::{ChannelId, WakerId, WorkflowId};
use tardigrade_worker::{MessageStream, WorkerStorageConnection, WorkerStoragePool};

/// Async, transactional storage for workflows and workflow channels.
///
/// A storage is required to instantiate a [`Runtime`](crate::runtime::Runtime).
#[async_trait]
pub trait Storage: Send + Sync {
    /// Read/write transaction for the storage. See [`StorageTransaction`] for required
    /// transaction semantics.
    type Transaction<'a>: 'a + StorageTransaction
    where
        Self: 'a;
    /// Readonly transaction for the storage.
    type ReadonlyTransaction<'a>: 'a + ReadonlyStorageTransaction
    where
        Self: 'a;

    /// Creates a new read/write transaction.
    async fn transaction(&self) -> Self::Transaction<'_>;
    /// Creates a new readonly transaction.
    async fn readonly_transaction(&self) -> Self::ReadonlyTransaction<'_>;
}

#[async_trait]
impl<S: Storage + ?Sized> Storage for &S {
    type Transaction<'a>
    where
        Self: 'a,
    = S::Transaction<'a>;
    type ReadonlyTransaction<'a>
    where
        Self: 'a,
    = S::ReadonlyTransaction<'a>;

    #[inline]
    async fn transaction(&self) -> Self::Transaction<'_> {
        (**self).transaction().await
    }

    #[inline]
    async fn readonly_transaction(&self) -> Self::ReadonlyTransaction<'_> {
        (**self).readonly_transaction().await
    }
}

#[async_trait]
impl<S: Storage + ?Sized> Storage for Arc<S> {
    type Transaction<'a>
    where
        S: 'a,
    = S::Transaction<'a>;
    type ReadonlyTransaction<'a>
    where
        S: 'a,
    = S::ReadonlyTransaction<'a>;

    #[inline]
    async fn transaction(&self) -> Self::Transaction<'_> {
        (**self).transaction().await
    }

    #[inline]
    async fn readonly_transaction(&self) -> Self::ReadonlyTransaction<'_> {
        (**self).readonly_transaction().await
    }
}

/// [`Storage`] transaction with readonly access to the storage.
pub trait ReadonlyStorageTransaction:
    Send + Sync + ReadModules + ReadChannels + ReadWorkflows
{
}

impl<T> ReadonlyStorageTransaction for T where
    T: Send + Sync + ReadModules + ReadChannels + ReadWorkflows
{
}

/// [`Storage`] transaction with read/write access to the storage.
///
/// Transactions must satisfy the ACID semantics. In particular, they must apply atomically
/// and be isolated (i.e., not visible to other transactions until committed).
///
/// [`Self::commit()`] must be called for before the transaction is dropped.
/// Explicit transaction rollback support is not required; all transactions instantiated
/// by a [`Runtime`] are guaranteed to eventually be committed (save for corner cases,
/// e.g., panicking when the transaction is active).
/// If rollback *is* supported by the storage, it is assumed to be the default behavior
/// on transaction drop. It would be an error to commit transactions on drop,
/// since this would produce errors in the aforementioned corner cases.
///
/// [`Runtime`]: crate::runtime::Runtime
#[must_use = "transactions must be committed to take effect"]
#[async_trait]
pub trait StorageTransaction:
    Send
    + Sync
    + WriteModules
    + WriteChannels
    + WriteWorkflows
    + WriteWorkflowWakers
    + WorkerStorageConnection<Error = Infallible>
{
    /// Commits this transaction to the storage. This method must be called
    /// to (atomically) apply transaction changes.
    async fn commit(self);
}

/// Allows reading stored information about [`WorkflowModule`]s.
///
/// [`WorkflowModule`]: crate::engine::WorkflowModule
#[async_trait]
pub trait ReadModules {
    /// Retrieves a module with the specified ID.
    async fn module(&self, id: &str) -> Option<ModuleRecord>;

    /// Streams all modules.
    fn modules(&self) -> BoxStream<'_, ModuleRecord>;
}

/// Allows modifying stored information about [`WorkflowModule`]s.
///
/// [`WorkflowModule`]: crate::engine::WorkflowModule
#[async_trait]
pub trait WriteModules: ReadModules {
    /// Inserts the module into the storage.
    async fn insert_module(&mut self, module: ModuleRecord);

    /// Updates tracing metadata for the module.
    ///
    /// It is acceptable to merge the provided metadata JSON to the current one to prevent edit
    /// conflicts.
    async fn update_tracing_metadata(&mut self, module_id: &str, metadata: PersistedMetadata);
}

/// Allows reading stored information about workflow channels.
#[async_trait]
pub trait ReadChannels {
    /// Retrieves information about the channel with the specified ID.
    async fn channel(&self, id: ChannelId) -> Option<ChannelRecord>;

    /// Gets a message with the specified 0-based index from the specified channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be retrieved.
    async fn channel_message(&self, id: ChannelId, index: u64) -> Result<Vec<u8>, MessageError>;

    /// Gets messages with indices in the specified range from the specified channel.
    fn channel_messages(
        &self,
        id: ChannelId,
        indices: ops::RangeInclusive<u64>,
    ) -> BoxStream<'_, (u64, Vec<u8>)>;
}

/// Allows modifying stored information about channels.
#[async_trait]
pub trait WriteChannels: ReadChannels {
    /// Allocates a new unique ID for a channel. The allocated ID must be positive; 0th channel ID
    /// is reserved for a closed channel.
    async fn allocate_channel_id(&mut self) -> ChannelId;

    /// Creates a new channel with the provided `state`. If the channel with the specified ID
    /// already exists, does nothing (i.e., the channel state is not updated in any way).
    async fn insert_channel(&mut self, id: ChannelId, state: ChannelRecord);

    /// Changes the channel state and selects the updated state.
    async fn manipulate_channel<F: FnOnce(&mut ChannelRecord) + Send>(
        &mut self,
        id: ChannelId,
        action: F,
    ) -> ChannelRecord;

    /// Pushes one or more messages into the channel.
    async fn push_messages(&mut self, id: ChannelId, messages: Vec<Vec<u8>>);

    /// Truncates the channel so that `min_index` is the minimum retained index.
    async fn truncate_channel(&mut self, id: ChannelId, min_index: u64);
}

/// Allows reading information about workflows.
#[async_trait]
pub trait ReadWorkflows {
    /// Returns the number of active workflows.
    async fn count_active_workflows(&self) -> u64;
    /// Retrieves a snapshot of the workflow with the specified ID.
    async fn workflow(&self, id: WorkflowId) -> Option<WorkflowRecord>;

    /// Finds the nearest timer expiration in all active workflows.
    async fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>>;
}

/// Allows modifying stored information about workflows.
#[async_trait]
pub trait WriteWorkflows: ReadWorkflows {
    /// Allocates a new unique ID for a workflow. The allocated ID must be positive; 0th ID
    /// is reserved.
    async fn allocate_workflow_id(&mut self) -> WorkflowId;
    /// Inserts a new workflow into the storage.
    async fn insert_workflow(&mut self, record: WorkflowRecord);
    /// Returns a workflow record for the specified ID for update.
    async fn workflow_for_update(&mut self, id: WorkflowId) -> Option<WorkflowRecord>;

    /// Updates the state of a workflow with the specified ID. This method must increase
    /// `execution_count` in the [`WorkflowRecord`] by one.
    async fn update_workflow(&mut self, id: WorkflowId, state: WorkflowState);

    /// Finds an active workflow with wakers and selects it for update.
    async fn workflow_with_wakers_for_update(
        &mut self,
    ) -> Option<WorkflowRecord<ActiveWorkflowState>>;

    /// Finds an active workflow with an inbound channel that has a message or EOF marker
    /// consumable by the workflow, and selects the workflow for update.
    async fn workflow_with_consumable_channel_for_update(
        &mut self,
    ) -> Option<WorkflowRecord<ActiveWorkflowState>>;
}

/// Allows modifying stored information about [`WorkflowWaker`].
#[async_trait]
pub trait WriteWorkflowWakers {
    /// Inserts a workflow waker into the queue.
    async fn insert_waker(&mut self, workflow_id: WorkflowId, waker: WorkflowWaker);

    /// Inserts a workflow waker for all non-completed workflows matching the specified criteria.
    async fn insert_waker_for_matching_workflows(
        &mut self,
        criteria: WorkflowSelectionCriteria,
        waker: WorkflowWaker,
    );

    /// Selects all wakers for the specified workflow.
    async fn wakers_for_workflow(&self, workflow_id: WorkflowId) -> Vec<WorkflowWakerRecord>;

    /// Deletes wakers with the specified IDs for the given workflow ID.
    async fn delete_wakers(&mut self, workflow_id: WorkflowId, waker_ids: &[WakerId]);
}

/// Wrapper for [`StorageTransaction`] implementations that allows safely using them
/// as readonly transactions.
#[derive(Debug)]
pub struct Readonly<T> {
    inner: T,
}

impl<T: ReadonlyStorageTransaction> From<T> for Readonly<T> {
    fn from(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: ReadonlyStorageTransaction> AsRef<T> for Readonly<T> {
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

delegate_read_traits!(Readonly<T> { inner: T });

/// Wrapper around a [`Storage`] implementing [`WorkerConnection`] trait.
/// This can be used for in-process workers.
#[derive(Debug, Clone)]
pub struct InProcessConnection<S>(pub S);

#[async_trait]
impl<S> WorkerStoragePool for InProcessConnection<S>
where
    S: StreamMessages + 'static,
    for<'a> S::Transaction<'a>: WorkerStorageConnection<Error = Infallible>,
{
    type Error = Infallible;
    type Connection<'a> = S::Transaction<'a> where Self: 'a;

    async fn connect(&self) -> Self::Connection<'_> {
        self.0.transaction().await
    }

    fn stream_messages(&self, channel_id: ChannelId, start_idx: u64) -> MessageStream<Self::Error> {
        let (sx, rx) = mpsc::channel(16);
        self.0
            .stream_messages(channel_id, start_idx, sx)
            .map(|()| rx)
            .flatten_stream()
            .filter_map(|message| {
                future::ready(match message {
                    MessageOrEof::Message(idx, payload) => Some(Ok((idx, payload))),
                    MessageOrEof::Eof => None,
                })
            })
            .boxed()
    }
}
