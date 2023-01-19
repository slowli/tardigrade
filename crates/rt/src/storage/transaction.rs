//! `StorageTransaction` used as storage.

use async_trait::async_trait;
use futures::lock::{Mutex, MutexGuard};
use tracing_tunnel::PersistedMetadata;

use std::sync::atomic::{AtomicBool, Ordering};

use super::{
    ActiveWorkflowState, ChannelRecord, ModuleRecord, Storage, StorageTransaction, WorkflowRecord,
    WorkflowSelectionCriteria, WorkflowState, WorkflowWaker, WorkflowWakerRecord, WriteChannels,
    WriteModules, WriteWorkflowWakers, WriteWorkflows,
};
use tardigrade::{ChannelId, WakerId, WorkflowId};

/// [Transaction](StorageTransaction) acting as a [`Storage`]. This allows atomically
/// committing multiple operations which would normally require multiple transactions
/// (e.g., pushing messages to multiple channels using channel [handles](crate::handle)),
/// or committing operations with a delay (e.g., committing a workflow after it was successfully
/// initialized).
///
/// # Implementation
///
/// Implemented using an async-aware mutex. (Motivated by the necessity to operate on a shared
/// reference to the storage in many cases; e.g., channel / workflow handles mutating the storage
/// would be impossible without it.) This has the following consequences:
///
/// - Although this is not reflected in the [`Storage`] signature, inner transactions
///   are in fact exclusive. There can be no more than one alive inner transaction
///   at any point. Creating multiple transactions thus may lead to deadlocks.
/// - Inner transaction isolation is partial. If an inner transaction was rolled back, its changes
///   may still be observed by the further inner transactions. However, there will be no way
///   to commit the original transaction; [`Self::into_inner()`] will return `None`.
#[derive(Debug)]
pub struct TransactionAsStorage<T> {
    // TODO: is this passable, or must `RwLock` be used here?
    inner: Mutex<T>,
    has_rollbacks: AtomicBool,
}

impl<T: StorageTransaction> From<T> for TransactionAsStorage<T> {
    fn from(transaction: T) -> Self {
        Self {
            inner: Mutex::new(transaction),
            has_rollbacks: AtomicBool::new(false),
        }
    }
}

impl<T: StorageTransaction> TransactionAsStorage<T> {
    /// Returns `true` iff any of the inner transactions based on this transaction was not
    pub fn has_rollbacks(&self) -> bool {
        self.has_rollbacks.load(Ordering::SeqCst)
    }

    /// Unwraps the contained transaction provided that no inner transactions were rolled back.
    /// If any inner transaction is rolled back, this method will return `None`; thus, there
    /// will be no other choice other than to roll back the contained transaction.
    pub fn into_inner(mut self) -> Option<T> {
        if *self.has_rollbacks.get_mut() {
            None
        } else {
            Some(self.inner.into_inner())
        }
    }
}

/// Transaction on top of [`TransactionAsStorage`].
#[derive(Debug)]
pub struct TransactionLock<'a, T> {
    inner: MutexGuard<'a, T>,
    was_rolled_back: bool,
    parent_has_rollbacks: &'a AtomicBool,
}

/// Readonly transaction on top of [`TransactionAsStorage`].
#[derive(Debug)]
pub struct TransactionReadLock<'a, T> {
    inner: MutexGuard<'a, T>,
}

#[async_trait]
impl<T: StorageTransaction> Storage for TransactionAsStorage<T> {
    type Transaction<'a>
    where
        Self: 'a,
    = TransactionLock<'a, T>;

    type ReadonlyTransaction<'a>
    where
        Self: 'a,
    = TransactionReadLock<'a, T>;

    async fn transaction(&self) -> Self::Transaction<'_> {
        let lock = self.inner.lock().await;
        TransactionLock {
            inner: lock,
            was_rolled_back: true, // set to `false` in `Self::commit()`
            parent_has_rollbacks: &self.has_rollbacks,
        }
    }

    async fn readonly_transaction(&self) -> Self::ReadonlyTransaction<'_> {
        let lock = self.inner.lock().await;
        TransactionReadLock { inner: lock }
    }
}

delegate_read_traits!(TransactionLock<'_, T> { inner: T });
delegate_read_traits!(TransactionReadLock<'_, T> { inner: T });

impl<T> Drop for TransactionLock<'_, T> {
    fn drop(&mut self) {
        if self.was_rolled_back {
            self.parent_has_rollbacks.fetch_or(true, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl<T: StorageTransaction> WriteModules for TransactionLock<'_, T> {
    async fn insert_module(&mut self, module: ModuleRecord) {
        self.inner.insert_module(module).await;
    }

    async fn update_tracing_metadata(&mut self, module_id: &str, metadata: PersistedMetadata) {
        self.inner
            .update_tracing_metadata(module_id, metadata)
            .await;
    }
}

#[async_trait]
impl<T: StorageTransaction> WriteChannels for TransactionLock<'_, T> {
    async fn allocate_channel_id(&mut self) -> ChannelId {
        self.inner.allocate_channel_id().await
    }

    async fn insert_channel(&mut self, id: ChannelId, state: ChannelRecord) {
        self.inner.insert_channel(id, state).await;
    }

    async fn manipulate_channel<F: FnOnce(&mut ChannelRecord) + Send>(
        &mut self,
        id: ChannelId,
        action: F,
    ) -> ChannelRecord {
        self.inner.manipulate_channel(id, action).await
    }

    async fn push_messages(&mut self, id: ChannelId, messages: Vec<Vec<u8>>) {
        self.inner.push_messages(id, messages).await;
    }

    async fn truncate_channel(&mut self, id: ChannelId, min_index: usize) {
        self.inner.truncate_channel(id, min_index).await;
    }
}

#[async_trait]
impl<T: StorageTransaction> WriteWorkflows for TransactionLock<'_, T> {
    async fn allocate_workflow_id(&mut self) -> WorkflowId {
        self.inner.allocate_workflow_id().await
    }

    async fn insert_workflow(&mut self, record: WorkflowRecord) {
        self.inner.insert_workflow(record).await;
    }

    async fn workflow_for_update(&mut self, id: WorkflowId) -> Option<WorkflowRecord> {
        self.inner.workflow_for_update(id).await
    }

    async fn update_workflow(&mut self, id: WorkflowId, state: WorkflowState) {
        self.inner.update_workflow(id, state).await;
    }

    async fn workflow_with_wakers_for_update(
        &mut self,
    ) -> Option<WorkflowRecord<ActiveWorkflowState>> {
        self.inner.workflow_with_wakers_for_update().await
    }

    async fn workflow_with_consumable_channel_for_update(
        &mut self,
    ) -> Option<WorkflowRecord<ActiveWorkflowState>> {
        self.inner
            .workflow_with_consumable_channel_for_update()
            .await
    }
}

#[async_trait]
impl<T: StorageTransaction> WriteWorkflowWakers for TransactionLock<'_, T> {
    async fn insert_waker(&mut self, workflow_id: WorkflowId, waker: WorkflowWaker) {
        self.inner.insert_waker(workflow_id, waker).await;
    }

    async fn insert_waker_for_matching_workflows(
        &mut self,
        criteria: WorkflowSelectionCriteria,
        waker: WorkflowWaker,
    ) {
        self.inner
            .insert_waker_for_matching_workflows(criteria, waker)
            .await;
    }

    async fn wakers_for_workflow(&self, workflow_id: WorkflowId) -> Vec<WorkflowWakerRecord> {
        self.inner.wakers_for_workflow(workflow_id).await
    }

    async fn delete_wakers(&mut self, workflow_id: WorkflowId, waker_ids: &[WakerId]) {
        self.inner.delete_wakers(workflow_id, waker_ids).await;
    }
}

#[async_trait]
impl<T: StorageTransaction> StorageTransaction for TransactionLock<'_, T> {
    async fn commit(mut self) {
        self.was_rolled_back = false;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::storage::{LocalStorage, ReadChannels};

    fn create_channel_record() -> ChannelRecord {
        ChannelRecord {
            receiver_workflow_id: Some(1),
            sender_workflow_ids: HashSet::new(),
            has_external_sender: true,
            is_closed: false,
            received_messages: 0,
        }
    }

    #[async_std::test]
    async fn transaction_lock_basics() {
        let storage = LocalStorage::default();
        let mut transaction = storage.transaction().await;
        transaction.insert_channel(1, create_channel_record()).await;

        let transaction = TransactionAsStorage::from(transaction);
        let mut inner_tx = transaction.transaction().await;
        let channel = inner_tx.channel(1).await.unwrap();
        assert_eq!(channel.receiver_workflow_id, Some(1));
        inner_tx.insert_channel(2, create_channel_record()).await;
        inner_tx.commit().await;

        let transaction = transaction.into_inner().unwrap();
        transaction.commit().await;

        let transaction = storage.readonly_transaction().await;
        let channel = transaction.channel(1).await.unwrap();
        assert_eq!(channel.receiver_workflow_id, Some(1));
        let channel = transaction.channel(2).await.unwrap();
        assert_eq!(channel.receiver_workflow_id, Some(1));
    }

    #[async_std::test]
    async fn rolling_back_inner_transaction() {
        let storage = LocalStorage::default();
        let transaction = storage.transaction().await;

        let transaction = TransactionAsStorage::from(transaction);
        let mut inner_tx = transaction.transaction().await;
        inner_tx.insert_channel(2, create_channel_record()).await;
        drop(inner_tx); // rolls back the inner transaction

        assert!(transaction.into_inner().is_none());
    }
}
