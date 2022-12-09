//! Storage streaming.

use async_trait::async_trait;
use futures::{channel::mpsc, stream, SinkExt};
use tracing_tunnel::PersistedMetadata;

use std::collections::HashMap;

use super::{
    ActiveWorkflowState, ChannelRecord, ModuleRecord, Storage, StorageTransaction, WorkflowRecord,
    WorkflowSelectionCriteria, WorkflowState, WorkflowWaker, WorkflowWakerRecord, WriteChannels,
    WriteModules, WriteWorkflowWakers, WriteWorkflows,
};
use tardigrade::{ChannelId, WakerId, WorkflowId};

/// Event signalling that one or more messages were added to the channel.
#[derive(Debug)]
pub struct MessageEvent {
    /// Channel ID.
    pub channel_id: ChannelId,
    /// Number of new messages inserted into the channel.
    pub count: usize,
}

/// Storage that streams [events](MessageEvent) about new messages when they are committed.
#[derive(Debug)]
pub struct Streaming<S> {
    inner: S,
    events_sink: mpsc::Sender<MessageEvent>,
}

impl<S: Storage> Streaming<S> {
    /// Creates a new streaming storage based on the provided storage and the sink for
    /// [`MessageEvent`]s.
    pub fn new(storage: S, events_sink: mpsc::Sender<MessageEvent>) -> Self {
        Self {
            inner: storage,
            events_sink,
        }
    }
}

#[async_trait]
impl<S: Storage> Storage for Streaming<S> {
    type Transaction<'a>
    where
        S: 'a,
    = StreamingTransaction<S::Transaction<'a>>;
    type ReadonlyTransaction<'a>
    where
        S: 'a,
    = S::ReadonlyTransaction<'a>;

    async fn transaction(&self) -> Self::Transaction<'_> {
        let transaction = self.inner.transaction().await;
        StreamingTransaction {
            inner: transaction,
            events_sink: self.events_sink.clone(),
            new_messages: HashMap::new(),
        }
    }

    async fn readonly_transaction(&self) -> Self::ReadonlyTransaction<'_> {
        self.inner.readonly_transaction().await
    }
}

/// Wrapper around a transaction that streams [`MessageEvent`]s on commit. Used
/// as the transaction type for [`Streaming`] storages.
///
/// Note that committing this transaction may involve dealing with backpressure.
#[derive(Debug)]
pub struct StreamingTransaction<S> {
    inner: S,
    events_sink: mpsc::Sender<MessageEvent>,
    new_messages: HashMap<ChannelId, usize>,
}

delegate_read_traits!(StreamingTransaction { inner });

#[async_trait]
impl<T: StorageTransaction> WriteModules for StreamingTransaction<T> {
    #[inline]
    async fn insert_module(&mut self, module: ModuleRecord) {
        self.inner.insert_module(module).await;
    }

    #[inline]
    async fn update_tracing_metadata(&mut self, module_id: &str, metadata: PersistedMetadata) {
        self.inner
            .update_tracing_metadata(module_id, metadata)
            .await;
    }
}

#[async_trait]
impl<T: StorageTransaction> WriteChannels for StreamingTransaction<T> {
    #[inline]
    async fn allocate_channel_id(&mut self) -> ChannelId {
        self.inner.allocate_channel_id().await
    }

    #[inline]
    async fn insert_channel(&mut self, id: ChannelId, state: ChannelRecord) {
        self.inner.insert_channel(id, state).await;
    }

    #[inline]
    async fn manipulate_channel<F: FnOnce(&mut ChannelRecord) + Send>(
        &mut self,
        id: ChannelId,
        action: F,
    ) -> ChannelRecord {
        self.inner.manipulate_channel(id, action).await
    }

    async fn push_messages(&mut self, id: ChannelId, messages: Vec<Vec<u8>>) {
        let len = messages.len();
        self.inner.push_messages(id, messages).await;
        *self.new_messages.entry(id).or_default() += len;
    }

    #[inline]
    async fn truncate_channel(&mut self, id: ChannelId, min_index: usize) {
        self.inner.truncate_channel(id, min_index).await;
    }
}

#[async_trait]
impl<T: StorageTransaction> WriteWorkflows for StreamingTransaction<T> {
    #[inline]
    async fn allocate_workflow_id(&mut self) -> WorkflowId {
        self.inner.allocate_workflow_id().await
    }

    #[inline]
    async fn insert_workflow(&mut self, state: WorkflowRecord) {
        self.inner.insert_workflow(state).await;
    }

    #[inline]
    async fn workflow_for_update(&mut self, id: WorkflowId) -> Option<WorkflowRecord> {
        self.inner.workflow_for_update(id).await
    }

    #[inline]
    async fn update_workflow(&mut self, id: WorkflowId, state: WorkflowState) {
        self.inner.update_workflow(id, state).await;
    }

    #[inline]
    async fn workflow_with_wakers_for_update(
        &mut self,
    ) -> Option<WorkflowRecord<ActiveWorkflowState>> {
        self.inner.workflow_with_wakers_for_update().await
    }

    #[inline]
    async fn workflow_with_consumable_channel_for_update(
        &self,
    ) -> Option<WorkflowRecord<ActiveWorkflowState>> {
        self.inner
            .workflow_with_consumable_channel_for_update()
            .await
    }
}

#[async_trait]
impl<T: StorageTransaction> WriteWorkflowWakers for StreamingTransaction<T> {
    #[inline]
    async fn insert_waker(&mut self, workflow_id: WorkflowId, waker: WorkflowWaker) {
        self.inner.insert_waker(workflow_id, waker).await;
    }

    #[inline]
    async fn insert_waker_for_matching_workflows(
        &mut self,
        criteria: WorkflowSelectionCriteria,
        waker: WorkflowWaker,
    ) {
        self.inner
            .insert_waker_for_matching_workflows(criteria, waker)
            .await;
    }

    #[inline]
    async fn wakers_for_workflow(&self, workflow_id: WorkflowId) -> Vec<WorkflowWakerRecord> {
        self.inner.wakers_for_workflow(workflow_id).await
    }

    #[inline]
    async fn delete_wakers(&mut self, workflow_id: WorkflowId, waker_ids: &[WakerId]) {
        self.inner.delete_wakers(workflow_id, waker_ids).await;
    }
}

#[async_trait]
impl<T: StorageTransaction> StorageTransaction for StreamingTransaction<T> {
    async fn commit(mut self) {
        self.inner.commit().await;
        let events = self
            .new_messages
            .into_iter()
            .map(|(channel_id, count)| Ok(MessageEvent { channel_id, count }));
        self.events_sink
            .send_all(&mut stream::iter(events))
            .await
            .ok();
    }
}
