//! Local in-memory storage implementation.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{
    lock::{Mutex, MutexGuard},
    stream::{self, BoxStream},
    StreamExt,
};
use serde::{Deserialize, Serialize};
use tracing_tunnel::{PersistedMetadata, PersistedSpans};

use std::{
    borrow::Cow,
    cmp,
    collections::{HashMap, HashSet, VecDeque},
    ops,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use super::{
    ChannelRecord, MessageError, ModuleRecord, ReadChannels, ReadModules, ReadWorkflows, Storage,
    StorageTransaction, WorkflowRecord, WorkflowSelectionCriteria, WriteChannels, WriteModules,
    WriteWorkflows,
};
use crate::{utils::Message, PersistedWorkflow};
use tardigrade::{channel::SendError, ChannelId, WorkflowId};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalChannel {
    record: ChannelRecord,
    messages: VecDeque<Message>,
    next_message_idx: usize,
}

impl LocalChannel {
    fn new(record: ChannelRecord) -> Self {
        Self {
            record,
            messages: VecDeque::new(),
            next_message_idx: 0,
        }
    }

    fn contains_index(&self, idx: usize) -> bool {
        let start_idx = self.next_message_idx - self.messages.len();
        (start_idx..self.next_message_idx).contains(&idx)
            || (self.record.is_closed && idx == self.next_message_idx) // EOF marker
    }

    fn truncate(&mut self, min_index: usize) {
        let start_idx = self.next_message_idx - self.messages.len();
        let messages_to_truncate = min_index.saturating_sub(start_idx);
        let messages_to_truncate = cmp::min(messages_to_truncate, self.messages.len());
        self.messages = self.messages.split_off(messages_to_truncate);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Inner {
    modules: HashMap<String, ModuleRecord>,
    channels: HashMap<ChannelId, LocalChannel>,
    workflows: HashMap<WorkflowId, WorkflowRecord>,
}

impl Default for Inner {
    fn default() -> Self {
        let closed_channel = LocalChannel::new(ChannelRecord {
            receiver_workflow_id: None,
            sender_workflow_ids: HashSet::new(),
            has_external_sender: false,
            is_closed: true,
            received_messages: 0,
        });

        Self {
            modules: HashMap::new(),
            channels: HashMap::from_iter([(0, closed_channel)]),
            workflows: HashMap::new(),
        }
    }
}

/// Thin wrapper around [`ModuleRecord`] that allows accessing its fields and changing
/// only module bytes.
#[derive(Debug)]
pub struct ModuleRecordMut<'a> {
    inner: &'a mut ModuleRecord,
}

impl ModuleRecordMut<'_> {
    /// Replaces bytes for this module.
    pub fn set_bytes(&mut self, bytes: impl Into<Arc<[u8]>>) {
        self.inner.bytes = bytes.into();
    }
}

impl ops::Deref for ModuleRecordMut<'_> {
    type Target = ModuleRecord;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

/// Serializable snapshot of a [`LocalStorage`].
#[derive(Debug, Serialize, Deserialize)]
pub struct LocalStorageSnapshot<'a> {
    next_channel_id: u64,
    next_workflow_id: u64,
    inner: Cow<'a, Inner>,
}

impl LocalStorageSnapshot<'_> {
    /// Provides mutable access to all contained modules. This could be useful to decrease
    /// serialized snapshot size, e.g. by removing module bytes or replacing them with
    /// cryptographic hashes.
    pub fn modules_mut(&mut self) -> impl Iterator<Item = ModuleRecordMut<'_>> + '_ {
        let modules = self.inner.to_mut().modules.values_mut();
        modules.map(|inner| ModuleRecordMut { inner })
    }
}

/// Local in-memory [`Storage`].
///
/// `LocalStorage` can be [serialized] by taking a [snapshot](LocalStorageSnapshot)
/// and then restored from it. Since workflow module bytes constitute the largest serialization
/// part, the snapshot provides [a method](LocalStorageSnapshot::replace_module_bytes())
/// to manipulate them, e.g. in order to store module bytes separately.
///
/// [serialized]: https://docs.rs/serde/1/serde
///
/// # Examples
///
/// ```
/// # use async_std::fs;
/// # use std::str;
/// # use tardigrade_rt::{manager::WorkflowManager, storage::{LocalStorage, LocalStorageSnapshot}};
/// # async fn test_wrapper() -> anyhow::Result<()> {
/// let mut storage = LocalStorage::default();
/// // Remove messages consumed by workflows.
/// storage.truncate_workflow_messages();
/// let manager = WorkflowManager::builder(storage).build().await?;
/// // Do something with the manager...
///
/// let mut storage = manager.into_storage();
/// let mut snapshot = storage.snapshot();
/// for mut module in snapshot.modules_mut() {
///     // Save modules to the file system, rather than using the storage.
///     let filename = format!("{}.wasm", module.id);
///     fs::write(&filename, &module.bytes).await?;
///     module.set_bytes(filename.into_bytes());
/// }
/// let serialized = serde_json::to_string_pretty(&snapshot)?;
///
/// // Restoring the storage:
/// let mut snapshot: LocalStorageSnapshot<'_> =
///     serde_json::from_str(&serialized)?;
/// for mut module in snapshot.modules_mut() {
///     let filename = str::from_utf8(&module.bytes)?;
///     module.set_bytes(fs::read(filename).await?);
/// }
/// let storage = LocalStorage::from(snapshot);
/// # Ok(())
/// # }
/// ```
///
/// [(de)serialized]: https://docs.rs/serde/1/serde/
#[derive(Debug)]
pub struct LocalStorage {
    inner: Mutex<Inner>,
    next_channel_id: AtomicU64,
    next_workflow_id: AtomicU64,
    truncate_messages: bool,
}

impl Default for LocalStorage {
    fn default() -> Self {
        Self {
            inner: Mutex::default(),
            next_channel_id: AtomicU64::new(1), // skip the closed channel
            next_workflow_id: AtomicU64::new(0),
            truncate_messages: false,
        }
    }
}

impl LocalStorage {
    /// Returns a snapshot of the storage. The returned snapshot can be (de)serialized with `serde`.
    pub fn snapshot(&mut self) -> LocalStorageSnapshot<'_> {
        LocalStorageSnapshot {
            inner: Cow::Borrowed(self.inner.get_mut()),
            next_channel_id: *self.next_channel_id.get_mut(),
            next_workflow_id: *self.next_workflow_id.get_mut(),
        }
    }

    /// Automatically truncates messages received by workflows.
    pub fn truncate_workflow_messages(&mut self) {
        self.truncate_messages = true;
    }
}

impl From<LocalStorageSnapshot<'_>> for LocalStorage {
    fn from(snapshot: LocalStorageSnapshot<'_>) -> Self {
        Self {
            inner: Mutex::new(snapshot.inner.into_owned()),
            next_channel_id: AtomicU64::new(snapshot.next_channel_id),
            next_workflow_id: AtomicU64::new(snapshot.next_workflow_id),
            truncate_messages: false,
        }
    }
}

/// Transactions used by [`LocalStorage`].
// TODO: use foolproof approach w/ cloning the state
#[derive(Debug)]
pub struct LocalTransaction<'a> {
    inner: MutexGuard<'a, Inner>,
    next_channel_id: &'a AtomicU64,
    next_workflow_id: &'a AtomicU64,
    truncate_messages: bool,
}

impl LocalTransaction<'_> {
    #[cfg(test)]
    pub(crate) fn peek_workflows(&self) -> &HashMap<WorkflowId, WorkflowRecord> {
        &self.inner.workflows
    }
}

#[async_trait]
impl ReadModules for LocalTransaction<'_> {
    async fn module(&self, id: &str) -> Option<ModuleRecord> {
        self.inner.modules.get(id).cloned()
    }

    fn modules(&self) -> BoxStream<'_, ModuleRecord> {
        stream::iter(self.inner.modules.values().cloned()).boxed()
    }
}

#[async_trait]
impl WriteModules for LocalTransaction<'_> {
    async fn insert_module(&mut self, module: ModuleRecord) {
        self.inner.modules.insert(module.id.clone(), module);
    }

    async fn update_tracing_metadata(&mut self, module_id: &str, metadata: PersistedMetadata) {
        let module = self.inner.modules.get_mut(module_id).unwrap();
        module.tracing_metadata.extend(metadata);
    }
}

#[async_trait]
impl ReadChannels for LocalTransaction<'_> {
    async fn channel(&self, id: ChannelId) -> Option<ChannelRecord> {
        let channel = self.inner.channels.get(&id)?;
        Some(channel.record.clone())
    }

    async fn has_messages_for_receiver_workflow(&self, id: ChannelId) -> bool {
        if let Some(channel) = self.inner.channels.get(&id) {
            if let Some(workflow_id) = channel.record.receiver_workflow_id {
                let workflow = &self.inner.workflows[&workflow_id].persisted;
                let (.., state) = workflow.find_inbound_channel(id);
                return state.received_message_count() < channel.next_message_idx;
            }
        }
        false
    }

    async fn channel_message(&self, id: ChannelId, index: usize) -> Result<Vec<u8>, MessageError> {
        let channel = self
            .inner
            .channels
            .get(&id)
            .ok_or(MessageError::UnknownChannelId)?;

        let start_idx = channel.next_message_idx - channel.messages.len();
        let idx_in_channel = index
            .checked_sub(start_idx)
            .ok_or(MessageError::Truncated)?;
        let is_closed = channel.record.is_closed;
        channel
            .messages
            .get(idx_in_channel)
            .map(|message| message.clone().into())
            .ok_or(MessageError::NonExistingIndex { is_closed })
    }
}

#[async_trait]
impl WriteChannels for LocalTransaction<'_> {
    async fn allocate_channel_id(&mut self) -> ChannelId {
        ChannelId::from(self.next_channel_id.fetch_add(1, Ordering::SeqCst))
    }

    async fn get_or_insert_channel(
        &mut self,
        id: ChannelId,
        record: ChannelRecord,
    ) -> ChannelRecord {
        self.inner
            .channels
            .entry(id)
            .or_insert_with(|| LocalChannel::new(record))
            .record
            .clone()
    }

    async fn manipulate_channel<F: FnOnce(&mut ChannelRecord) + Send>(
        &mut self,
        id: ChannelId,
        action: F,
    ) -> ChannelRecord {
        let channel = self.inner.channels.get_mut(&id).unwrap();
        action(&mut channel.record);
        channel.record.clone()
    }

    async fn push_messages(
        &mut self,
        id: ChannelId,
        messages: Vec<Vec<u8>>,
    ) -> Result<(), SendError> {
        let channel = self.inner.channels.get_mut(&id).unwrap();
        if channel.record.is_closed {
            return Err(SendError::Closed);
        }

        let len = messages.len();
        channel
            .messages
            .extend(messages.into_iter().map(Message::from));
        channel.next_message_idx += len;
        Ok(())
    }

    async fn truncate_channel(&mut self, id: ChannelId, min_index: usize) {
        if let Some(channel) = self.inner.channels.get_mut(&id) {
            channel.truncate(min_index);
        }
    }
}

#[async_trait]
impl ReadWorkflows for LocalTransaction<'_> {
    async fn count_workflows(&self) -> usize {
        self.inner.workflows.len()
    }

    async fn workflow(&self, id: WorkflowId) -> Option<WorkflowRecord> {
        self.inner.workflows.get(&id).cloned()
    }

    async fn find_pending_workflow(&self) -> Option<WorkflowRecord> {
        self.inner
            .workflows
            .values()
            .find(|record| {
                let persisted = &record.persisted;
                !persisted.is_initialized() || persisted.pending_wakeup_causes().next().is_some()
            })
            .cloned()
    }

    async fn find_consumable_channel(&self) -> Option<(ChannelId, usize, WorkflowRecord)> {
        let workflows = self.inner.workflows.values();
        let mut all_channels = workflows.flat_map(|record| {
            record
                .persisted
                .inbound_channels()
                .map(move |(_, _, state)| (record, state))
        });
        all_channels.find_map(|(record, state)| {
            if state.waits_for_message() {
                let channel_id = state.id();
                let next_message_idx = state.received_message_count();
                if self.inner.channels[&channel_id].contains_index(next_message_idx) {
                    return Some((channel_id, next_message_idx, record.clone()));
                }
            }
            None
        })
    }

    async fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>> {
        let workflows = self.inner.workflows.values();
        let timers = workflows.flat_map(|record| record.persisted.timers());
        let expirations = timers.filter_map(|(_, state)| {
            if state.completed_at().is_none() {
                Some(state.definition().expires_at)
            } else {
                None
            }
        });
        expirations.min()
    }
}

#[async_trait]
impl WriteWorkflows for LocalTransaction<'_> {
    async fn allocate_workflow_id(&mut self) -> WorkflowId {
        self.next_workflow_id.fetch_add(1, Ordering::SeqCst)
    }

    async fn insert_workflow(&mut self, state: WorkflowRecord) {
        self.inner.workflows.insert(state.id, state);
    }

    async fn persist_workflow(
        &mut self,
        id: WorkflowId,
        workflow: PersistedWorkflow,
        tracing_spans: PersistedSpans,
    ) {
        let record = self.inner.workflows.get_mut(&id).unwrap();
        record.persisted = workflow;
        record.tracing_spans = tracing_spans;
    }

    async fn manipulate_workflow<F: FnOnce(&mut PersistedWorkflow) + Send>(
        &mut self,
        id: WorkflowId,
        action: F,
    ) -> Option<WorkflowRecord> {
        let record = self.inner.workflows.get_mut(&id)?;
        action(&mut record.persisted);
        Some(record.clone())
    }

    async fn manipulate_all_workflows<F: FnMut(&mut PersistedWorkflow) + Send>(
        &mut self,
        criteria: WorkflowSelectionCriteria,
        mut action: F,
    ) {
        for record in self.inner.workflows.values_mut() {
            if criteria.matches(record) {
                action(&mut record.persisted);
            }
        }
    }

    async fn delete_workflow(&mut self, id: WorkflowId) {
        self.inner.workflows.remove(&id);
    }
}

#[async_trait]
impl StorageTransaction for LocalTransaction<'_> {
    async fn commit(mut self) {
        if self.truncate_messages {
            let inner = &mut *self.inner;
            for (&id, channel) in &mut inner.channels {
                if let Some(workflow_id) = channel.record.receiver_workflow_id {
                    let workflow = &inner.workflows[&workflow_id].persisted;
                    let (.., state) = workflow.find_inbound_channel(id);
                    channel.truncate(state.received_message_count());
                }
            }
        }
    }
}

#[async_trait]
impl<'a> Storage<'a> for LocalStorage {
    type Transaction = LocalTransaction<'a>;
    type ReadonlyTransaction = LocalTransaction<'a>;

    async fn transaction(&'a self) -> Self::Transaction {
        let inner = self.inner.lock().await;
        LocalTransaction {
            inner,
            next_channel_id: &self.next_channel_id,
            next_workflow_id: &self.next_workflow_id,
            truncate_messages: self.truncate_messages,
        }
    }

    async fn readonly_transaction(&'a self) -> Self::ReadonlyTransaction {
        self.transaction().await
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    #[async_std::test]
    async fn truncating_channel() {
        let storage = LocalStorage::default();
        let mut transaction = storage.transaction().await;
        transaction.truncate_channel(1, 42).await;
        let err = transaction.channel_message(1, 0).await.unwrap_err();
        assert_matches!(err, MessageError::UnknownChannelId);

        let channel_state = ChannelRecord {
            receiver_workflow_id: Some(1),
            sender_workflow_ids: HashSet::new(),
            has_external_sender: true,
            is_closed: false,
            received_messages: 0,
        };
        transaction.get_or_insert_channel(1, channel_state).await;

        transaction
            .push_messages(1, vec![b"test".to_vec()])
            .await
            .unwrap();
        let message = transaction.channel_message(1, 0).await.unwrap();
        assert_eq!(message, b"test");
        let err = transaction.channel_message(1, 1).await.unwrap_err();
        assert_matches!(err, MessageError::NonExistingIndex { is_closed: false });

        transaction
            .push_messages(1, vec![b"other".to_vec()])
            .await
            .unwrap();
        let message = transaction.channel_message(1, 0).await.unwrap();
        assert_eq!(message, b"test");
        let message = transaction.channel_message(1, 1).await.unwrap();
        assert_eq!(message, b"other");

        transaction.truncate_channel(1, 1).await;
        let message = transaction.channel_message(1, 1).await.unwrap();
        assert_eq!(message, b"other");
        let err = transaction.channel_message(1, 0).await.unwrap_err();
        assert_matches!(err, MessageError::Truncated);
    }
}
