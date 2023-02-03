//! Local in-memory storage implementation.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{
    lock::{Mutex, MutexGuard},
    stream::{self, BoxStream},
    StreamExt,
};
use serde::{Deserialize, Serialize};
use tracing_tunnel::PersistedMetadata;

use std::{
    borrow::Cow,
    cmp,
    collections::{HashMap, VecDeque},
    convert::Infallible,
    ops,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use super::{
    helper::{ChannelSide, StorageHelper},
    ActiveWorkflowState, ChannelRecord, MessageError, ModuleRecord, ReadChannels, ReadModules,
    ReadWorkflows, Storage, StorageTransaction, WorkflowRecord, WorkflowSelectionCriteria,
    WorkflowState, WorkflowWaker, WorkflowWakerRecord, WriteChannels, WriteModules,
    WriteWorkflowWakers, WriteWorkflows,
};
use crate::{utils::Message, PersistedWorkflow};
use tardigrade::{ChannelId, WakerId, WorkflowId};
use tardigrade_worker::{WorkerRecord, WorkerStorageConnection};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalChannel {
    #[serde(flatten)]
    record: ChannelRecord,
    messages: VecDeque<Message>,
}

impl LocalChannel {
    fn new(record: ChannelRecord) -> Self {
        Self {
            record,
            messages: VecDeque::new(),
        }
    }

    fn contains_index(&self, idx: u64) -> bool {
        let received_messages = self.record.received_messages;
        let start_idx = received_messages - self.messages.len() as u64;
        (start_idx..received_messages).contains(&idx)
            || (self.record.is_closed && idx == received_messages) // EOF marker
    }

    fn truncate(&mut self, min_index: u64) {
        let start_idx = self.record.received_messages - self.messages.len() as u64;
        let messages_to_truncate = usize::try_from(min_index.saturating_sub(start_idx))
            .expect("integer overflow when truncating messages");
        let messages_to_truncate = cmp::min(messages_to_truncate, self.messages.len());
        self.messages = self.messages.split_off(messages_to_truncate);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Inner {
    modules: HashMap<String, ModuleRecord>,
    channels: HashMap<ChannelId, LocalChannel>,
    workflows: HashMap<WorkflowId, WorkflowRecord>,
    workflow_wakers: Vec<WorkflowWakerRecord>,
    workers: HashMap<u64, WorkerRecord>,
}

impl Default for Inner {
    fn default() -> Self {
        let closed_channel = LocalChannel::new(ChannelRecord::closed());
        Self {
            modules: HashMap::new(),
            channels: HashMap::from_iter([(0, closed_channel)]),
            workflows: HashMap::new(),
            workflow_wakers: Vec::new(),
            workers: HashMap::new(),
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
    next_waker_id: u64,
    next_worker_id: u64,
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
/// part, the snapshot provides [a method](LocalStorageSnapshot::modules_mut())
/// to manipulate them, e.g. in order to store module bytes separately.
///
/// [serialized]: https://docs.rs/serde/1/serde
///
/// # Examples
///
/// ```
/// # use async_std::fs;
/// # use std::str;
/// # use tardigrade_rt::{
/// #     engine::Wasmtime, runtime::Runtime, storage::{LocalStorage, LocalStorageSnapshot},
/// # };
/// #
/// # async fn test_wrapper() -> anyhow::Result<()> {
/// let engine = Wasmtime::default();
/// let mut storage = LocalStorage::default();
/// // Remove messages consumed by workflows.
/// storage.truncate_workflow_messages();
/// let runtime = Runtime::builder(engine, storage).build();
/// // Do something with the runtime...
///
/// let mut storage = runtime.into_storage();
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
    next_waker_id: AtomicU64,
    next_worker_id: AtomicU64,
    truncate_messages: bool,
}

impl Default for LocalStorage {
    fn default() -> Self {
        Self {
            inner: Mutex::default(),
            next_channel_id: AtomicU64::new(1), // skip the closed channel
            next_workflow_id: AtomicU64::new(1),
            next_waker_id: AtomicU64::new(1),
            next_worker_id: AtomicU64::new(1),
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
            next_waker_id: *self.next_waker_id.get_mut(),
            next_worker_id: *self.next_worker_id.get_mut(),
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
            next_waker_id: AtomicU64::new(snapshot.next_waker_id),
            next_worker_id: AtomicU64::new(snapshot.next_worker_id),
            truncate_messages: false,
        }
    }
}

/// Readonly transaction used by [`LocalStorage`].
#[derive(Debug, Clone)]
pub struct LocalReadonlyTransaction<'a> {
    inner: Inner,
    next_channel_id: &'a AtomicU64,
    next_workflow_id: &'a AtomicU64,
    next_waker_id: &'a AtomicU64,
    next_worker_id: &'a AtomicU64,
}

impl LocalReadonlyTransaction<'_> {
    fn active_workflow_states(&self) -> impl Iterator<Item = &PersistedWorkflow> + '_ {
        self.inner.workflows.values().filter_map(|record| {
            if let WorkflowState::Active(state) = &record.state {
                Some(&state.persisted)
            } else {
                None
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn wakers_for_workflow(
        &self,
        workflow_id: WorkflowId,
    ) -> impl Iterator<Item = &WorkflowWaker> + '_ {
        self.inner.workflow_wakers.iter().filter_map(move |record| {
            if record.workflow_id == workflow_id {
                Some(&record.waker)
            } else {
                None
            }
        })
    }
}

#[async_trait]
impl ReadModules for LocalReadonlyTransaction<'_> {
    async fn module(&self, id: &str) -> Option<ModuleRecord> {
        self.inner.modules.get(id).cloned()
    }

    fn modules(&self) -> BoxStream<'_, ModuleRecord> {
        stream::iter(self.inner.modules.values().cloned()).boxed()
    }
}

#[async_trait]
impl ReadChannels for LocalReadonlyTransaction<'_> {
    async fn channel(&self, id: ChannelId) -> Option<ChannelRecord> {
        let channel = self.inner.channels.get(&id)?;
        Some(channel.record.clone())
    }

    async fn channel_message(&self, id: ChannelId, index: u64) -> Result<Vec<u8>, MessageError> {
        let channel = self
            .inner
            .channels
            .get(&id)
            .ok_or(MessageError::UnknownChannelId)?;

        let start_idx = channel.record.received_messages - channel.messages.len() as u64;
        let idx_in_channel = index
            .checked_sub(start_idx)
            .ok_or(MessageError::Truncated)?;

        let is_closed = channel.record.is_closed;
        let idx_in_channel = usize::try_from(idx_in_channel)
            .map_err(|_| MessageError::NonExistingIndex { is_closed })?;
        channel
            .messages
            .get(idx_in_channel)
            .map(|message| message.clone().into())
            .ok_or(MessageError::NonExistingIndex { is_closed })
    }

    #[allow(clippy::cast_possible_truncation)] // cast is safe due to previous checks
    fn channel_messages(
        &self,
        id: ChannelId,
        indices: ops::RangeInclusive<u64>,
    ) -> BoxStream<'_, (u64, Vec<u8>)> {
        let Some(channel) = self.inner.channels.get(&id) else {
            return stream::empty().boxed();
        };
        let Some(max_idx) = channel.messages.len().checked_sub(1) else {
            // If the channel has no messages stored, it can only return an empty stream.
            return stream::empty().boxed();
        };
        let max_idx = max_idx as u64;

        let first_idx = channel.record.received_messages - channel.messages.len() as u64;
        let start_idx = indices.start().saturating_sub(first_idx);
        if start_idx > max_idx {
            return stream::empty().boxed();
        }
        let end_idx = indices.end().saturating_sub(first_idx).min(max_idx);
        let indexed_messages = (start_idx..=end_idx).map(move |i| {
            (
                first_idx + i,
                channel.messages[i as usize].as_ref().to_vec(),
            )
        });
        stream::iter(indexed_messages).boxed()
    }
}

#[async_trait]
impl ReadWorkflows for LocalReadonlyTransaction<'_> {
    async fn count_active_workflows(&self) -> u64 {
        self.active_workflow_states().count() as u64
    }

    async fn workflow(&self, id: WorkflowId) -> Option<WorkflowRecord> {
        self.inner.workflows.get(&id).cloned()
    }

    async fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>> {
        let workflows = self.active_workflow_states();
        let expirations = workflows.filter_map(|workflow| workflow.common().nearest_timer());
        expirations.min()
    }
}

/// Transaction used by [`LocalStorage`].
#[derive(Debug)]
pub struct LocalTransaction<'a> {
    // Alternatively, we could manipulate the guarded data directly.
    // This wouldn't break isolation because of the `Mutex`, and wouldn't break atomicity
    // in `Runtime` because there are no rollbacks, and all storage operations
    // are synchronous. That is, there are no wait points in the `Runtime`-produced futures
    // at which the future may be cancelled to observe a partially applied transaction.
    // However, this approach is hard to reason about in the general case; it is not cancel-safe
    // if used together with "true" async operations. Hence, foolproof cloning of `Inner`.
    target: MutexGuard<'a, Inner>,
    readonly: LocalReadonlyTransaction<'a>,
    truncate_messages: bool,
}

impl LocalTransaction<'_> {
    fn inner(&self) -> &Inner {
        &self.readonly.inner
    }

    fn inner_mut(&mut self) -> &mut Inner {
        &mut self.readonly.inner
    }

    /// Ensures that the specified workflow will be polled.
    #[cfg(test)]
    pub(crate) fn prepare_wakers_for_workflow(&mut self, workflow_id: WorkflowId) {
        let wakers = &mut self.inner_mut().workflow_wakers;
        let pos = wakers
            .iter()
            .position(|record| record.workflow_id == workflow_id);
        if let Some(pos) = pos {
            wakers.swap(0, pos);
        } else {
            assert!(
                wakers.is_empty(),
                "no wakers for workflow {workflow_id}: {wakers:?}"
            );
        }
    }
}

delegate_read_traits!(LocalTransaction<'_> { readonly });

#[async_trait]
impl WriteModules for LocalTransaction<'_> {
    async fn insert_module(&mut self, module: ModuleRecord) {
        self.inner_mut().modules.insert(module.id.clone(), module);
    }

    async fn update_tracing_metadata(&mut self, module_id: &str, metadata: PersistedMetadata) {
        let module = self.inner_mut().modules.get_mut(module_id).unwrap();
        module.tracing_metadata.extend(metadata);
    }
}

#[async_trait]
impl WriteChannels for LocalTransaction<'_> {
    async fn allocate_channel_id(&mut self) -> ChannelId {
        self.readonly.next_channel_id.fetch_add(1, Ordering::SeqCst)
    }

    async fn insert_channel(&mut self, id: ChannelId, record: ChannelRecord) {
        self.inner_mut()
            .channels
            .entry(id)
            .or_insert_with(|| LocalChannel::new(record));
    }

    async fn manipulate_channel<F: FnOnce(&mut ChannelRecord) + Send>(
        &mut self,
        id: ChannelId,
        action: F,
    ) -> ChannelRecord {
        let channel = self.inner_mut().channels.get_mut(&id).unwrap();
        action(&mut channel.record);
        channel.record.clone()
    }

    async fn push_messages(&mut self, id: ChannelId, messages: Vec<Vec<u8>>) {
        let channel = self.inner_mut().channels.get_mut(&id).unwrap();
        if !channel.record.is_closed {
            let len = messages.len() as u64;
            channel
                .messages
                .extend(messages.into_iter().map(Message::from));
            channel.record.received_messages += len;
        }
    }

    async fn truncate_channel(&mut self, id: ChannelId, min_index: u64) {
        if let Some(channel) = self.inner_mut().channels.get_mut(&id) {
            channel.truncate(min_index);
        }
    }
}

#[async_trait]
impl WriteWorkflows for LocalTransaction<'_> {
    async fn allocate_workflow_id(&mut self) -> WorkflowId {
        self.readonly
            .next_workflow_id
            .fetch_add(1, Ordering::SeqCst)
    }

    async fn insert_workflow(&mut self, record: WorkflowRecord) {
        self.inner_mut().workflows.insert(record.id, record);
    }

    async fn workflow_for_update(&mut self, id: WorkflowId) -> Option<WorkflowRecord> {
        self.workflow(id).await
    }

    async fn update_workflow(&mut self, id: WorkflowId, state: WorkflowState) {
        let record = self.inner_mut().workflows.get_mut(&id).unwrap();
        record.state = state;
        record.execution_count += 1;
    }

    async fn workflow_with_wakers_for_update(
        &mut self,
    ) -> Option<WorkflowRecord<ActiveWorkflowState>> {
        let workflows = &self.readonly.inner.workflows;
        self.inner().workflow_wakers.iter().find_map(|record| {
            let workflow = &workflows[&record.workflow_id];
            if matches!(workflow.state, WorkflowState::Active(_)) {
                return workflow.clone().into_active();
            }
            None
        })
    }

    async fn workflow_with_consumable_channel_for_update(
        &mut self,
    ) -> Option<WorkflowRecord<ActiveWorkflowState>> {
        let workflows = self.inner().workflows.values().filter_map(|record| {
            if let WorkflowState::Active(state) = &record.state {
                Some((record, &state.persisted))
            } else {
                None
            }
        });
        let mut all_channels = workflows.flat_map(|(record, persisted)| {
            persisted
                .receivers()
                .map(move |(id, state)| (record, id, state))
        });

        all_channels.find_map(|(record, channel_id, state)| {
            if state.waits_for_message() {
                let next_message_idx = state.received_message_count();
                let channel = &self.inner().channels[&channel_id];
                if channel.contains_index(next_message_idx) {
                    return record.clone().into_active();
                }
            }
            None
        })
    }
}

#[async_trait]
impl WriteWorkflowWakers for LocalTransaction<'_> {
    async fn insert_waker(&mut self, workflow_id: WorkflowId, waker: WorkflowWaker) {
        let waker_id = self.readonly.next_waker_id.fetch_add(1, Ordering::SeqCst);
        self.inner_mut().workflow_wakers.push(WorkflowWakerRecord {
            workflow_id,
            waker_id,
            waker,
        });
    }

    async fn insert_waker_for_matching_workflows(
        &mut self,
        criteria: WorkflowSelectionCriteria,
        waker: WorkflowWaker,
    ) {
        let wakers = &mut self.readonly.inner.workflow_wakers;
        for (&id, record) in &self.readonly.inner.workflows {
            let persisted = match &record.state {
                WorkflowState::Active(state) => &state.persisted,
                WorkflowState::Errored(state) => &state.persisted,
                WorkflowState::Completed(_) => continue,
            };

            if criteria.matches(persisted) {
                let waker_id = self.readonly.next_waker_id.fetch_add(1, Ordering::SeqCst);
                wakers.push(WorkflowWakerRecord {
                    workflow_id: id,
                    waker_id,
                    waker: waker.clone(),
                });
            }
        }
    }

    async fn wakers_for_workflow(&self, workflow_id: WorkflowId) -> Vec<WorkflowWakerRecord> {
        let filtered = self.inner().workflow_wakers.iter().filter_map(|record| {
            Some(record)
                .filter(|&it| it.workflow_id == workflow_id)
                .cloned()
        });
        filtered.collect()
    }

    async fn delete_wakers(&mut self, workflow_id: WorkflowId, waker_ids: &[WakerId]) {
        self.inner_mut().workflow_wakers.retain(|record| {
            record.workflow_id != workflow_id || !waker_ids.contains(&record.waker_id)
        });
    }
}

#[async_trait]
impl WorkerStorageConnection for LocalTransaction<'_> {
    type Error = Infallible;

    async fn worker(&mut self, name: &str) -> Result<Option<WorkerRecord>, Self::Error> {
        let mut workers = self.inner().workers.values();
        let existing_worker = workers.find(|worker| worker.name == name);
        Ok(existing_worker.cloned())
    }

    async fn get_or_create_worker(&mut self, name: &str) -> Result<WorkerRecord, Self::Error> {
        let mut workers = self.inner().workers.values();
        let existing_worker = workers.find(|worker| worker.name == name);
        Ok(if let Some(worker) = existing_worker {
            worker.clone()
        } else {
            let id = self.readonly.next_worker_id.fetch_add(1, Ordering::SeqCst);
            let inbound_channel_id = self.allocate_channel_id().await;
            self.insert_channel(inbound_channel_id, ChannelRecord::owned_by_host())
                .await;

            let record = WorkerRecord {
                id,
                name: name.to_owned(),
                inbound_channel_id,
                cursor: 0,
            };
            self.inner_mut().workers.insert(id, record.clone());
            record
        })
    }

    async fn update_worker_cursor(
        &mut self,
        worker_id: u64,
        cursor: u64,
    ) -> Result<(), Self::Error> {
        let worker = self.inner_mut().workers.get_mut(&worker_id).unwrap();
        worker.cursor = cursor;
        Ok(())
    }

    async fn push_message(
        &mut self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), Self::Error> {
        self.push_messages(channel_id, vec![message]).await;
        Ok(())
    }

    async fn close_response_channel(&mut self, channel_id: ChannelId) -> Result<bool, Self::Error> {
        Ok(StorageHelper::new(self)
            .close_channel_side(channel_id, ChannelSide::HostSender)
            .await)
    }

    async fn release(mut self) {
        self.commit().await;
    }
}

#[async_trait]
impl StorageTransaction for LocalTransaction<'_> {
    async fn commit(mut self) {
        if self.truncate_messages {
            let inner = self.inner_mut();
            for (&id, channel) in &mut inner.channels {
                if let Some(workflow_id) = channel.record.receiver_workflow_id {
                    let workflow = match &inner.workflows[&workflow_id].state {
                        WorkflowState::Active(state) => &state.persisted,
                        WorkflowState::Completed(_) | WorkflowState::Errored(_) => continue,
                    };
                    let state = workflow.receiver(id).unwrap();
                    channel.truncate(state.received_message_count());
                }
            }
        }
        *self.target = self.readonly.inner;
    }
}

#[async_trait]
impl Storage for LocalStorage {
    type Transaction<'a> = LocalTransaction<'a>;
    type ReadonlyTransaction<'a> = LocalReadonlyTransaction<'a>;

    async fn transaction(&self) -> Self::Transaction<'_> {
        let target = self.inner.lock().await;
        LocalTransaction {
            readonly: LocalReadonlyTransaction {
                inner: target.clone(),
                next_channel_id: &self.next_channel_id,
                next_workflow_id: &self.next_workflow_id,
                next_waker_id: &self.next_waker_id,
                next_worker_id: &self.next_worker_id,
            },
            target,
            truncate_messages: self.truncate_messages,
        }
    }

    async fn readonly_transaction(&self) -> Self::ReadonlyTransaction<'_> {
        LocalReadonlyTransaction {
            inner: self.inner.lock().await.clone(),
            next_channel_id: &self.next_channel_id,
            next_workflow_id: &self.next_workflow_id,
            next_waker_id: &self.next_waker_id,
            next_worker_id: &self.next_worker_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use std::collections::HashSet;

    use super::*;

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
    async fn truncating_channel() {
        let storage = LocalStorage::default();
        let mut transaction = storage.transaction().await;
        transaction.truncate_channel(1, 42).await;
        let err = transaction.channel_message(1, 0).await.unwrap_err();
        assert_matches!(err, MessageError::UnknownChannelId);

        let channel_state = create_channel_record();
        transaction.insert_channel(1, channel_state).await;

        transaction.push_messages(1, vec![b"test".to_vec()]).await;
        let message = transaction.channel_message(1, 0).await.unwrap();
        assert_eq!(message, b"test");
        let err = transaction.channel_message(1, 1).await.unwrap_err();
        assert_matches!(err, MessageError::NonExistingIndex { is_closed: false });

        transaction.push_messages(1, vec![b"other".to_vec()]).await;
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

    #[async_std::test]
    async fn message_ranges() {
        let storage = LocalStorage::default();
        let mut transaction = storage.transaction().await;

        let channel_state = create_channel_record();
        transaction.insert_channel(1, channel_state).await;

        let messages: Vec<_> = transaction
            .channel_messages(1, 0..=u64::MAX)
            .collect()
            .await;
        assert_messages(&messages, &[]);
        let messages: Vec<_> = transaction.channel_messages(1, 1..=2).collect().await;
        assert_messages(&messages, &[]);

        let first_message: &[u8] = b"test";
        transaction
            .push_messages(1, vec![first_message.to_vec()])
            .await;

        let messages: Vec<_> = transaction
            .channel_messages(1, 0..=u64::MAX)
            .collect()
            .await;
        assert_messages(&messages, &[(0, first_message)]);
        let messages: Vec<_> = transaction.channel_messages(1, 0..=10).collect().await;
        assert_messages(&messages, &[(0, first_message)]);
        let messages: Vec<_> = transaction.channel_messages(1, 0..=0).collect().await;
        assert_messages(&messages, &[(0, first_message)]);

        let second_message: &[u8] = b"other";
        transaction
            .push_messages(1, vec![second_message.to_vec()])
            .await;

        for full_range in [0..=u64::MAX, 0..=2, 0..=1] {
            let messages: Vec<_> = transaction.channel_messages(1, full_range).collect().await;
            assert_messages(&messages, &[(0, first_message), (1, second_message)]);
        }
        let messages: Vec<_> = transaction.channel_messages(1, 0..=0).collect().await;
        assert_messages(&messages, &[(0, first_message)]);
        for end_range in [1..=u64::MAX, 1..=2, 1..=1] {
            let messages: Vec<_> = transaction.channel_messages(1, end_range).collect().await;
            assert_messages(&messages, &[(1, second_message)]);
        }
        let messages: Vec<_> = transaction.channel_messages(1, 2..=2).collect().await;
        assert_messages(&messages, &[]);

        transaction.truncate_channel(1, 1).await;

        for full_range in [0..=u64::MAX, 0..=2, 0..=1, 1..=u64::MAX, 1..=2, 1..=1] {
            let messages: Vec<_> = transaction.channel_messages(1, full_range).collect().await;
            assert_messages(&messages, &[(1, second_message)]);
        }
        let messages: Vec<_> = transaction.channel_messages(1, 2..=2).collect().await;
        assert_messages(&messages, &[]);
    }

    fn assert_messages(actual: &[(u64, Vec<u8>)], expected: &[(u64, &[u8])]) {
        assert_eq!(actual.len(), expected.len(), "{actual:?} != {expected:?}");
        for (actual_msg, expected_msg) in actual.iter().zip(expected) {
            assert_eq!(actual_msg.0, expected_msg.0);
            assert_eq!(actual_msg.1, expected_msg.1);
        }
    }
}
