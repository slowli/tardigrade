//! Local in-memory storage implementation.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{
    lock::{Mutex, MutexGuard},
    stream::{self, BoxStream},
    StreamExt,
};
use tracing_tunnel::{PersistedMetadata, PersistedSpans};

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering},
};

use super::{
    ChannelRecord, ChannelState, MessageOperationError, MessageOrEof, ModuleRecord, ReadChannels,
    ReadModules, ReadWorkflows, Storage, StorageTransaction, WorkflowRecord,
    WorkflowSelectionCriteria, WriteChannels, WriteModules, WriteWorkflows,
};
use crate::PersistedWorkflow;
use tardigrade::{channel::SendError, ChannelId, WorkflowId};

#[derive(Debug)]
struct LocalMessageRecord {
    is_ready: bool,
    received_count: usize,
    payload: MessageOrEof,
}

impl LocalMessageRecord {
    fn new(payload: Vec<u8>) -> Self {
        Self {
            is_ready: true,
            received_count: 0,
            payload: MessageOrEof::Message(payload),
        }
    }

    fn eof() -> Self {
        Self {
            is_ready: true,
            received_count: 0,
            payload: MessageOrEof::Eof,
        }
    }
}

#[derive(Debug)]
struct LocalChannel {
    state: ChannelState,
    messages: BTreeMap<usize, LocalMessageRecord>,
    next_message_idx: usize,
}

impl LocalChannel {
    fn new(state: ChannelState) -> Self {
        Self {
            state,
            messages: BTreeMap::new(),
            next_message_idx: 0,
        }
    }

    fn has_message(&self) -> bool {
        self.messages.values().any(|message| message.is_ready)
    }
}

impl ModuleRecord<'_> {
    fn into_owned(self) -> ModuleRecord<'static> {
        ModuleRecord {
            id: self.id,
            bytes: Cow::Owned(self.bytes.into_owned()),
            tracing_metadata: self.tracing_metadata,
        }
    }
}

#[derive(Debug)]
struct Inner {
    modules: HashMap<String, ModuleRecord<'static>>,
    channels: HashMap<ChannelId, LocalChannel>,
    workflows: HashMap<WorkflowId, WorkflowRecord>,
}

impl Default for Inner {
    fn default() -> Self {
        let closed_channel = LocalChannel::new(ChannelState {
            receiver_workflow_id: None,
            sender_workflow_ids: HashSet::new(),
            has_external_sender: false,
            is_closed: true,
        });

        Self {
            modules: HashMap::new(),
            channels: HashMap::from_iter([(0, closed_channel)]),
            workflows: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct LocalStorage {
    inner: Mutex<Inner>,
    next_channel_id: AtomicU64,
    next_workflow_id: AtomicU64,
}

impl Default for LocalStorage {
    fn default() -> Self {
        Self {
            inner: Mutex::default(),
            next_channel_id: AtomicU64::new(1), // skip the closed channel
            next_workflow_id: AtomicU64::new(0),
        }
    }
}

#[derive(Debug)]
pub struct LocalTransaction<'a> {
    inner: MutexGuard<'a, Inner>,
    next_channel_id: &'a AtomicU64,
    next_workflow_id: &'a AtomicU64,
}

impl LocalTransaction<'_> {
    pub fn peek_channel(&self, channel_id: ChannelId) -> Option<impl Iterator<Item = &[u8]>> {
        let channel = self.inner.channels.get(&channel_id)?;
        let messages = channel.messages.values().filter_map(|message| {
            if message.is_ready {
                if let MessageOrEof::Message(payload) = &message.payload {
                    return Some(payload.as_slice());
                }
            }
            None
        });
        Some(messages)
    }
}

#[async_trait]
impl ReadModules for LocalTransaction<'_> {
    async fn module(&self, id: &str) -> Option<ModuleRecord<'static>> {
        self.inner.modules.get(id).cloned()
    }

    fn modules(&self) -> BoxStream<'_, ModuleRecord<'static>> {
        stream::iter(self.inner.modules.values().cloned()).boxed()
    }
}

#[async_trait]
impl WriteModules for LocalTransaction<'_> {
    async fn insert_module(&mut self, module: ModuleRecord<'_>) {
        self.inner
            .modules
            .insert(module.id.clone(), module.into_owned());
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
        Some(ChannelRecord {
            state: channel.state.clone(),
            message_count: channel
                .messages
                .values()
                .filter(|message| message.is_ready)
                .count(),
            next_message_idx: channel.next_message_idx,
        })
    }
}

#[derive(Debug)]
pub struct LocalToken {
    channel_id: ChannelId,
    message_idx: usize,
    received_count: usize,
}

#[async_trait]
impl WriteChannels for LocalTransaction<'_> {
    type Token = LocalToken;

    async fn allocate_channel_id(&mut self) -> ChannelId {
        ChannelId::from(self.next_channel_id.fetch_add(1, Ordering::SeqCst))
    }

    async fn get_or_insert_channel(&mut self, id: ChannelId, state: ChannelState) -> ChannelState {
        self.inner
            .channels
            .entry(id)
            .or_insert_with(|| LocalChannel::new(state))
            .state
            .clone()
    }

    async fn manipulate_channel<F: FnOnce(&mut ChannelState) + Send>(
        &mut self,
        id: ChannelId,
        action: F,
    ) -> ChannelState {
        let channel = self.inner.channels.get_mut(&id).unwrap();
        let was_closed = channel.state.is_closed;
        action(&mut channel.state);

        if channel.state.is_closed && !was_closed {
            channel
                .messages
                .insert(channel.next_message_idx, LocalMessageRecord::eof());
        }
        channel.state.clone()
    }

    async fn push_messages(
        &mut self,
        id: ChannelId,
        messages: Vec<Vec<u8>>,
    ) -> Result<(), SendError> {
        let channel = self.inner.channels.get_mut(&id).unwrap();
        if channel.state.is_closed {
            return Err(SendError::Closed);
        }

        let len = messages.len();
        let indices = channel.next_message_idx..(channel.next_message_idx + len);
        let messages = messages.into_iter().map(LocalMessageRecord::new);
        channel.messages.extend(indices.zip(messages));
        channel.next_message_idx += len;
        Ok(())
    }

    async fn receive_message(&mut self, id: ChannelId) -> Option<(MessageOrEof, Self::Token)> {
        let channel = self.inner.channels.get_mut(&id)?;
        channel.messages.iter_mut().find_map(|(&idx, record)| {
            if record.is_ready {
                record.received_count += 1;
                record.is_ready = false;
                let token = LocalToken {
                    channel_id: id,
                    message_idx: idx,
                    received_count: record.received_count,
                };
                Some((record.payload.clone(), token))
            } else {
                None
            }
        })
    }

    async fn remove_message(&mut self, token: Self::Token) -> Result<(), MessageOperationError> {
        let channel = self
            .inner
            .channels
            .get_mut(&token.channel_id)
            .ok_or(MessageOperationError::InvalidToken)?;
        let message = channel
            .messages
            .get(&token.message_idx)
            .ok_or(MessageOperationError::AlreadyRemoved)?;

        if !message.is_ready && message.received_count == token.received_count {
            channel.messages.remove(&token.message_idx);
            Ok(())
        } else {
            Err(MessageOperationError::ExpiredToken)
        }
    }

    async fn revert_message(&mut self, token: Self::Token) -> Result<(), MessageOperationError> {
        let channel = self
            .inner
            .channels
            .get_mut(&token.channel_id)
            .ok_or(MessageOperationError::InvalidToken)?;
        let message = channel
            .messages
            .get_mut(&token.message_idx)
            .ok_or(MessageOperationError::AlreadyRemoved)?;

        if !message.is_ready && message.received_count == token.received_count {
            message.is_ready = true;
            Ok(())
        } else {
            Err(MessageOperationError::ExpiredToken)
        }
    }
}

#[async_trait]
impl ReadWorkflows for LocalTransaction<'_> {
    async fn workflow(&self, id: WorkflowId) -> Option<WorkflowRecord> {
        self.inner.workflows.get(&id).cloned()
    }

    async fn find_workflow_with_pending_tasks(&self) -> Option<WorkflowRecord> {
        self.inner
            .workflows
            .values()
            .find(|record| {
                let persisted = &record.persisted;
                !persisted.is_initialized() || persisted.pending_events().next().is_some()
            })
            .cloned()
    }

    async fn find_consumable_channel(&self) -> Option<(ChannelId, WorkflowRecord)> {
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
                if self.inner.channels[&channel_id].has_message() {
                    return Some((channel_id, record.clone()));
                }
            }
            None
        })
    }

    async fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>> {
        let workflows = self.inner.workflows.values();
        let timers = workflows.flat_map(|record| record.persisted.timers());
        timers.map(|(_, timer)| timer.definition().expires_at).min()
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
    ) -> WorkflowRecord {
        let record = self.inner.workflows.get_mut(&id).unwrap();
        action(&mut record.persisted);
        record.clone()
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
    async fn commit(self) {
        // Does nothing.
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
        }
    }

    async fn readonly_transaction(&'a self) -> Self::ReadonlyTransaction {
        self.transaction().await
    }
}
