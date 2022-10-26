//! Persistence for `WorkflowManager`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing_tunnel::{PersistedMetadata, PersistedSpans, TracingEventReceiver};

use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem,
    task::Poll,
};

use crate::{
    manager::{transaction::Transaction, ChannelInfo, ChannelSide},
    receipt::Receipt,
    utils::{clone_join_error, Message},
    workflow::ChannelIds,
    PersistedWorkflow,
};
use tardigrade::{channel::SendError, interface::ChannelKind, ChannelId, WorkflowId};

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ChannelState {
    pub receiver_workflow_id: Option<WorkflowId>, // `None` means external receiver
    pub sender_workflow_ids: HashSet<WorkflowId>,
    pub has_external_sender: bool,
    pub is_closed: bool,
    pub messages: VecDeque<Message>,
    pub next_message_idx: usize,
}

impl ChannelState {
    fn new(
        sender_workflow_id: Option<WorkflowId>,
        receiver_workflow_id: Option<WorkflowId>,
    ) -> Self {
        Self {
            receiver_workflow_id,
            sender_workflow_ids: sender_workflow_id.into_iter().collect(),
            has_external_sender: sender_workflow_id.is_none(),
            is_closed: false,
            messages: VecDeque::new(),
            next_message_idx: 0,
        }
    }

    fn closed() -> Self {
        let mut this = Self::new(None, None);
        this.has_external_sender = false;
        this.is_closed = true;
        this
    }

    pub(super) fn info(&self) -> ChannelInfo {
        ChannelInfo {
            receiver_workflow_id: self.receiver_workflow_id,
            is_closed: self.is_closed,
            message_count: self.messages.len(),
            next_message_idx: self.next_message_idx,
        }
    }

    fn push_messages(
        &mut self,
        messages: impl IntoIterator<Item = Message>,
    ) -> Result<(), SendError> {
        if self.is_closed {
            return Err(SendError::Closed);
        }
        let old_len = self.messages.len();
        self.messages.extend(messages);
        self.next_message_idx += self.messages.len() - old_len;
        Ok(())
    }

    pub fn drain_messages(&mut self) -> (usize, VecDeque<Message>) {
        let start_idx = self.next_message_idx - self.messages.len();
        (start_idx, mem::take(&mut self.messages))
    }

    /// Returns `true` if the channel was closed.
    fn close_side(&mut self, side: ChannelSide) -> bool {
        match side {
            ChannelSide::HostSender => {
                self.has_external_sender = false;
            }
            ChannelSide::WorkflowSender(id) => {
                self.sender_workflow_ids.remove(&id);
            }
            ChannelSide::Receiver => {
                self.receiver_workflow_id = None;
                self.is_closed = true;
                return true;
            }
        }

        if !self.has_external_sender && self.sender_workflow_ids.is_empty() {
            self.is_closed = true;
        }
        self.is_closed
    }
}

/// Persisted information associated with a `WorkflowSpawner`.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(super) struct SpawnerMeta {
    pub tracing_metadata: PersistedMetadata,
}

/// Wrapper for either ongoing or persisted workflow.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct WorkflowWithMeta {
    pub definition_id: String,
    pub parent_id: Option<WorkflowId>,
    pub workflow: PersistedWorkflow,
    pub tracing_spans: PersistedSpans,
}

/// Serializable persisted view of the workflows and channels managed by a [`WorkflowManager`].
///
/// [`WorkflowManager`]: crate::manager::WorkflowManager
#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedWorkflows {
    pub(super) workflows: HashMap<WorkflowId, WorkflowWithMeta>,
    pub(super) channels: HashMap<ChannelId, ChannelState>,
    pub(super) next_workflow_id: WorkflowId,
    pub(super) next_channel_id: ChannelId,
    pub(super) spawners: HashMap<String, SpawnerMeta>,
}

impl PersistedWorkflows {
    pub(super) fn new(spawner_ids: impl Iterator<Item = String>) -> Self {
        let mut channels = HashMap::with_capacity(1);
        channels.insert(0, ChannelState::closed());
        Self {
            workflows: HashMap::new(),
            channels,
            next_workflow_id: 0,
            next_channel_id: 1, // avoid "pre-closed" channel ID
            spawners: spawner_ids.map(|id| (id, SpawnerMeta::default())).collect(),
        }
    }

    pub(super) fn take_message(&mut self, channel_id: ChannelId) -> Option<(Message, bool)> {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.messages.pop_front().map(|message| {
            let signal_closure = channel_state.is_closed && channel_state.messages.is_empty();
            (message, signal_closure)
        })
    }

    pub(super) fn revert_taking_message(&mut self, channel_id: ChannelId, message: Message) {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.messages.push_front(message);
    }

    #[tracing::instrument(skip(self), err)]
    pub(super) fn send_message(
        &mut self,
        channel_id: ChannelId,
        message: Message,
    ) -> Result<(), SendError> {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        tracing::debug!(
            idx = channel_state.next_message_idx,
            "allocated index for message"
        );
        channel_state.push_messages([message])
    }

    #[tracing::instrument(skip(self), fields(closed = false))]
    pub(super) fn close_channel_side(&mut self, channel_id: ChannelId, side: ChannelSide) {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        tracing::debug!(?channel_state, "current channel state");
        if !channel_state.close_side(side) {
            // The channel was not closed; no further actions are necessary.
            return;
        }
        tracing::Span::current().record("closed", true);

        // If the channel is closed by the receiver, no need to notify it again.
        if !matches!(side, ChannelSide::Receiver) && channel_state.messages.is_empty() {
            // We can notify the receiver immediately only if there are no unconsumed messages
            // in the channel.
            if let Some(receiver_workflow_id) = channel_state.receiver_workflow_id {
                let workflow = &mut self
                    .workflows
                    .get_mut(&receiver_workflow_id)
                    .unwrap()
                    .workflow;
                let (child_id, name) = workflow.find_inbound_channel(channel_id);
                workflow.close_inbound_channel(child_id, &name);
            }
        }

        for sender_workflow_id in &channel_state.sender_workflow_ids {
            // The workflow may be missing from `self.workflows` if it has just completed.
            if let Some(persisted) = self.workflows.get_mut(sender_workflow_id) {
                persisted.workflow.close_outbound_channels_by_id(channel_id);
            }
        }
    }

    pub(super) fn channel_ids(&self, workflow_id: WorkflowId) -> Option<ChannelIds> {
        let persisted = &self.workflows.get(&workflow_id)?.workflow;
        Some(persisted.channel_ids())
    }

    // TODO: clearly inefficient
    #[tracing::instrument(level = "debug", skip(self), ret)]
    pub(super) fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>> {
        let timers = self
            .workflows
            .values()
            .flat_map(|persisted| persisted.workflow.timers());
        let expirations = timers.filter_map(|(_, state)| {
            if state.completed_at().is_none() {
                Some(state.definition().expires_at)
            } else {
                None
            }
        });
        expirations.min()
    }

    #[tracing::instrument(level = "debug", skip(self), ret)]
    pub(super) fn find_workflow_with_pending_tasks(&self) -> Option<WorkflowId> {
        self.workflows.iter().find_map(|(&workflow_id, persisted)| {
            let workflow = &persisted.workflow;
            if !workflow.is_initialized() || workflow.pending_events().next().is_some() {
                Some(workflow_id)
            } else {
                None
            }
        })
    }

    #[tracing::instrument(level = "debug", skip(self), ret)]
    pub(super) fn find_consumable_channel(&self) -> Option<(ChannelId, WorkflowId)> {
        let mut all_channels = self.workflows.iter().flat_map(|(&workflow_id, persisted)| {
            persisted
                .workflow
                .inbound_channels()
                .map(move |(_, _, state)| (workflow_id, state))
        });
        all_channels.find_map(|(workflow_id, state)| {
            if state.waits_for_message() {
                let channel_id = state.id();
                if !self.channels[&channel_id].messages.is_empty() {
                    return Some((channel_id, workflow_id));
                }
            }
            None
        })
    }

    #[tracing::instrument(
        skip_all,
        fields(tx.workflow_id = transaction.executing_workflow_id())
    )]
    pub(super) fn commit(&mut self, transaction: Transaction, receipt: &Receipt) {
        let executed_workflow_id = transaction.executing_workflow_id();
        let transaction = transaction.into_inner();
        debug_assert!(self.next_workflow_id <= transaction.next_workflow_id);
        self.next_workflow_id = transaction.next_workflow_id;
        debug_assert!(self.next_channel_id <= transaction.next_channel_id);
        self.next_channel_id = transaction.next_channel_id;

        // Create new channels and write outbound messages for them when appropriate.
        for (child_id, mut child_workflow) in transaction.new_workflows {
            let child_span = tracing::debug_span!("prepare_child", child_id);
            let _entered = child_span.enter();

            let channel_ids = child_workflow.workflow.channel_ids();
            tracing::debug!(?channel_ids, "handling channels for new workflow");

            for (name, &channel_id) in &channel_ids.inbound {
                let channel_state = self
                    .channels
                    .entry(channel_id)
                    .or_insert_with(|| ChannelState::new(executed_workflow_id, Some(child_id)));
                if channel_state.is_closed {
                    child_workflow.workflow.close_inbound_channel(None, name);
                }
                tracing::debug!(name, channel_id, ?channel_state, "prepared inbound channel");
            }
            for (name, &channel_id) in &channel_ids.outbound {
                let channel_state = self
                    .channels
                    .entry(channel_id)
                    .or_insert_with(|| ChannelState::new(Some(child_id), executed_workflow_id));
                if channel_state.is_closed {
                    child_workflow.workflow.close_outbound_channel(None, name);
                } else {
                    channel_state.sender_workflow_ids.insert(child_id);
                }
                tracing::debug!(
                    name,
                    channel_id,
                    ?channel_state,
                    "prepared outbound channel"
                );
            }

            self.workflows.insert(child_id, child_workflow);
        }

        for (channel_kind, channel_id) in receipt.closed_channel_ids() {
            let side = match channel_kind {
                ChannelKind::Inbound => ChannelSide::Receiver,
                ChannelKind::Outbound => ChannelSide::WorkflowSender(executed_workflow_id.unwrap()),
                // ^ `unwrap()` is safe: for "external" executions, the `receipt` is empty
            };
            self.close_channel_side(channel_id, side);
        }
    }

    pub(crate) fn push_messages(&mut self, messages: HashMap<ChannelId, Vec<Message>>) {
        for (channel_id, messages) in messages {
            let channel = self.channels.get_mut(&channel_id).unwrap();
            channel.push_messages(messages).ok(); // we're ok with messages getting dropped
        }
    }

    pub(super) fn persist_workflow(
        &mut self,
        id: WorkflowId,
        workflow: PersistedWorkflow,
        tracer: TracingEventReceiver,
    ) {
        let persisted = self.workflows.get_mut(&id).unwrap();
        persisted.workflow = workflow;
        let spawner_meta = self.spawners.get_mut(&persisted.definition_id).unwrap();
        tracer.persist_metadata(&mut spawner_meta.tracing_metadata);
        persisted.tracing_spans = tracer.persist_spans();
        self.handle_workflow_update(id);
    }

    pub(super) fn handle_workflow_update(&mut self, id: WorkflowId) {
        let persisted = self.workflows.remove(&id).unwrap();
        let workflow = &persisted.workflow;
        let completion_notification = if let Poll::Ready(result) = workflow.result() {
            let parent_id = persisted.parent_id;
            // Close all channels linked to the workflow.
            for (.., state) in workflow.inbound_channels() {
                self.close_channel_side(state.id(), ChannelSide::Receiver);
            }
            for (.., state) in workflow.outbound_channels() {
                self.close_channel_side(state.id(), ChannelSide::WorkflowSender(id));
            }
            parent_id.map(|id| (id, result.map_err(clone_join_error)))
        } else {
            self.workflows.insert(id, persisted);
            None
        };

        if let Some((parent_id, result)) = completion_notification {
            // The parent may be already completed, thus refutable matching.
            if let Some(persisted) = self.workflows.get_mut(&parent_id) {
                persisted.workflow.notify_on_child_completion(id, result);
            }
        }
    }
}
