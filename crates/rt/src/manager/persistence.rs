//! Persistence for `WorkflowManager`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem,
};

use crate::{
    manager::{transaction::Transaction, ChannelClosureCause, ChannelInfo},
    receipt::Receipt,
    utils::Message,
    workflow::{ChannelIds, Workflow},
    PersistedWorkflow,
};
use tardigrade_shared::{ChannelId, SendError, WorkflowId};

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ChannelState {
    pub receiver_workflow_id: Option<WorkflowId>, // `None` means external receiver
    pub sender_workflow_ids: HashSet<WorkflowId>,
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
            is_closed: false,
            messages: VecDeque::new(),
            next_message_idx: 0,
        }
    }

    fn closed() -> Self {
        let mut this = Self::new(None, None);
        this.close();
        this
    }

    pub(super) fn info(&self) -> ChannelInfo {
        ChannelInfo {
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

    fn can_provide_message(&self) -> bool {
        !self.messages.is_empty() || self.is_closed
    }

    fn pop_message(&mut self) -> Option<Message> {
        self.messages.pop_front()
    }

    pub fn drain_messages(&mut self) -> (usize, VecDeque<Message>) {
        let start_idx = self.next_message_idx - self.messages.len();
        (start_idx, mem::take(&mut self.messages))
    }

    fn close(&mut self) {
        self.is_closed = true;
    }
}

/// Wrapper for either ongoing or persisted workflow.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct WorkflowWithMeta {
    pub definition_id: String,
    pub parent_id: Option<WorkflowId>,
    pub workflow: PersistedWorkflow,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedWorkflows {
    pub(super) workflows: HashMap<WorkflowId, WorkflowWithMeta>,
    pub(super) channels: HashMap<ChannelId, ChannelState>,
    pub(super) next_workflow_id: WorkflowId,
    pub(super) next_channel_id: ChannelId,
}

impl Default for PersistedWorkflows {
    fn default() -> Self {
        let mut channels = HashMap::with_capacity(1);
        channels.insert(0, ChannelState::closed());
        Self {
            workflows: HashMap::new(),
            channels,
            next_workflow_id: 0,
            next_channel_id: 1, // avoid "pre-closed" channel ID
        }
    }
}

impl PersistedWorkflows {
    pub(super) fn take_message(&mut self, channel_id: ChannelId) -> Option<(Message, bool)> {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state
            .pop_message()
            .map(|message| (message, channel_state.is_closed))
    }

    pub(super) fn revert_taking_message(&mut self, channel_id: ChannelId, message: Message) {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.messages.push_front(message);
    }

    pub(super) fn send_message(
        &mut self,
        channel_id: ChannelId,
        message: Message,
    ) -> Result<(), SendError> {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.push_messages([message])
    }

    pub(super) fn close_channel(&mut self, channel_id: ChannelId, cause: ChannelClosureCause) {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.close();

        // If the channel is closed by the receiver, no need to notify it again.
        if !matches!(cause, ChannelClosureCause::Receiver) && channel_state.messages.is_empty() {
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
            let workflow = &mut self.workflows.get_mut(sender_workflow_id).unwrap().workflow;
            workflow.close_outbound_channels_by_id(channel_id);
        }
    }

    pub(super) fn channel_ids(&self, workflow_id: WorkflowId) -> Option<ChannelIds> {
        let persisted = &self.workflows.get(&workflow_id)?.workflow;
        Some(persisted.channel_ids())
    }

    // TODO: clearly inefficient
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
                if self.channels[&channel_id].can_provide_message() {
                    return Some((channel_id, workflow_id));
                }
            }
            None
        })
    }

    pub(super) fn commit(&mut self, transaction: Transaction, receipt: &Receipt) {
        let executed_workflow_id = transaction.executing_workflow_id();
        let transaction = transaction.into_inner();
        debug_assert!(self.next_workflow_id <= transaction.next_workflow_id);
        self.next_workflow_id = transaction.next_workflow_id;
        debug_assert!(self.next_channel_id <= transaction.next_channel_id);
        self.next_channel_id = transaction.next_channel_id;

        // Create new channels and write outbound messages for them when appropriate.
        for (child_id, mut child_workflow) in transaction.new_workflows {
            let channel_ids = child_workflow.workflow.channel_ids();
            for (name, &channel_id) in &channel_ids.inbound {
                let channel_state = self
                    .channels
                    .entry(channel_id)
                    .or_insert_with(|| ChannelState::new(executed_workflow_id, Some(child_id)));
                if channel_state.is_closed {
                    child_workflow.workflow.close_inbound_channel(None, name);
                }
            }
            for (name, &channel_id) in &channel_ids.outbound {
                let channel_state = self
                    .channels
                    .entry(channel_id)
                    .or_insert_with(|| ChannelState::new(Some(child_id), executed_workflow_id));
                if channel_state.is_closed {
                    child_workflow.workflow.close_outbound_channel(None, name);
                }
            }

            self.workflows.insert(child_id, child_workflow);
        }

        for channel_id in receipt.closed_channel_ids() {
            self.close_channel(channel_id, ChannelClosureCause::Receiver);
        }
    }

    pub(super) fn drain_and_persist_workflow(&mut self, id: WorkflowId, mut workflow: Workflow) {
        // Drain outbound messages generated by the executed workflow. At this point,
        // new channels are already created.
        for (channel_id, messages) in workflow.drain_messages() {
            let channel = self.channels.get_mut(&channel_id).unwrap();
            channel.push_messages(messages).ok(); // we're ok with messages getting dropped
        }

        // Since all outbound messages are drained, persisting the workflow is safe.
        let workflow = workflow.persist().unwrap();
        let mut completion_notification_receiver = None;
        let persisted = self.workflows.get_mut(&id).unwrap();
        if workflow.is_finished() {
            completion_notification_receiver = persisted.parent_id;
            // Close all channels linked to the workflow.
            for (.., state) in workflow.inbound_channels() {
                self.close_channel(state.id(), ChannelClosureCause::Receiver);
            }
            // FIXME: close outbound channels as well
            self.workflows.remove(&id);
        } else {
            persisted.workflow = workflow;
        }

        if let Some(parent_id) = completion_notification_receiver {
            let persisted = self.workflows.get_mut(&parent_id).unwrap();
            persisted.workflow.notify_on_child_completion(id, Ok(()));
        }
    }
}
