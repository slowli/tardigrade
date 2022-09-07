//! Persistence for `WorkflowManager`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use std::{
    collections::{HashMap, VecDeque},
    mem,
};

use crate::{
    manager::{transaction::Transaction, ChannelInfo},
    receipt::Receipt,
    utils::Message,
    workflow::{ChannelIds, Workflow},
    PersistedWorkflow,
};
use tardigrade_shared::{ChannelId, SendError, WorkflowId};

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ChannelState {
    pub receiver_workflow_id: Option<WorkflowId>, // `None` means external receiver
    pub is_closed: bool,
    pub messages: VecDeque<Message>,
    pub next_message_idx: usize,
}

impl ChannelState {
    fn new(receiver_workflow_id: Option<WorkflowId>) -> Self {
        Self {
            receiver_workflow_id,
            is_closed: false,
            messages: VecDeque::new(),
            next_message_idx: 0,
        }
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
        Self {
            workflows: HashMap::new(),
            channels: HashMap::new(),
            next_workflow_id: 0,
            next_channel_id: 1, // avoid "pre-closed" channel ID
        }
    }
}

impl PersistedWorkflows {
    pub(super) fn take_message(&mut self, channel_id: ChannelId) -> Option<Message> {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.pop_message()
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

    pub(super) fn close_channel_sender(&mut self, channel_id: ChannelId) {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.close();
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
        self.next_workflow_id = transaction.next_workflow_id;
        self.next_channel_id = transaction.next_channel_id;

        // Create new channels and write outbound messages for them when appropriate.
        for (child_id, child_workflow) in transaction.new_workflows {
            let channel_ids = child_workflow.workflow.channel_ids();
            for &channel_id in channel_ids.inbound.values() {
                self.channels
                    .entry(channel_id)
                    .or_insert_with(|| ChannelState::new(Some(child_id)));
            }
            for &channel_id in channel_ids.outbound.values() {
                self.channels
                    .entry(channel_id)
                    .or_insert_with(|| ChannelState::new(executed_workflow_id));
            }

            self.workflows.insert(child_id, child_workflow);
        }

        for channel_id in receipt.closed_channel_ids() {
            self.channels.get_mut(&channel_id).unwrap().close();
        }
    }

    pub(super) fn drain_and_persist_workflow(
        &mut self,
        id: WorkflowId,
        definition_id: Option<String>,
        mut workflow: Workflow,
    ) {
        // Drain outbound messages generated by the executed workflow. At this point,
        // new channels are already created.
        for (channel_id, messages) in workflow.drain_messages() {
            let channel = self.channels.get_mut(&channel_id).unwrap();
            channel.push_messages(messages).ok(); // we're ok with messages getting dropped
        }

        // Since all outbound messages are drained, persisting the workflow is safe.
        let workflow = workflow.persist().unwrap();
        if let Some(persisted) = self.workflows.get_mut(&id) {
            persisted.workflow = workflow;
        } else {
            self.workflows.insert(
                id,
                WorkflowWithMeta {
                    definition_id: definition_id.expect("no `definition_id` provided"),
                    workflow,
                },
            );
        }
    }
}
