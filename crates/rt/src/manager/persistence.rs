//! Persistence for `WorkflowManager`.

use chrono::{DateTime, Utc};
use tracing_tunnel::PersistedSpans;

use std::{collections::HashMap, task::Poll};

use crate::{
    manager::ChannelSide,
    receipt::{ChannelEvent, ChannelEventKind, Receipt},
    storage::{ChannelState, WorkflowSelectionCriteria, WriteChannels, WriteWorkflows},
    utils::{clone_join_error, Message},
    PersistedWorkflow,
};
use tardigrade::{interface::ChannelKind, ChannelId, WorkflowId};

impl ChannelState {
    fn close_side(&mut self, side: ChannelSide) {
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
                return;
            }
        }

        if !self.has_external_sender && self.sender_workflow_ids.is_empty() {
            self.is_closed = true;
        }
    }
}

#[derive(Debug)]
pub(super) struct StorageHelper<'a, T> {
    inner: &'a mut T,
}

impl<'a, T: WriteChannels + WriteWorkflows> StorageHelper<'a, T> {
    pub fn new(inner: &'a mut T) -> Self {
        Self { inner }
    }

    #[tracing::instrument(
        skip(self, workflow, tracing_spans),
        fields(
            workflow.result = ?workflow.result(),
            tracing_spans.len = tracing_spans.len()
        )
    )]
    pub async fn persist_workflow(
        &mut self,
        id: WorkflowId,
        parent_id: Option<WorkflowId>,
        workflow: PersistedWorkflow,
        tracing_spans: PersistedSpans,
    ) {
        self.handle_workflow_update(id, parent_id, &workflow).await;
        if workflow.result().is_pending() {
            self.inner
                .persist_workflow(id, workflow, tracing_spans)
                .await;
        }
    }

    /// Returns `true` if the workflow needs to be persisted (i.e., it's not completed).
    pub async fn handle_workflow_update(
        &mut self,
        id: WorkflowId,
        parent_id: Option<WorkflowId>,
        workflow: &PersistedWorkflow,
    ) {
        let completion_notification = if let Poll::Ready(result) = workflow.result() {
            // Close all channels linked to the workflow.
            for (.., state) in workflow.inbound_channels() {
                self.close_channel_side(state.id(), ChannelSide::Receiver)
                    .await;
            }
            for (.., state) in workflow.outbound_channels() {
                self.close_channel_side(state.id(), ChannelSide::WorkflowSender(id))
                    .await;
            }
            self.inner.delete_workflow(id).await;
            parent_id.map(|id| (id, result.map_err(clone_join_error)))
        } else {
            None
        };

        if let Some((parent_id, result)) = completion_notification {
            self.inner
                .manipulate_workflow(parent_id, |persisted| {
                    persisted.notify_on_child_completion(id, result);
                })
                .await;
        }
    }

    #[tracing::instrument(skip(self, receipt))]
    pub async fn close_channels(&mut self, workflow_id: WorkflowId, receipt: &Receipt) {
        for (channel_kind, channel_id) in receipt.closed_channel_ids() {
            let side = match channel_kind {
                ChannelKind::Inbound => ChannelSide::Receiver,
                ChannelKind::Outbound => ChannelSide::WorkflowSender(workflow_id),
            };
            self.close_channel_side(channel_id, side).await;
        }
    }

    // TODO: potential bottleneck (multiple workflows touched). Rework as tx outbox?
    #[tracing::instrument(skip(self))]
    pub async fn close_channel_side(&mut self, channel_id: ChannelId, side: ChannelSide) {
        let channel_state = self
            .inner
            .manipulate_channel(channel_id, |state| {
                tracing::debug!(?state, "current channel state");
                state.close_side(side);
            })
            .await;

        tracing::info!(is_closed = channel_state.is_closed, "channel closed");
        if !channel_state.is_closed {
            return;
        }

        // If the channel is closed by the receiver, no need to notify it again.
        if !matches!(side, ChannelSide::Receiver) {
            let has_messages = self.inner.has_messages(channel_id).await;
            if !has_messages {
                // We can notify the receiver immediately only if there are no unconsumed messages
                // in the channel.
                if let Some(receiver_workflow_id) = channel_state.receiver_workflow_id {
                    self.inner
                        .manipulate_workflow(receiver_workflow_id, |persisted| {
                            let (child_id, name) = persisted.find_inbound_channel(channel_id);
                            persisted.close_inbound_channel(child_id, &name);
                        })
                        .await;
                }
            }
        }

        for &sender_workflow_id in &channel_state.sender_workflow_ids {
            // The workflow may be missing from `self.workflows` if it has just completed.
            self.inner
                .manipulate_workflow(sender_workflow_id, |persisted| {
                    persisted.close_outbound_channels_by_id(channel_id);
                })
                .await;
        }
    }

    pub async fn push_messages(&mut self, messages: HashMap<ChannelId, Vec<Message>>) {
        for (channel_id, messages) in messages {
            let messages = messages.into_iter().map(Into::into).collect();
            self.inner.push_messages(channel_id, messages).await.ok();
            // we're ok with messages getting dropped
        }
    }

    pub async fn set_current_time(&mut self, time: DateTime<Utc>) {
        let criteria = WorkflowSelectionCriteria::HasTimerBefore(time);
        self.inner
            .manipulate_all_workflows(criteria, |persisted| persisted.set_current_time(time))
            .await;
    }
}

impl Receipt {
    fn closed_channel_ids(&self) -> impl Iterator<Item = (ChannelKind, ChannelId)> + '_ {
        self.events().filter_map(|event| {
            if let Some(ChannelEvent { kind, .. }) = event.as_channel_event() {
                return match kind {
                    ChannelEventKind::InboundChannelClosed(channel_id) => {
                        Some((ChannelKind::Inbound, *channel_id))
                    }
                    ChannelEventKind::OutboundChannelClosed {
                        channel_id,
                        remaining_alias_count: 0,
                    } => Some((ChannelKind::Outbound, *channel_id)),
                    _ => None,
                };
            }
            None
        })
    }
}

impl PersistedWorkflow {
    pub(super) fn find_inbound_channel(
        &self,
        channel_id: ChannelId,
    ) -> (Option<WorkflowId>, String) {
        self.inbound_channels()
            .find_map(|(child_id, name, state)| {
                if state.id() == channel_id {
                    Some((child_id, name.to_owned()))
                } else {
                    None
                }
            })
            .unwrap()
    }
}
