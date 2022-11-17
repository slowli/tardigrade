//! Persistence for `WorkflowManager`.

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use tracing_tunnel::PersistedSpans;

use std::{collections::HashMap, task::Poll};

use crate::storage::WorkflowWaker;
use crate::{
    manager::{AsManager, ChannelSide},
    receipt::{ChannelEvent, ChannelEventKind, Receipt},
    storage::{ChannelRecord, Storage, StorageTransaction, WorkflowSelectionCriteria},
    utils::{clone_join_error, Message},
    PersistedWorkflow,
};
use tardigrade::{interface::ChannelKind, ChannelId, WorkflowId};

impl ChannelRecord {
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

pub(super) async fn in_transaction<'a, M, F, R>(manager: &'a M, action: F) -> R
where
    M: AsManager,
    F: for<'tx> FnOnce(&'tx mut <M::Storage as Storage<'a>>::Transaction) -> BoxFuture<'tx, R>,
{
    let mut transaction = manager.as_manager().storage.transaction().await;
    let output = action(&mut transaction).await;
    transaction.commit().await;
    output
}

#[derive(Debug)]
pub(super) struct StorageHelper<'a, T> {
    inner: &'a mut T,
}

impl<'a, T: StorageTransaction> StorageHelper<'a, T> {
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
            let has_internal_waker = workflow.pending_wakeup_causes().next().is_some();
            if has_internal_waker {
                self.inner
                    .insert_waker(WorkflowWaker::Internal.for_workflow(id))
                    .await;
            }

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
            let waker = WorkflowWaker::ChildCompletion(id, result);
            self.inner.insert_waker(waker.for_workflow(parent_id)).await;
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

        for &sender_workflow_id in &channel_state.sender_workflow_ids {
            let waker = WorkflowWaker::OutboundChannelClosure(channel_id);
            self.inner
                .insert_waker(waker.for_workflow(sender_workflow_id))
                .await;
        }
    }

    pub async fn push_messages(&mut self, messages: HashMap<ChannelId, Vec<Message>>) {
        for (channel_id, messages) in messages {
            let messages = messages.into_iter().map(Into::into).collect();
            self.inner.push_messages(channel_id, messages).await;
        }
    }

    pub async fn set_current_time(&mut self, time: DateTime<Utc>) {
        let criteria = WorkflowSelectionCriteria::HasTimerBefore(time);
        let waker = WorkflowWaker::Timer(time);
        self.inner
            .insert_waker_for_matching_workflows(criteria, waker)
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
