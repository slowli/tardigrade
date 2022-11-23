//! Persistence for `WorkflowManager`.

use chrono::{DateTime, Utc};
use tracing_tunnel::PersistedSpans;

use std::{collections::HashMap, task::Poll};

use crate::{
    manager::ChannelSide,
    receipt::{ChannelEvent, ChannelEventKind, ExecutionError, Receipt},
    storage::{
        ActiveWorkflowState, ChannelRecord, CompletedWorkflowState, ErroneousMessageRef,
        StorageTransaction, WorkflowSelectionCriteria, WorkflowState, WorkflowWaker,
    },
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
        let state = match workflow.result() {
            Poll::Pending => {
                let has_internal_waker = workflow.pending_wakeup_causes().next().is_some();
                if has_internal_waker {
                    self.inner.insert_waker(id, WorkflowWaker::Internal).await;
                }
                WorkflowState::from(ActiveWorkflowState {
                    persisted: workflow,
                    tracing_spans,
                })
            }
            Poll::Ready(result) => WorkflowState::from(CompletedWorkflowState {
                result: result.map_err(clone_join_error),
            }),
        };
        self.inner.update_workflow(id, state).await;
    }

    async fn handle_workflow_update(
        &mut self,
        id: WorkflowId,
        parent_id: Option<WorkflowId>,
        workflow: &PersistedWorkflow,
    ) {
        let completion_receiver = if workflow.result().is_ready() {
            // Close all channels linked to the workflow.
            for (.., state) in workflow.inbound_channels() {
                self.close_channel_side(state.id(), ChannelSide::Receiver)
                    .await;
            }
            for (.., state) in workflow.outbound_channels() {
                self.close_channel_side(state.id(), ChannelSide::WorkflowSender(id))
                    .await;
            }
            parent_id
        } else {
            None
        };

        if let Some(parent_id) = completion_receiver {
            let waker = WorkflowWaker::ChildCompletion(id);
            self.inner.insert_waker(parent_id, waker).await;
        }
    }

    #[tracing::instrument(skip(self, error), fields(err = %error))]
    pub async fn persist_workflow_error(
        &mut self,
        workflow_id: WorkflowId,
        error: ExecutionError,
        erroneous_messages: Vec<ErroneousMessageRef>,
    ) {
        let state = self.inner.workflow(workflow_id).await.unwrap().state;
        let state = if let WorkflowState::Active(state) = state {
            state.with_error(error, erroneous_messages)
        } else {
            unreachable!()
        };
        self.inner.update_workflow(workflow_id, state.into()).await;
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
            self.inner.insert_waker(sender_workflow_id, waker).await;
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
            if let Some(ChannelEvent { kind, channel_id }) = event.as_channel_event() {
                return match kind {
                    ChannelEventKind::InboundChannelClosed => {
                        Some((ChannelKind::Inbound, *channel_id))
                    }
                    ChannelEventKind::OutboundChannelClosed {
                        remaining_alias_count: 0,
                    } => Some((ChannelKind::Outbound, *channel_id)),
                    _ => None,
                };
            }
            None
        })
    }
}
