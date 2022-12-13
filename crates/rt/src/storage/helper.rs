//! Persistence for `WorkflowManager`.

use chrono::{DateTime, Utc};
use futures::{Future, FutureExt};
use tracing_tunnel::PersistedSpans;

use std::{
    collections::{HashMap, HashSet},
    error, fmt,
    task::Poll,
};

use crate::{
    receipt::{ChannelEvent, ChannelEventKind, ExecutionError, Receipt},
    storage::{
        ActiveWorkflowState, ChannelRecord, CompletedWorkflowState, ErroneousMessageRef,
        Storage, StorageTransaction, WorkflowSelectionCriteria, WorkflowState,
        WorkflowWaker,
    },
    utils::{clone_join_error, Message},
    PersistedWorkflow,
};
use tardigrade::{channel::SendError, handle::Handle, ChannelId, WorkflowId};

#[derive(Debug, Clone, Copy)]
pub(crate) enum ChannelSide {
    HostSender,
    WorkflowSender(WorkflowId),
    HostReceiver,
    Receiver(WorkflowId),
}

impl ChannelRecord {
    fn owned_by_host() -> Self {
        Self {
            receiver_workflow_id: None,
            sender_workflow_ids: HashSet::new(),
            has_external_sender: true,
            is_closed: false,
            received_messages: 0,
        }
    }

    fn owned_by_workflow(workflow_id: WorkflowId) -> Self {
        Self {
            receiver_workflow_id: Some(workflow_id),
            sender_workflow_ids: HashSet::from_iter([workflow_id]),
            has_external_sender: false,
            is_closed: false,
            received_messages: 0,
        }
    }

    fn close_side(&mut self, side: ChannelSide) {
        match side {
            ChannelSide::HostSender => {
                self.has_external_sender = false;
            }
            ChannelSide::WorkflowSender(id) => {
                self.sender_workflow_ids.remove(&id);
            }

            ChannelSide::Receiver(id) => {
                if self.receiver_workflow_id == Some(id) {
                    self.receiver_workflow_id = None;
                    self.is_closed = true;
                }
                return;
            }
            ChannelSide::HostReceiver => {
                if self.receiver_workflow_id.is_none() {
                    self.is_closed = true;
                }
                return;
            }
        }

        if !self.has_external_sender && self.sender_workflow_ids.is_empty() {
            self.is_closed = true;
        }
    }
}

/// Concurrency errors for modifying operations on workflows.
#[derive(Debug)]
pub struct ConcurrencyError(());

impl ConcurrencyError {
    pub(crate) fn new() -> Self {
        Self(())
    }
}

impl fmt::Display for ConcurrencyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("operation failed because of a concurrent edit")
    }
}

impl error::Error for ConcurrencyError {}

#[derive(Debug)]
pub(crate) struct StorageHelper<'a, T> {
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
            for (channel_id, _) in workflow.receivers() {
                self.close_channel_side(channel_id, ChannelSide::Receiver(id))
                    .await;
            }
            for (channel_id, _) in workflow.senders() {
                self.close_channel_side(channel_id, ChannelSide::WorkflowSender(id))
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
        let WorkflowState::Active(state) = state else { unreachable!() };
        let state = state.with_error(error, erroneous_messages);
        self.inner.update_workflow(workflow_id, state.into()).await;
    }

    #[tracing::instrument(skip(self, receipt))]
    pub async fn close_channels(&mut self, workflow_id: WorkflowId, receipt: &Receipt) {
        for id_handle in receipt.closed_channel_ids() {
            let side = match id_handle {
                Handle::Receiver(_) => ChannelSide::Receiver(workflow_id),
                Handle::Sender(_) => ChannelSide::WorkflowSender(workflow_id),
            };
            self.close_channel_side(id_handle.factor(), side).await;
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
            let waker = WorkflowWaker::SenderClosure(channel_id);
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
    fn closed_channel_ids(&self) -> impl Iterator<Item = Handle<ChannelId>> + '_ {
        self.events().filter_map(|event| {
            if let Some(ChannelEvent { kind, channel_id }) = event.as_channel_event() {
                return match kind {
                    ChannelEventKind::ReceiverClosed => Some(Handle::Receiver(*channel_id)),
                    ChannelEventKind::SenderClosed => Some(Handle::Sender(*channel_id)),
                    _ => None,
                };
            }
            None
        })
    }
}

pub(crate) fn send_message<S: Storage>(
    storage: &S,
    channel_id: ChannelId,
    message: Vec<u8>,
) -> impl Future<Output = Result<(), SendError>> + Send + '_ {
    // A separate function to make Rust properly infer that the returned future is `Send`
    // (otherwise, it's only `Send` for `S: 'static`, bizarrely enough).
    async fn do_send_message<T: StorageTransaction>(
        mut transaction: T,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), SendError> {
        let channel = transaction.channel(channel_id).await.unwrap();
        if channel.is_closed {
            return Err(SendError::Closed);
        }

        transaction.push_messages(channel_id, vec![message]).await;
        transaction.commit().await;
        Ok(())
    }

    storage
        .transaction()
        .then(move |transaction| do_send_message(transaction, channel_id, message))
}

pub(crate) fn close_host_sender<S: Storage>(
    storage: &S,
    channel_id: ChannelId,
) -> impl Future<Output = ()> + Send + '_ {
    async fn do_close_sender<T: StorageTransaction>(
        mut transaction: T,
        channel_id: ChannelId
    ) {
        StorageHelper::new(&mut transaction)
            .close_channel_side(channel_id, ChannelSide::HostSender)
            .await;
        transaction.commit().await;
    }

    storage
        .transaction()
        .then(move |transaction| do_close_sender(transaction, channel_id))
}

pub(crate) fn close_host_receiver<S: Storage>(
    storage: &S,
    channel_id: ChannelId,
) -> impl Future<Output = ()> + Send + '_ {
    async fn do_close_receiver<T: StorageTransaction>(
        mut transaction: T,
        channel_id: ChannelId
    ) {
        StorageHelper::new(&mut transaction)
            .close_channel_side(channel_id, ChannelSide::HostReceiver)
            .await;
        transaction.commit().await;
    }

    storage
        .transaction()
        .then(move |transaction| do_close_receiver(transaction, channel_id))
}

/// Aborts the workflow with the specified ID. The parent workflow, if any, will be notified,
/// and all channel handles owned by the workflow will be properly disposed.
#[tracing::instrument(skip(storage))]
pub(crate) fn abort_workflow<S: Storage>(
    storage: &S,
    workflow_id: WorkflowId,
) -> impl Future<Output = Result<(), ConcurrencyError>> + Send + '_ {
    async fn do_abort<T: StorageTransaction>(
        mut transaction: T,
        workflow_id: WorkflowId,
    ) -> Result<(), ConcurrencyError> {
        let record = transaction
            .workflow_for_update(workflow_id)
            .await
            .ok_or_else(ConcurrencyError::new)?;
        let (parent_id, mut persisted) = match record.state {
            WorkflowState::Active(state) => (record.parent_id, state.persisted),
            WorkflowState::Errored(state) => (record.parent_id, state.persisted),
            WorkflowState::Completed(_) => return Err(ConcurrencyError::new()),
        };

        persisted.abort();
        let spans = PersistedSpans::default();
        StorageHelper::new(&mut transaction)
            .persist_workflow(workflow_id, parent_id, persisted, spans)
            .await;
        transaction.commit().await;
        Ok(())
    }

    storage.transaction().then(move |transaction| do_abort(transaction, workflow_id))
}

#[tracing::instrument(skip(storage))]
pub(crate) fn repair_workflow<S: Storage>(
    storage: &S,
    workflow_id: WorkflowId,
) -> impl Future<Output = Result<(), ConcurrencyError>> + Send + '_ {
    // See the comment for `send_message`.
    async fn do_repair_workflow<T: StorageTransaction>(
        mut transaction: T,
        workflow_id: WorkflowId,
    ) -> Result<(), ConcurrencyError> {
        let record = transaction
            .workflow_for_update(workflow_id)
            .await
            .ok_or_else(ConcurrencyError::new)?;
        let record = record.into_errored().ok_or_else(ConcurrencyError::new)?;
        let repaired_state = record.state.repair();
        transaction
            .update_workflow(workflow_id, repaired_state.into())
            .await;
        transaction.commit().await;
        Ok(())
    }

    storage.transaction().then(move |transaction| {
        do_repair_workflow(transaction, workflow_id)
    })
}

#[tracing::instrument(skip(storage))]
pub(crate) fn drop_message<'s, S: Storage>(
    storage: &'s S,
    workflow_id: WorkflowId,
    message_ref: &'s ErroneousMessageRef,
) -> impl Future<Output = Result<(), ConcurrencyError>> + Send + 's {
    // See the comment for `repair_workflow` for why this is required.
    async fn do_drop_message<T: StorageTransaction>(
        mut transaction: T,
        workflow_id: WorkflowId,
        message_ref: &ErroneousMessageRef,
    ) -> Result<(), ConcurrencyError> {
        let record = transaction
            .workflow_for_update(workflow_id)
            .await
            .ok_or_else(ConcurrencyError::new)?;
        let mut record = record.into_errored().ok_or_else(ConcurrencyError::new)?;
        let persisted = &mut record.state.persisted;
        let state = persisted.receiver(message_ref.channel_id).unwrap();
        if state.received_message_count() == message_ref.index {
            persisted.drop_message_for_receiver(message_ref.channel_id);
            transaction
                .update_workflow(workflow_id, record.state.into())
                .await;
        } else {
            tracing::warn!(?message_ref, workflow_id, "failed dropping inbound message");
            return Err(ConcurrencyError::new());
        }
        transaction.commit().await;
        Ok(())
    }

    storage.transaction().then(move |transaction| {
        do_drop_message(transaction, workflow_id, message_ref)
    })
}

pub(crate) async fn commit_channel<T: StorageTransaction>(
    executed_workflow_id: Option<WorkflowId>,
    transaction: &mut T,
) -> (ChannelId, ChannelRecord) {
    let channel_id = transaction.allocate_channel_id().await;
    let state = executed_workflow_id.map_or_else(
        ChannelRecord::owned_by_host,
        ChannelRecord::owned_by_workflow,
    );
    transaction.insert_channel(channel_id, state.clone()).await;
    (channel_id, state)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>(value: T) -> T {
        value
    }

    #[allow(dead_code)]
    async fn helper_futures_are_send<S: Storage>(storage: &S) {
        assert_send(send_message(storage, 1, b"test".to_vec())).await.ok();
        assert_send(close_host_sender(storage, 1)).await;
        assert_send(close_host_receiver(storage, 1)).await;
        assert_send(abort_workflow(storage, 1)).await.ok();
        assert_send(repair_workflow(storage, 1)).await.ok();

        let message_ref = ErroneousMessageRef {
            channel_id: 1,
            index: 0,
        };
        assert_send(drop_message(storage, 1, &message_ref)).await.ok();
    }
}
