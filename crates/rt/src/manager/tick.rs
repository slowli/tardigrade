//! Tick logic for `WorkflowManager`.

use chrono::{DateTime, Utc};
use tracing::field;
use tracing_tunnel::TracingEventReceiver;

use std::{error, fmt};

use super::{new_workflows::NewWorkflows, persistence::StorageHelper, AsManager, WorkflowManager};
use crate::{
    module::{Clock, Services},
    receipt::{ExecutionError, Receipt},
    storage::{
        MessageError, ReadChannels, ReadModules, ReadWorkflows, Storage, StorageTransaction,
        WorkflowRecord, WorkflowWaker, WorkflowWakerRecord, WriteModules, WriteWorkflowWakers,
        WriteWorkflows,
    },
    workflow::Workflow,
    PersistedWorkflow, WorkflowSpawner,
};
use tardigrade::{ChannelId, WorkflowId};

#[derive(Debug)]
struct DropMessageAction {
    channel_id: ChannelId,
    message_idx: usize,
}

impl DropMessageAction {
    async fn execute<M: AsManager>(self, manager: &M, workflow_id: WorkflowId) {
        let mut transaction = manager.as_manager().storage.transaction().await;
        transaction
            .manipulate_workflow(workflow_id, |persisted| {
                let (child_id, name, state) = persisted.find_inbound_channel(self.channel_id);
                if state.received_message_count() == self.message_idx {
                    let name = name.to_owned();
                    persisted.drop_inbound_message(child_id, &name);
                } else {
                    tracing::warn!(?self, workflow_id, "failed dropping inbound message");
                }
            })
            .await;
        transaction.commit().await;
    }
}

/// Result of [ticking](WorkflowManager::tick()) a [`WorkflowManager`].
#[derive(Debug)]
#[must_use = "Result can contain an execution error which should be handled"]
pub struct TickResult<T> {
    workflow_id: WorkflowId,
    result: Result<Receipt, ExecutionError>,
    extra: T,
}

impl<T> TickResult<T> {
    /// Returns the ID of the executed workflow.
    pub fn workflow_id(&self) -> WorkflowId {
        self.workflow_id
    }

    /// Returns a reference to the underlying execution result.
    #[allow(clippy::missing_errors_doc)] // doesn't make sense semantically
    pub fn as_ref(&self) -> Result<&Receipt, &ExecutionError> {
        self.result.as_ref()
    }

    /// Returns the underlying execution result.
    #[allow(clippy::missing_errors_doc)] // doesn't make sense semantically
    pub fn into_inner(self) -> Result<Receipt, ExecutionError> {
        self.result
    }

    pub(crate) fn drop_extra(self) -> TickResult<()> {
        TickResult {
            workflow_id: self.workflow_id,
            result: self.result,
            extra: (),
        }
    }
}

/// Actions that can be performed on a [`WorkflowManager`]. Used within the [`TickResult`]
/// wrapper.
#[derive(Debug)]
pub struct Actions<'a, M> {
    manager: &'a M,
    abort_action: bool,
    drop_message_action: Option<DropMessageAction>,
}

impl<'a, M> Actions<'a, M> {
    fn new(manager: &'a M, abort_action: bool) -> Self {
        Self {
            manager,
            abort_action,
            drop_message_action: None,
        }
    }

    fn allow_dropping_message(&mut self, channel_id: ChannelId, message_idx: usize) {
        self.drop_message_action = Some(DropMessageAction {
            channel_id,
            message_idx,
        });
    }
}

impl<'a, M: AsManager> TickResult<Actions<'a, M>> {
    /// Returns true if the workflow execution failed and the execution was triggered
    /// by consuming a message from a certain channel.
    pub fn can_drop_erroneous_message(&self) -> bool {
        self.extra.drop_message_action.is_some()
    }

    /// Aborts the erroneous workflow. Messages emitted by the workflow during the erroneous
    /// execution are discarded; same with the workflows spawned during the execution.
    ///
    /// # Panics
    ///
    /// Panics if the execution is successful.
    pub async fn abort_workflow(self) -> TickResult<()> {
        assert!(
            self.extra.abort_action,
            "cannot abort workflow because it did not error"
        );
        self.extra
            .manager
            .as_manager()
            .abort_workflow(self.workflow_id)
            .await;
        self.drop_extra()
    }

    /// Drops a message that led to erroneous workflow execution.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::can_drop_erroneous_message()`] returns `false`.
    pub async fn drop_erroneous_message(mut self) -> TickResult<()> {
        let action = self
            .extra
            .drop_message_action
            .take()
            .expect("cannot drop message");
        action.execute(self.extra.manager, self.workflow_id).await;
        self.drop_extra()
    }
}

/// An error returned by [`WorkflowManager::tick()`] if the manager cannot progress.
#[derive(Debug)]
pub struct WouldBlock {
    nearest_timer_expiration: Option<DateTime<Utc>>,
}

impl WouldBlock {
    /// Returns the nearest timer expiration for the workflows within the manager.
    pub fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>> {
        self.nearest_timer_expiration
    }
}

impl fmt::Display for WouldBlock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "cannot progress workflow manager")?;
        if let Some(expiration) = self.nearest_timer_expiration {
            write!(formatter, " (nearest timer expiration: {})", expiration)?;
        }
        Ok(())
    }
}

impl error::Error for WouldBlock {}

#[derive(Debug)]
struct PendingChannel {
    child_id: Option<WorkflowId>,
    name: String,
    channel_id: ChannelId,
    message_idx: usize,
    waits_for_message: bool,
}

impl PendingChannel {
    fn take_pending_message(&self, workflow: &mut Workflow<'_>) {
        if workflow.take_pending_inbound_message(self.child_id, &self.name) {
            // The message was not consumed. We still persist the workflow in order to
            // consume wakers (otherwise, we would loop indefinitely), and place the message
            // back to the channel.
            tracing::warn!(
                ?self,
                "message was not consumed by workflow; placing the message back"
            );
        }
    }
}

/// Temporary value holding parts necessary to restore a `Workflow`.
#[derive(Debug)]
struct WorkflowSeed<'a> {
    clock: &'a dyn Clock,
    spawner: &'a WorkflowSpawner<()>,
    persisted: PersistedWorkflow,
}

impl<'a> WorkflowSeed<'a> {
    fn apply_wakers(&mut self, waker_records: Vec<WorkflowWakerRecord>) {
        for record in waker_records {
            match record.waker {
                WorkflowWaker::Internal => {
                    // Handled separately; no modifications are required
                }
                WorkflowWaker::Timer(timer) => {
                    self.persisted.set_current_time(timer);
                }
                WorkflowWaker::OutboundChannelClosure(channel_id) => {
                    self.persisted.close_outbound_channels_by_id(channel_id);
                }
                WorkflowWaker::ChildCompletion(child_id, result) => {
                    self.persisted.notify_on_child_completion(child_id, result);
                }
            }
        }
    }

    // TODO: consider pushing messages to all channels or making this configurable.
    async fn update_inbound_channels<T: ReadChannels>(
        &mut self,
        transaction: &T,
    ) -> Option<PendingChannel> {
        let channels = self.persisted.inbound_channels();
        let pending_channels = channels.filter_map(|(child_id, name, state)| {
            if state.is_closed() {
                None
            } else {
                Some(PendingChannel {
                    child_id,
                    name: name.to_owned(),
                    channel_id: state.id(),
                    message_idx: state.received_message_count(),
                    waits_for_message: state.waits_for_message(),
                })
            }
        });
        let pending_channels: Vec<_> = pending_channels.collect();

        let mut pending_channel = None;
        for pending in pending_channels {
            let message_or_eof = transaction
                .channel_message(pending.channel_id, pending.message_idx)
                .await;
            match message_or_eof {
                Ok(message) if pending.waits_for_message && pending_channel.is_none() => {
                    self.persisted
                        .push_inbound_message(pending.child_id, &pending.name, message)
                        .unwrap();
                    // ^ `unwrap()` is safe: no messages can be persisted
                    pending_channel = Some(pending);
                }
                Err(MessageError::NonExistingIndex { is_closed: true }) => {
                    // Signal to the workflow that the channel is closed. This can be performed
                    // on a persisted workflow, without executing it.
                    self.persisted
                        .close_inbound_channel(pending.child_id, &pending.name);
                }
                _ => {
                    // Skip processing for now: we want to pinpoint consumption-related
                    // errors should they occur.
                }
            }
        }
        pending_channel
    }

    fn restore(
        self,
        workflows: &'a mut NewWorkflows,
        tracer: &'a mut TracingEventReceiver,
    ) -> Workflow<'a> {
        let services = Services {
            clock: self.clock,
            workflows: Some(workflows),
            tracer: Some(tracer),
        };
        self.persisted.restore(self.spawner, services).unwrap()
    }
}

impl<'a, C: Clock, S: Storage<'a>> WorkflowManager<C, S> {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            id = record.id,
            module_id = record.module_id,
            name_in_module = record.name_in_module
        )
    )]
    async fn restore_workflow(
        &'a self,
        persistence: &S::Transaction,
        record: WorkflowRecord,
    ) -> (WorkflowSeed<'_>, TracingEventReceiver) {
        let spawner = self.spawners.get(&record.module_id, &record.name_in_module);
        tracing::debug!(?spawner, "using spawner to restore workflow");

        let tracing_metadata = persistence
            .module(&record.module_id)
            .await
            .unwrap()
            .tracing_metadata;
        let local_spans = self
            .local_spans
            .lock()
            .await
            .remove(&record.id)
            .unwrap_or_default();
        let tracer = TracingEventReceiver::new(tracing_metadata, record.tracing_spans, local_spans);
        let template = WorkflowSeed {
            clock: &self.clock,
            spawner,
            persisted: record.persisted,
        };
        (template, tracer)
    }

    #[tracing::instrument(skip_all)]
    async fn tick_workflow(
        &'a self,
        transaction: &mut S::Transaction,
        workflow: WorkflowRecord,
        waker_records: Vec<WorkflowWakerRecord>,
    ) -> (Result<Receipt, ExecutionError>, Option<PendingChannel>) {
        let workflow_id = workflow.id;
        let parent_id = workflow.parent_id;
        let module_id = workflow.module_id.clone();
        let (mut template, mut tracer) = self.restore_workflow(transaction, workflow).await;
        template.apply_wakers(waker_records);
        let pending_channel = template.update_inbound_channels(transaction).await;
        let mut children = NewWorkflows::new(Some(workflow_id), self.shared());
        let mut workflow = template.restore(&mut children, &mut tracer);

        let result = workflow.tick();
        if let Ok(receipt) = &result {
            if let Some(pending) = &pending_channel {
                pending.take_pending_message(&mut workflow);
            }

            let span = tracing::debug_span!("persist_workflow_and_messages", workflow_id);
            let _entered = span.enter();
            let messages = workflow.drain_messages();
            let mut persisted = workflow.persist();
            children.commit(transaction, &mut persisted).await;
            let tracing_metadata = tracer.persist_metadata();
            let (spans, local_spans) = tracer.persist();

            let mut persistence = StorageHelper::new(transaction);
            persistence.push_messages(messages).await;
            persistence
                .persist_workflow(workflow_id, parent_id, persisted, spans)
                .await;
            persistence.close_channels(workflow_id, receipt).await;

            self.local_spans
                .lock()
                .await
                .insert(workflow_id, local_spans);
            transaction
                .update_tracing_metadata(&module_id, tracing_metadata)
                .await;
        } else {
            let err = result.as_ref().unwrap_err();
            tracing::warn!(%err, "workflow execution failed");
        }

        (result, pending_channel)
    }

    /// Attempts to advance a single workflow within this manager.
    ///
    /// A workflow can be advanced if it has outstanding wakers, e.g., ones produced by
    /// [expiring timers](Self::set_current_time()) or by flushing outbound messages
    /// during previous workflow executions. Alternatively, there is an inbound message
    /// that can be consumed by the workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the manager cannot be progressed.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub async fn tick(&'a self) -> Result<TickResult<Actions<'a, Self>>, WouldBlock> {
        let span = tracing::info_span!("tick", workflow_id = field::Empty);
        let _entered = span.enter();
        let mut transaction = self.storage.transaction().await;

        let waker_records = transaction.delete_wakers_for_single_workflow().await;
        let workflow_id = if let Some(waker) = waker_records.first() {
            waker.workflow_id
        } else if let Some((_, channel)) = transaction.find_consumable_channel().await {
            channel.receiver_workflow_id.unwrap()
        } else {
            let err = WouldBlock {
                nearest_timer_expiration: transaction.nearest_timer_expiration().await,
            };
            tracing::info!(%err, "workflow manager blocked");
            return Err(err);
        };

        let workflow = transaction.workflow(workflow_id).await.unwrap();
        span.record("workflow_id", workflow_id);

        let (result, pending_channel) = self
            .tick_workflow(&mut transaction, workflow, waker_records)
            .await;
        transaction.commit().await;
        let mut actions = Actions::new(self, result.is_err());
        if result.is_err() {
            if let Some(pending) = pending_channel {
                actions.allow_dropping_message(pending.channel_id, pending.message_idx);
            }
        }

        Ok(TickResult {
            workflow_id,
            extra: actions,
            result,
        })
    }
}
