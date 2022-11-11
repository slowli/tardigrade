//! Tick logic for `WorkflowManager`.

use chrono::{DateTime, Utc};
use tracing::field;
use tracing_tunnel::TracingEventReceiver;

use std::{error, fmt};

use super::{new_workflows::NewWorkflows, persistence::StorageHelper, AsManager, WorkflowManager};
use crate::storage::MessageError;
use crate::{
    module::{Clock, Services},
    receipt::{ExecutionError, Receipt},
    storage::{
        ReadChannels, ReadModules, ReadWorkflows, Storage, StorageTransaction, WorkflowRecord,
        WriteModules, WriteWorkflows,
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

/// Temporary value holding parts necessary to restore a `Workflow`.
#[derive(Debug)]
struct WorkflowSeed<'a> {
    clock: &'a dyn Clock,
    spawner: &'a WorkflowSpawner<()>,
    persisted: PersistedWorkflow,
}

impl<'a> WorkflowSeed<'a> {
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

    /// Atomically pops a message from a channel and feeds it to a listening workflow.
    /// If the workflow execution is successful, its results are committed to the manager state;
    /// otherwise, the results are reverted.
    #[tracing::instrument(skip(self, transaction, workflow), err)]
    pub(super) async fn feed_message_to_workflow(
        &'a self,
        transaction: &mut S::Transaction,
        channel_id: ChannelId,
        message_idx: usize,
        mut workflow: WorkflowRecord,
    ) -> Result<Receipt, ExecutionError> {
        let workflow_id = workflow.id;
        let (child_id, channel_name, state) = workflow.persisted.find_inbound_channel(channel_id);
        let channel_name = channel_name.to_owned();
        debug_assert_eq!(message_idx, state.received_message_count());

        let message_or_eof = transaction.channel_message(channel_id, message_idx).await;
        let message = match message_or_eof {
            Ok(message) => message,
            Err(MessageError::NonExistingIndex { is_closed: true }) => {
                // Signal to the workflow that the channel is closed. This can be performed
                // on a persisted workflow, without executing it.
                let mut persisted = workflow.persisted;
                persisted.close_inbound_channel(child_id, &channel_name);
                transaction
                    .persist_workflow(workflow_id, persisted, workflow.tracing_spans)
                    .await;
                return Ok(Receipt::default());
            }
            Err(err) => panic!("unexpected error receiving message: {err}"),
        };
        workflow
            .persisted
            .push_inbound_message(child_id, &channel_name, message)
            .unwrap();
        // ^ `unwrap()` is safe: no messages can be persisted

        let parent_id = workflow.parent_id;
        let module_id = workflow.module_id.clone();
        let (seed, mut tracer) = self.restore_workflow(transaction, workflow).await;
        let mut new_workflows = NewWorkflows::new(Some(workflow_id), self.shared());
        let mut workflow = seed.restore(&mut new_workflows, &mut tracer);
        let result = workflow.tick();

        if let Ok(receipt) = &result {
            if workflow.take_pending_inbound_message(child_id, &channel_name) {
                // The message was not consumed. We still persist the workflow in order to
                // consume wakers (otherwise, we would loop indefinitely), and place the message
                // back to the channel.
                tracing::warn!("message was not consumed by workflow; placing the message back");
            }

            let span = tracing::debug_span!("persist_workflow_and_messages", workflow_id);
            let _entered = span.enter();
            let messages = workflow.drain_messages();
            let mut persisted = workflow.persist();
            new_workflows.commit(transaction, &mut persisted).await;
            let tracing_metadata = tracer.persist_metadata();
            let (spans, local_spans) = tracer.persist();

            let mut persistence = StorageHelper::new(transaction);
            persistence
                .persist_workflow(workflow_id, parent_id, persisted, spans)
                .await;
            persistence.close_channels(workflow_id, receipt).await;
            persistence.push_messages(messages).await;

            self.local_spans
                .lock()
                .await
                .insert(workflow_id, local_spans);
            transaction
                .update_tracing_metadata(&module_id, tracing_metadata)
                .await;
        } else {
            // Do not commit the execution result.
        }
        result
    }

    #[tracing::instrument(skip_all, err)]
    pub(crate) async fn tick_workflow(
        &'a self,
        transaction: &mut S::Transaction,
        workflow: WorkflowRecord,
    ) -> Result<Receipt, ExecutionError> {
        let workflow_id = workflow.id;
        let parent_id = workflow.parent_id;
        let module_id = workflow.module_id.clone();
        let (template, mut tracer) = self.restore_workflow(transaction, workflow).await;
        let mut children = NewWorkflows::new(Some(workflow_id), self.shared());
        let mut workflow = template.restore(&mut children, &mut tracer);

        let result = workflow.tick();
        if let Ok(receipt) = &result {
            let span = tracing::debug_span!("persist_workflow_and_messages", workflow_id);
            let _entered = span.enter();
            let messages = workflow.drain_messages();
            let mut persisted = workflow.persist();
            children.commit(transaction, &mut persisted).await;
            let tracing_metadata = tracer.persist_metadata();
            let (spans, local_spans) = tracer.persist();

            let mut persistence = StorageHelper::new(transaction);
            persistence
                .persist_workflow(workflow_id, parent_id, persisted, spans)
                .await;
            persistence.close_channels(workflow_id, receipt).await;
            persistence.push_messages(messages).await;

            self.local_spans
                .lock()
                .await
                .insert(workflow_id, local_spans);
            transaction
                .update_tracing_metadata(&module_id, tracing_metadata)
                .await;
        }
        result
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
    pub async fn tick(&'a self) -> Result<TickResult<Actions<'a, Self>>, WouldBlock> {
        let span = tracing::info_span!("tick", workflow_id = field::Empty, err = field::Empty);
        let _entered = span.enter();
        let mut transaction = self.storage.transaction().await;

        let workflow = transaction.find_pending_workflow().await;
        if let Some(workflow) = workflow {
            let workflow_id = workflow.id;
            span.record("workflow_id", workflow_id);

            let result = self.tick_workflow(&mut transaction, workflow).await;
            transaction.commit().await;
            return Ok(TickResult {
                workflow_id,
                extra: Actions::new(self, result.is_err()),
                result,
            });
        }

        let channel_info = transaction.find_consumable_channel().await;
        if let Some((channel_id, message_idx, workflow)) = channel_info {
            let workflow_id = workflow.id;
            span.record("workflow_id", workflow_id);

            let result = self
                .feed_message_to_workflow(&mut transaction, channel_id, message_idx, workflow)
                .await;
            transaction.commit().await;
            let mut actions = Actions::new(self, result.is_err());
            if result.is_err() {
                actions.allow_dropping_message(channel_id, message_idx);
            }
            return Ok(TickResult {
                workflow_id,
                extra: actions,
                result,
            });
        }

        let err = WouldBlock {
            nearest_timer_expiration: transaction.nearest_timer_expiration().await,
        };
        tracing::info!(%err, "workflow manager blocked");
        Err(err)
    }
}
