//! Tick logic for `WorkflowManager`.

use chrono::{DateTime, Utc};
use tracing_tunnel::TracingEventReceiver;

use std::{error, fmt, sync::Arc};

use super::{
    persistence::StorageHelper, stubs::Stubs, AsManager, Clock, ConcurrencyError,
    ErroredWorkflowHandle, Services, WorkflowManager,
};
use crate::{
    engine::{DefineWorkflow, WorkflowEngine},
    receipt::{ExecutionError, Receipt},
    storage::{
        ActiveWorkflowState, ErroneousMessageRef, MessageError, ReadChannels, ReadModules,
        ReadWorkflows, Storage, StorageTransaction, WorkflowRecord, WorkflowWaker, WriteModules,
        WriteWorkflowWakers, WriteWorkflows,
    },
    workflow::Workflow,
    PersistedWorkflow,
};
use tardigrade::{handle::Handle, ChannelId, WorkflowId};

/// Result of [ticking](WorkflowManager::tick()) a [`WorkflowManager`].
#[derive(Debug)]
#[must_use = "Result can contain an execution error which should be handled"]
pub struct TickResult<E = ExecutionError> {
    workflow_id: WorkflowId,
    result: Result<Receipt, E>,
}

impl<E> TickResult<E> {
    /// Returns the ID of the executed workflow.
    pub fn workflow_id(&self) -> WorkflowId {
        self.workflow_id
    }

    /// Returns a reference to the underlying execution result.
    #[allow(clippy::missing_errors_doc)] // doesn't make sense semantically
    pub fn as_ref(&self) -> Result<&Receipt, &E> {
        self.result.as_ref()
    }

    /// Returns the underlying execution result.
    #[allow(clippy::missing_errors_doc)] // doesn't make sense semantically
    pub fn into_inner(self) -> Result<Receipt, E> {
        self.result
    }
}

impl<M: AsManager> TickResult<ErroredWorkflowHandle<'_, M>> {
    /// Replaces the associated errored workflow handle with the corresponding [`ExecutionError`].
    pub fn drop_handle(self) -> TickResult {
        TickResult {
            workflow_id: self.workflow_id,
            result: self.result.map_err(|handle| handle.error),
        }
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
    channel_id: ChannelId,
    message_idx: usize,
    waits_for_message: bool,
}

impl PendingChannel {
    fn take_pending_message<E: WorkflowEngine>(&self, workflow: &mut Workflow<E::Instance>) {
        if workflow.take_pending_inbound_message(self.channel_id) {
            // The message was not consumed. We still persist the workflow in order to
            // consume wakers (otherwise, we would loop indefinitely), and place the message
            // back to the channel.
            tracing::warn!(
                ?self,
                "message was not consumed by workflow; placing the message back"
            );
        }
    }

    fn into_message_ref(self) -> ErroneousMessageRef {
        ErroneousMessageRef {
            channel_id: self.channel_id,
            index: self.message_idx,
        }
    }
}

/// Temporary value holding parts necessary to restore a `Workflow`.
#[derive(Debug)]
struct WorkflowSeed<'a, D> {
    clock: Arc<dyn Clock>,
    definition: &'a D,
    persisted: PersistedWorkflow,
}

impl<'a, D: DefineWorkflow> WorkflowSeed<'a, D> {
    async fn apply_wakers<T: ReadWorkflows>(
        &mut self,
        transaction: &T,
        wakers: Vec<WorkflowWaker>,
    ) {
        for waker in wakers {
            match waker {
                WorkflowWaker::Internal => {
                    // Handled separately; no modifications are required
                }
                WorkflowWaker::Timer(timer) => {
                    self.persisted.set_current_time(timer);
                }
                WorkflowWaker::SenderClosure(channel_id) => {
                    self.persisted.close_channel(Handle::Sender(channel_id));
                }
                WorkflowWaker::ChildCompletion(child_id) => {
                    let child = transaction.workflow(child_id).await.unwrap();
                    let result = child.state.into_result().unwrap();
                    // ^ Both `unwrap()`s above are safe by construction.
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
        let channels = self.persisted.receivers();
        let pending_channels = channels.filter_map(|(id, state)| {
            if state.is_closed() {
                None
            } else {
                Some(PendingChannel {
                    channel_id: id,
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
                        .push_message_for_receiver(pending.channel_id, message)
                        .unwrap();
                    // ^ `unwrap()` is safe: no messages can be persisted
                    pending_channel = Some(pending);
                }
                Err(MessageError::NonExistingIndex { is_closed: true }) => {
                    // Signal to the workflow that the channel is closed. This can be performed
                    // on a persisted workflow, without executing it.
                    self.persisted
                        .close_channel(Handle::Receiver(pending.channel_id));
                }
                _ => {
                    // Skip processing for now: we want to pinpoint consumption-related
                    // errors should they occur.
                }
            }
        }
        pending_channel
    }

    fn restore(self, stubs: Stubs<D>, tracer: TracingEventReceiver) -> Workflow<D::Instance> {
        let services = Services {
            clock: self.clock,
            stubs: Some(Box::new(stubs)),
            tracer: Some(tracer),
        };
        self.persisted.restore(self.definition, services).unwrap()
    }

    fn extract_services(services: Services) -> (Stubs<D>, TracingEventReceiver) {
        let stubs = services.stubs.unwrap();
        let stubs = *stubs.downcast::<Stubs<D>>();
        let receiver = services.tracer.unwrap();
        (stubs, receiver)
    }
}

impl<E: WorkflowEngine, C: Clock, S: Storage> WorkflowManager<E, C, S> {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            id = record.id,
            module_id = record.module_id,
            name_in_module = record.name_in_module
        )
    )]
    async fn restore_workflow<'a>(
        &'a self,
        transaction: &S::Transaction<'a>,
        record: WorkflowRecord<ActiveWorkflowState>,
    ) -> (WorkflowSeed<E::Definition>, TracingEventReceiver) {
        let state = record.state;
        let definition = self
            .definitions
            .get(&record.module_id, &record.name_in_module);
        tracing::debug!(?definition, "using definition to restore workflow");

        let tracing_metadata = transaction
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
        let tracer = TracingEventReceiver::new(tracing_metadata, state.tracing_spans, local_spans);
        let template = WorkflowSeed {
            clock: Arc::clone(&self.clock) as Arc<dyn Clock>,
            definition,
            persisted: state.persisted,
        };
        (template, tracer)
    }

    #[tracing::instrument(
        skip_all,
        fields(
            workflow.id = workflow.id,
            workflow.module_id = workflow.module_id,
            workflow.name_in_module = workflow.name_in_module
        )
    )]
    async fn tick_workflow<'a>(
        &'a self,
        transaction: &mut S::Transaction<'a>,
        workflow: WorkflowRecord<ActiveWorkflowState>,
        wakers: Vec<WorkflowWaker>,
    ) -> (Result<Receipt, ExecutionError>, Option<PendingChannel>) {
        let workflow_id = workflow.id;
        let parent_id = workflow.parent_id;
        let module_id = workflow.module_id.clone();
        let (mut template, tracer) = self.restore_workflow(transaction, workflow).await;
        template.apply_wakers(transaction, wakers).await;
        let pending_channel = template.update_inbound_channels(transaction).await;
        let stubs = Stubs::new(Some(workflow_id), self.shared());
        let mut workflow = template.restore(stubs, tracer);

        let mut result = workflow.tick();
        if let Ok(receipt) = &mut result {
            if let Some(pending) = &pending_channel {
                pending.take_pending_message::<E>(&mut workflow);
            }

            let span = tracing::debug_span!("persist_workflow_and_messages", workflow_id);
            let _entered = span.enter();

            let (stubs, tracer) =
                WorkflowSeed::<E::Definition>::extract_services(workflow.take_services());
            let messages = workflow.drain_messages();
            stubs.commit(transaction, &mut workflow, receipt).await;
            let tracing_metadata = tracer.persist_metadata();
            let (spans, local_spans) = tracer.persist();

            let persisted = workflow.persist();
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
    pub async fn tick(&self) -> Result<TickResult<ErroredWorkflowHandle<'_, Self>>, WouldBlock> {
        let span = tracing::info_span!("tick");
        let _entered = span.enter();
        let mut transaction = self.storage.transaction().await;

        let mut workflow = transaction.workflow_with_wakers_for_update().await;
        let waker_records = if let Some(workflow) = &workflow {
            transaction.wakers_for_workflow(workflow.id).await
        } else {
            workflow = transaction
                .workflow_with_consumable_channel_for_update()
                .await;
            vec![]
        };

        let Some(workflow) = workflow else {
            let err = WouldBlock {
                nearest_timer_expiration: transaction.nearest_timer_expiration().await,
            };
            tracing::info!(%err, "workflow manager blocked");
            return Err(err);
        };
        let workflow_id = workflow.id;

        let (waker_ids, wakers): (Vec<_>, Vec<_>) = waker_records
            .into_iter()
            .map(|record| (record.waker_id, record.waker))
            .unzip();

        let (result, pending_channel) =
            self.tick_workflow(&mut transaction, workflow, wakers).await;
        let result = match result {
            Ok(receipt) => {
                transaction.delete_wakers(workflow_id, &waker_ids).await;
                Ok(receipt)
            }
            Err(error) => {
                let erroneous_messages: Vec<_> = pending_channel
                    .into_iter()
                    .map(PendingChannel::into_message_ref)
                    .collect();
                StorageHelper::new(&mut transaction)
                    .persist_workflow_error(workflow_id, error.clone(), erroneous_messages.clone())
                    .await;
                Err(ErroredWorkflowHandle::new(
                    self,
                    workflow_id,
                    error,
                    erroneous_messages,
                ))
            }
        };
        transaction.commit().await;

        Ok(TickResult {
            workflow_id,
            result,
        })
    }

    pub(super) async fn drop_message(
        &self,
        workflow_id: WorkflowId,
        message_ref: &ErroneousMessageRef,
    ) -> Result<(), ConcurrencyError> {
        let mut transaction = self.storage.transaction().await;
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
}
