//! Tick logic for `Runtime`.

use chrono::{DateTime, Utc};
use tracing_tunnel::TracingEventReceiver;

use std::{error, fmt, sync::Arc};

use super::{stubs::Stubs, CachedDefinitions, Clock, Runtime, RuntimeInner, Services};
use crate::{
    data::PersistedWorkflowData,
    engine::{DefineWorkflow, WorkflowEngine},
    receipt::{ExecutionError, Receipt},
    storage::{
        helper::StorageHelper, ActiveWorkflowState, ErroneousMessageRef, MessageError,
        ReadChannels, ReadModules, ReadWorkflows, Storage, StorageTransaction, WorkflowRecord,
        WorkflowWaker, WorkflowWakerRecord, WriteModules, WriteWorkflowWakers, WriteWorkflows,
    },
    workflow::{PersistedWorkflow, Workflow},
};
use tardigrade::{ChannelId, WorkflowId};

/// Result of [ticking](Runtime::tick()) a [`Runtime`].
#[derive(Debug)]
#[must_use = "Result can contain an execution error which should be handled"]
pub struct TickResult {
    workflow_id: WorkflowId,
    result: Result<Receipt, ExecutionError>,
}

impl TickResult {
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
}

/// An error returned by [`Runtime::tick()`] if the runtime cannot progress.
#[derive(Debug)]
pub struct WouldBlock {
    nearest_timer_expiration: Option<DateTime<Utc>>,
}

impl WouldBlock {
    /// Returns the nearest timer expiration for the workflows within the runtime.
    pub fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>> {
        self.nearest_timer_expiration
    }
}

impl fmt::Display for WouldBlock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "cannot progress workflow runtime")?;
        if let Some(expiration) = self.nearest_timer_expiration {
            write!(formatter, " (nearest timer expiration: {expiration})")?;
        }
        Ok(())
    }
}

impl error::Error for WouldBlock {}

/// Errors that can occur when [ticking a specific workflow](Runtime::tick_workflow()).
#[derive(Debug)]
#[non_exhaustive]
pub enum WorkflowTickError {
    /// Workflow not found.
    NotFound,
    /// Workflow is not active.
    NotActive,
    /// The workflow has no events to consume.
    WouldBlock(WouldBlock),
}

impl From<WouldBlock> for WorkflowTickError {
    fn from(err: WouldBlock) -> Self {
        Self::WouldBlock(err)
    }
}

impl fmt::Display for WorkflowTickError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => formatter.write_str("workflow not found"),
            Self::NotActive => formatter.write_str("workflow is not active"),
            Self::WouldBlock(err) => fmt::Display::fmt(err, formatter),
        }
    }
}

impl error::Error for WorkflowTickError {}

#[derive(Debug)]
struct PendingChannel {
    channel_id: ChannelId,
    message_idx: u64,
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

#[derive(Debug)]
enum WorkflowSeedInner<D: DefineWorkflow> {
    Persisted {
        definition: Arc<D>,
        persisted: PersistedWorkflow,
    },
    Cached(Workflow<D::Instance>),
}

/// Temporary value holding parts necessary to restore a `Workflow`.
struct WorkflowSeed<D: DefineWorkflow> {
    clock: Arc<dyn Clock>,
    inner: WorkflowSeedInner<D>,
}

impl<D> fmt::Debug for WorkflowSeed<D>
where
    D: DefineWorkflow + fmt::Debug,
    D::Instance: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, formatter)
    }
}

impl<D: DefineWorkflow> WorkflowSeed<D> {
    fn extract_services(services: Services) -> (Stubs, TracingEventReceiver) {
        let stubs = services.stubs.unwrap();
        let stubs = stubs.downcast::<Stubs>();
        let receiver = services.tracer.unwrap();
        (stubs, receiver)
    }

    fn persisted_mut(&mut self) -> &mut PersistedWorkflowData {
        match &mut self.inner {
            WorkflowSeedInner::Persisted { persisted, .. } => persisted.data_mut(),
            WorkflowSeedInner::Cached(workflow) => workflow.persisted_mut(),
        }
    }

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
                    self.persisted_mut().set_current_time(timer);
                }
                WorkflowWaker::SenderClosure(channel_id) => {
                    self.persisted_mut().close_sender(channel_id);
                }
                WorkflowWaker::ChildCompletion(child_id) => {
                    let child = transaction.workflow(child_id).await.unwrap();
                    let result = child.state.into_result().unwrap();
                    // ^ Both `unwrap()`s above are safe by construction.
                    self.persisted_mut()
                        .notify_on_child_completion(child_id, result);
                }
            }
        }
    }

    // TODO: consider pushing messages to all channels or making this configurable.
    async fn update_inbound_channels<T: ReadChannels>(
        &mut self,
        transaction: &T,
    ) -> Option<PendingChannel> {
        let channels = self.persisted_mut().receivers();
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
                    self.persisted_mut()
                        .push_message_for_receiver(pending.channel_id, message)
                        .unwrap();
                    // ^ `unwrap()` is safe: no messages can be persisted
                    pending_channel = Some(pending);
                }
                Err(MessageError::NonExistingIndex { is_closed: true }) => {
                    // Signal to the workflow that the channel is closed. This can be performed
                    // on a persisted workflow, without executing it.
                    self.persisted_mut().close_receiver(pending.channel_id);
                }
                _ => {
                    // Skip processing for now: we want to pinpoint consumption-related
                    // errors should they occur.
                }
            }
        }
        pending_channel
    }

    fn restore(self, stubs: Stubs, tracer: TracingEventReceiver) -> Workflow<D::Instance> {
        let services = Services {
            clock: self.clock,
            stubs: Some(Box::new(stubs)),
            tracer: Some(tracer),
        };

        match self.inner {
            WorkflowSeedInner::Persisted {
                persisted,
                definition,
            } => {
                tracing::debug!(?definition, "using definition to restore workflow");
                persisted.restore(definition.as_ref(), services).unwrap()
            }

            WorkflowSeedInner::Cached(mut workflow) => {
                workflow.set_services(services);
                workflow
            }
        }
    }
}

impl<E: WorkflowEngine, C: Clock> RuntimeInner<E, C> {
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
        &self,
        transaction: &impl ReadModules,
        record: WorkflowRecord<ActiveWorkflowState>,
    ) -> (WorkflowSeed<E::Definition>, TracingEventReceiver) {
        let state = record.state;
        let cached_workflow = {
            let mut cache = self.cached_workflows.lock().await;
            cache.take(record.id, record.execution_count)
        };

        let inner = if let Some(workflow) = cached_workflow {
            WorkflowSeedInner::Cached(workflow)
        } else {
            let definition_id =
                CachedDefinitions::full_id(&record.module_id, &record.name_in_module);
            let mut definitions = self.definitions().await;
            let definition = definitions
                .get(&definition_id, transaction)
                .await
                .expect("definition missing for an existing workflow");
            WorkflowSeedInner::Persisted {
                definition,
                persisted: state.persisted,
            }
        };

        let module = transaction.module(&record.module_id).await.unwrap();
        let tracing_metadata = module.tracing_metadata;
        let local_spans = {
            let mut spans_guard = self.local_spans.lock().await;
            spans_guard.remove(&record.id).unwrap_or_default()
        };
        let tracer = TracingEventReceiver::new(tracing_metadata, state.tracing_spans, local_spans);

        let seed = WorkflowSeed {
            clock: Arc::clone(&self.clock) as Arc<dyn Clock>,
            inner,
        };
        (seed, tracer)
    }
}

impl<E: WorkflowEngine, C: Clock, S: Storage> Runtime<E, C, S> {
    #[tracing::instrument(
        skip_all,
        fields(
            workflow.id = workflow.id,
            workflow.module_id = workflow.module_id,
            workflow.name_in_module = workflow.name_in_module
        )
    )]
    async fn tick_workflow_in_transaction<'a>(
        &'a self,
        transaction: &mut S::Transaction<'a>,
        workflow: WorkflowRecord<ActiveWorkflowState>,
        wakers: Vec<WorkflowWaker>,
    ) -> (Result<Receipt, ExecutionError>, Option<PendingChannel>) {
        let workflow_id = workflow.id;
        let parent_id = workflow.parent_id;
        let module_id = workflow.module_id.clone();
        let execution_count = workflow.execution_count;

        let (mut seed, tracer) = self.inner.restore_workflow(transaction, workflow).await;
        seed.apply_wakers(transaction, wakers).await;
        let pending_channel = seed.update_inbound_channels(transaction).await;
        let stubs = Stubs::new(Some(workflow_id));
        let mut workflow = seed.restore(stubs, tracer);

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
            let definitions = self.inner.definitions().await;
            stubs
                .commit(definitions, transaction, &mut workflow, receipt)
                .await;
            let tracing_metadata = tracer.persist_metadata();
            let (spans, new_local_spans) = tracer.persist();

            let persisted = workflow.persist();
            let mut persistence = StorageHelper::new(transaction);
            persistence.push_messages(messages).await;
            persistence
                .persist_workflow(workflow_id, parent_id, persisted, spans)
                .await;
            persistence.close_channels(workflow_id, receipt).await;

            {
                let mut local_spans = self.inner.local_spans.lock().await;
                local_spans.insert(workflow_id, new_local_spans);
            }
            transaction
                .update_tracing_metadata(&module_id, tracing_metadata)
                .await;
            let mut cached_workflows = self.inner.cached_workflows.lock().await;
            cached_workflows.insert(workflow_id, workflow, execution_count + 1);
        } else {
            let err = result.as_ref().unwrap_err();
            tracing::warn!(%err, "workflow execution failed");
        }

        (result, pending_channel)
    }

    /// Attempts to advance a single workflow within this runtime.
    ///
    /// A workflow can be advanced if it has outstanding wakers, e.g., ones produced by
    /// [expiring timers](Self::set_current_time()) or by flushing outbound messages
    /// during previous workflow executions. Alternatively, there is an inbound message
    /// that can be consumed by the workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime cannot be progressed.
    pub async fn tick(&self) -> Result<TickResult, WouldBlock> {
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
            tracing::info!(%err, "workflow runtime blocked");
            return Err(err);
        };
        let workflow_id = workflow.id;
        let result = self.do_tick(transaction, workflow, waker_records).await;

        Ok(TickResult {
            workflow_id,
            result,
        })
    }

    async fn do_tick<'a>(
        &'a self,
        mut transaction: S::Transaction<'a>,
        workflow: WorkflowRecord<ActiveWorkflowState>,
        waker_records: Vec<WorkflowWakerRecord>,
    ) -> Result<Receipt, ExecutionError> {
        let workflow_id = workflow.id;
        let (waker_ids, wakers): (Vec<_>, Vec<_>) = waker_records
            .into_iter()
            .map(|record| (record.waker_id, record.waker))
            .unzip();

        let (result, pending_channel) = self
            .tick_workflow_in_transaction(&mut transaction, workflow, wakers)
            .await;
        let result = match result {
            Ok(receipt) => {
                transaction.delete_wakers(workflow_id, &waker_ids).await;
                Ok(receipt)
            }
            Err(err) => {
                let erroneous_messages: Vec<_> = pending_channel
                    .into_iter()
                    .map(PendingChannel::into_message_ref)
                    .collect();
                StorageHelper::new(&mut transaction)
                    .persist_workflow_error(workflow_id, err.clone(), erroneous_messages.clone())
                    .await;
                Err(err)
            }
        };
        transaction.commit().await;

        result
    }

    /// Attempts to advance a specific workflow within this runtime.
    ///
    /// # Errors
    ///
    /// Returns an error in any of the following cases:
    ///
    /// - The workflow with the specified ID does not exist.
    /// - The workflow is not currently active.
    /// - The workflow cannot be advanced (see [`WouldBlock`])
    pub async fn tick_workflow(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<TickResult, WorkflowTickError> {
        let span = tracing::info_span!("tick_workflow", workflow_id);
        let _entered = span.enter();
        let mut transaction = self.storage.transaction().await;

        let workflow = transaction
            .workflow_for_update(workflow_id)
            .await
            .ok_or(WorkflowTickError::NotFound)?;
        let workflow = workflow.into_active().ok_or(WorkflowTickError::NotActive)?;
        let nearest_timer_expiration = workflow.state.persisted.common().nearest_timer();
        let waker_records = transaction.wakers_for_workflow(workflow_id).await;

        let result = self.do_tick(transaction, workflow, waker_records).await;
        if let Ok(receipt) = &result {
            if receipt.executions().is_empty() {
                let err = WouldBlock {
                    nearest_timer_expiration,
                };
                return Err(err.into());
            }
        }

        Ok(TickResult {
            workflow_id,
            result,
        })
    }
}
