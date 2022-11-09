//! [`WorkflowManager`] and tightly related types.
//!
//! See `WorkflowManager` docs for an overview and examples of usage.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{lock::Mutex, StreamExt};
use tracing::field;
use tracing_tunnel::{LocalSpans, PersistedMetadata, TracingEventReceiver};

use std::{borrow::Cow, collections::HashMap, error, fmt, sync::Arc};

pub mod future; // FIXME: rename `driver`?
mod handle;
mod new_workflows;
mod persistence;

#[cfg(test)]
mod tests;

pub use self::handle::{MessageReceiver, MessageSender, TakenMessages, WorkflowHandle};

use self::{new_workflows::NewWorkflows, persistence::PersistenceManager};
use crate::{
    module::{Clock, Schedule, Services, StashWorkflows},
    receipt::{ChannelEvent, ChannelEventKind, ExecutionError, Receipt},
    storage::{
        ChannelRecord, MessageOrEof, ModuleRecord, ReadChannels, ReadModules, ReadWorkflows,
        Storage, StorageTransaction, WorkflowRecord, WriteChannels, WriteModules, WriteWorkflows,
    },
    workflow::{ChannelIds, Workflow},
    PersistedWorkflow, WorkflowEngine, WorkflowModule, WorkflowSpawner,
};
use tardigrade::{
    channel::SendError,
    interface::{ChannelKind, Interface},
    spawn::{ChannelsConfig, ManageInterfaces, ManageWorkflows, SpecifyWorkflowChannels},
    workflow::WorkflowFn,
    ChannelId, WorkflowId,
};

#[derive(Debug)]
struct WorkflowAndChannelIds {
    workflow_id: WorkflowId,
    channel_ids: ChannelIds,
}

#[derive(Debug)]
struct DropMessageAction {
    channel_id: ChannelId,
    // TODO: add message index to protect against edit conflicts
}

impl DropMessageAction {
    async fn execute<'a, S: Storage<'a>>(self, manager: &'a WorkflowManager<S>) {
        let mut transaction = manager.storage.transaction().await;
        if let Some((_, token)) = transaction.pop_message(self.channel_id).await {
            transaction.confirm_message_removal(token).await.ok();
        }
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
pub struct Actions<'a, S> {
    manager: &'a WorkflowManager<S>,
    abort_action: bool,
    drop_message_action: Option<DropMessageAction>,
}

impl<'a, S> Actions<'a, S> {
    fn new(manager: &'a WorkflowManager<S>, abort_action: bool) -> Self {
        Self {
            manager,
            abort_action,
            drop_message_action: None,
        }
    }

    fn allow_dropping_message(&mut self, channel_id: ChannelId) {
        self.drop_message_action = Some(DropMessageAction { channel_id });
    }
}

impl<'a, S: Storage<'a>> TickResult<Actions<'a, S>> {
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
        self.extra.manager.abort_workflow(self.workflow_id).await;
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
        action.execute(self.extra.manager).await;
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
    clock: &'a ClockOrScheduler,
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

#[derive(Debug, Default)]
struct WorkflowSpawners {
    inner: HashMap<String, HashMap<String, WorkflowSpawner<()>>>,
}

impl WorkflowSpawners {
    fn get(&self, module_id: &str, name_in_module: &str) -> &WorkflowSpawner<()> {
        &self.inner[module_id][name_in_module]
    }

    fn split_full_id(full_id: &str) -> Option<(&str, &str)> {
        full_id.split_once("::")
    }

    fn for_full_id(&self, full_id: &str) -> Option<&WorkflowSpawner<()>> {
        let (module_id, name_in_module) = Self::split_full_id(full_id)?;
        self.inner.get(module_id)?.get(name_in_module)
    }

    fn insert(&mut self, module_id: String, name_in_module: String, spawner: WorkflowSpawner<()>) {
        let module_entry = self.inner.entry(module_id).or_default();
        module_entry.insert(name_in_module, spawner);
    }
}

/// Part of the manager used during workflow instantiation.
#[derive(Debug, Clone, Copy)]
struct Shared<'a> {
    clock: &'a ClockOrScheduler,
    spawners: &'a WorkflowSpawners,
}

#[derive(Debug, Clone, Copy)]
enum ChannelSide {
    HostSender,
    WorkflowSender(WorkflowId),
    Receiver,
}

#[derive(Debug)]
enum ClockOrScheduler {
    Clock(Arc<dyn Clock>),
    Scheduler(Arc<dyn Schedule>),
}

impl Default for ClockOrScheduler {
    fn default() -> Self {
        Self::Clock(Arc::new(Utc::now))
    }
}

impl Clock for ClockOrScheduler {
    fn now(&self) -> DateTime<Utc> {
        match self {
            Self::Clock(clock) => clock.now(),
            Self::Scheduler(scheduler) => scheduler.now(),
        }
    }
}

impl ClockOrScheduler {
    fn as_scheduler(&self) -> Option<&dyn Schedule> {
        match self {
            Self::Scheduler(scheduler) => Some(scheduler.as_ref()),
            Self::Clock(_) => None,
        }
    }
}

/// Simple in-memory implementation of a workflow manager.
///
/// A workflow manager is responsible for managing state and interfacing with workflows
/// and channels connected to the workflows. In particular, a manager supports the following
/// operations:
///
/// - Spawning new workflows (including from the workflow code)
/// - Writing messages to channels and reading messages from channels
/// - Driving the contained workflows to completion (either [directly](Self::tick()) or
///   using [future-based API][`AsyncEnv`])
///
/// This is the simplest manager implementation that stores the entire state in RAM.
/// It is not a good choice for high-load, but is sufficiently flexible to support
/// most basic use cases (e.g., persisting / resuming workflows).
///
/// [`AsyncEnv`]: crate::manager::future::AsyncEnv
///
/// # Examples
///
/// ```
/// # use tardigrade_rt::{manager::{WorkflowHandle, WorkflowManager}, WorkflowModule};
/// # fn test_wrapper(module: WorkflowModule) -> anyhow::Result<()> {
/// // A manager is instantiated using the builder pattern:
/// let module: WorkflowModule = // ...
/// #   module;
/// let spawner = module.for_untyped_workflow("TestWorkflow").unwrap();
/// let mut manager = WorkflowManager::builder()
///     .with_spawner("test", spawner)
///     .build();
///
/// // After that, new workflows can be spawned using `ManageWorkflowsExt`
/// // trait from the `tardigrade` crate:
/// use tardigrade::spawn::ManageWorkflowsExt;
/// let args = b"test_args".to_vec();
/// let workflow =
///     manager.new_workflow::<()>("test", args)?.build()?;
/// // Do something with `workflow`, e.g., write something to its channels...
///
/// // Initialize the workflow:
/// let receipt = manager.tick()?.into_inner()?;
/// println!("{:?}", receipt);
/// # Ok(())
/// # }
/// ```
pub struct WorkflowManager<S> {
    clock: ClockOrScheduler,
    spawners: WorkflowSpawners,
    storage: S,
    local_spans: Mutex<HashMap<WorkflowId, LocalSpans>>,
}

impl<S> fmt::Debug for WorkflowManager<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowManager")
            .field("clock", &self.clock)
            .field("spawners", &self.spawners)
            .field("local_spans", &self.local_spans)
            .finish()
    }
}

impl<S: for<'a> Storage<'a>> WorkflowManager<S> {
    /// Creates a builder that will use the specified storage.
    pub fn builder(storage: S) -> WorkflowManagerBuilder<'static, S> {
        WorkflowManagerBuilder {
            module_creator: Box::new(WorkflowEngine::default()),
            clock: ClockOrScheduler::default(),
            storage,
        }
    }

    async fn new(
        clock: ClockOrScheduler,
        storage: S,
        module_creator: &dyn CreateModule,
    ) -> anyhow::Result<Self> {
        let spawners = {
            let transaction = storage.readonly_transaction().await;
            let mut spawners = WorkflowSpawners::default();
            let mut module_records = transaction.modules();
            while let Some(record) = module_records.next().await {
                let module = module_creator.create_module(&record).await?;
                for (name, spawner) in module.into_spawners() {
                    spawners.insert(record.id.clone(), name, spawner);
                }
            }
            spawners
        };

        Ok(Self {
            clock,
            spawners,
            storage,
            local_spans: Mutex::default(),
        })
    }

    /// Returns the encapsulated storage.
    pub fn into_storage(self) -> S {
        self.storage
    }
}

impl<'a, S: Storage<'a>> WorkflowManager<S> {
    fn shared(&'a self) -> Shared<'a> {
        Shared {
            clock: &self.clock,
            spawners: &self.spawners,
        }
    }

    /// Inserts the specified module into the manager.
    pub async fn insert_module(&'a mut self, id: &str, module: WorkflowModule<'_>) {
        let mut transaction = self.storage.transaction().await;
        let module_record = ModuleRecord {
            id: id.to_owned(),
            bytes: Cow::Borrowed(module.bytes),
            tracing_metadata: PersistedMetadata::default(),
        };
        transaction.insert_module(module_record).await;

        for (name, spawner) in module.into_spawners() {
            self.spawners.insert(id.to_owned(), name, spawner);
        }
        transaction.commit().await;
    }

    /// Returns current information about the channel with the specified ID, or `None` if a channel
    /// with this ID does not exist.
    pub async fn channel(&'a self, channel_id: ChannelId) -> Option<ChannelRecord> {
        let transaction = self.storage.readonly_transaction().await;
        let record = transaction.channel(channel_id).await?;
        Some(record)
    }

    /// Returns a handle to a workflow with the specified ID.
    pub async fn workflow(&'a self, workflow_id: WorkflowId) -> Option<WorkflowHandle<'a, (), S>> {
        let transaction = self.storage.readonly_transaction().await;
        let record = transaction.workflow(workflow_id).await?;
        let interface = self
            .spawners
            .get(&record.module_id, &record.name_in_module)
            .interface();
        let handle = WorkflowHandle::new(self, &transaction, interface, record).await;
        Some(handle)
    }

    /// Returns the number of non-completed workflows.
    pub async fn workflow_count(&'a self) -> usize {
        let transaction = self.storage.readonly_transaction().await;
        transaction.count_workflows().await
    }

    pub(crate) async fn send_message(
        &'a self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), SendError> {
        let mut transaction = self.storage.transaction().await;
        let result = transaction.push_messages(channel_id, vec![message]).await;
        transaction.commit().await;
        result
    }

    pub(crate) async fn close_host_sender(&'a self, channel_id: ChannelId) {
        let mut transaction = self.storage.transaction().await;
        PersistenceManager::new(&mut transaction)
            .close_channel_side(channel_id, ChannelSide::HostSender)
            .await;
        transaction.commit().await;
    }

    pub(crate) async fn close_host_receiver(&'a self, channel_id: ChannelId) {
        let mut transaction = self.storage.transaction().await;
        if cfg!(debug_assertions) {
            let channel = transaction.channel(channel_id).await.unwrap();
            debug_assert!(
                channel.state.receiver_workflow_id.is_none(),
                "Attempted to close channel {} for which the host doesn't hold receiver",
                channel_id
            );
        }
        PersistenceManager::new(&mut transaction)
            .close_channel_side(channel_id, ChannelSide::Receiver)
            .await;
        transaction.commit().await;
    }

    // TODO: remove in favor of more granular message handling
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn take_outbound_messages(&'a self, channel_id: ChannelId) -> Vec<Vec<u8>> {
        let mut transaction = self.storage.transaction().await;
        let channel_state = transaction.channel(channel_id).await.unwrap().state;
        assert!(
            channel_state.receiver_workflow_id.is_none(),
            "cannot receive a message for a channel with the receiver connected to a workflow"
        );

        let mut messages = vec![];
        while let Some((message, token)) = transaction.pop_message(channel_id).await {
            if let MessageOrEof::Message(payload) = message {
                messages.push(payload);
            }
            transaction.confirm_message_removal(token).await.ok();
        }
        transaction.commit().await;
        messages
    }

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
    async fn feed_message_to_workflow(
        &'a self,
        transaction: &mut S::Transaction,
        channel_id: ChannelId,
        workflow: WorkflowRecord,
    ) -> Result<Receipt, ExecutionError> {
        let workflow_id = workflow.id;
        let (message_or_eof, message_token) = transaction
            .pop_message(channel_id)
            .await
            .expect("no message to feed");
        let (child_id, channel_name) = workflow.persisted.find_inbound_channel(channel_id);
        let message = match message_or_eof {
            MessageOrEof::Message(message) => message,
            MessageOrEof::Eof => {
                // Signal to the workflow that the channel is closed. This can be performed
                // on a persisted workflow, without executing it.
                let mut persisted = workflow.persisted;
                persisted.close_inbound_channel(child_id, &channel_name);
                transaction
                    .persist_workflow(workflow_id, persisted, workflow.tracing_spans)
                    .await;
                transaction
                    .confirm_message_removal(message_token)
                    .await
                    .ok();
                return Ok(Receipt::default());
            }
        };

        let parent_id = workflow.parent_id;
        let module_id = workflow.module_id.clone();
        let (seed, mut tracer) = self.restore_workflow(transaction, workflow).await;
        let mut new_workflows = NewWorkflows::new(Some(workflow_id), self.shared());
        let mut workflow = seed.restore(&mut new_workflows, &mut tracer);
        let result = Self::push_message(&mut workflow, child_id, &channel_name, message);

        if let Ok(receipt) = &result {
            if workflow.take_pending_inbound_message(child_id, &channel_name) {
                // The message was not consumed. We still persist the workflow in order to
                // consume wakers (otherwise, we would loop indefinitely), and place the message
                // back to the channel.
                tracing::warn!("message was not consumed by workflow; placing the message back");
                transaction.revert_message_removal(message_token).await.ok();
            } else {
                transaction
                    .confirm_message_removal(message_token)
                    .await
                    .ok();
            }

            {
                let span = tracing::debug_span!("persist_workflow_and_messages", workflow_id);
                let _entered = span.enter();
                let messages = workflow.drain_messages();
                let mut persisted = workflow.persist();
                new_workflows.commit(transaction, &mut persisted).await;
                let tracing_metadata = tracer.persist_metadata();
                let (spans, local_spans) = tracer.persist();

                let mut persistence = PersistenceManager::new(transaction);
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
        } else {
            // Do not commit the execution result. Instead, put the message back to the channel.
            transaction.revert_message_removal(message_token).await.ok();
        }
        result
    }

    /// Returns `Ok(None)` if the message cannot be consumed right now (the workflow channel
    /// does not listen to it).
    #[tracing::instrument(
        level = "debug",
        skip(workflow, message),
        fields(message.len = message.len())
    )]
    fn push_message(
        workflow: &mut Workflow,
        child_id: Option<WorkflowId>,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<Receipt, ExecutionError> {
        workflow
            .push_inbound_message(child_id, channel_name, message)
            .unwrap();
        workflow.tick()
    }

    /// Sets the current time for this manager. This may expire timers in some of the contained
    /// workflows.
    #[tracing::instrument(skip(self))]
    pub async fn set_current_time(&'a self, time: DateTime<Utc>) {
        let mut transaction = self.storage.transaction().await;
        PersistenceManager::new(&mut transaction)
            .set_current_time(time)
            .await;
        transaction.commit().await;
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

            let mut persistence = PersistenceManager::new(transaction);
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

    /// Aborts the workflow with the specified ID. The parent workflow, if any, will be notified,
    /// and all channel handles owned by the workflow will be properly disposed.
    ///
    /// # Panics
    ///
    /// Panics if the manager does not contain a workflow with the specified ID.
    #[tracing::instrument(skip(self))]
    pub async fn abort_workflow(&'a self, workflow_id: WorkflowId) {
        let mut transaction = self.storage.transaction().await;
        let record = transaction
            .manipulate_workflow(workflow_id, |persisted| {
                persisted.abort();
            })
            .await;
        if let Some(record) = record {
            PersistenceManager::new(&mut transaction)
                .handle_workflow_update(workflow_id, record.parent_id, &record.persisted)
                .await;
        }
        transaction.commit().await;
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
    pub async fn tick(&'a self) -> Result<TickResult<Actions<'a, S>>, WouldBlock> {
        let span = tracing::info_span!("tick", workflow_id = field::Empty, err = field::Empty);
        let _entered = span.enter();
        let mut transaction = self.storage.transaction().await;

        let workflow = transaction.find_workflow_with_pending_tasks().await;
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
        if let Some((channel_id, workflow)) = channel_info {
            let workflow_id = workflow.id;
            span.record("workflow_id", workflow_id);

            let result = self
                .feed_message_to_workflow(&mut transaction, channel_id, workflow)
                .await;
            transaction.commit().await;
            let mut actions = Actions::new(self, result.is_err());
            if result.is_err() {
                actions.allow_dropping_message(channel_id);
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

/// Customizable [`WorkflowModule`] instantiation logic.
#[async_trait]
pub trait CreateModule {
    /// Restores module from a [`ModuleRecord`].
    ///
    /// # Errors
    ///
    /// Returns an error if instantiation fails for whatever reason. This error will bubble up
    /// in [`WorkflowManagerBuilder::build()`].
    async fn create_module<'a>(
        &self,
        module: &'a ModuleRecord<'_>,
    ) -> anyhow::Result<WorkflowModule<'a>>;
}

impl fmt::Debug for dyn CreateModule + '_ {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateModule")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CreateModule for WorkflowEngine {
    async fn create_module<'a>(
        &self,
        module: &'a ModuleRecord<'_>,
    ) -> anyhow::Result<WorkflowModule<'a>> {
        WorkflowModule::new(self, module.bytes.as_ref())
    }
}

/// Builder for a [`WorkflowManager`].
#[derive(Debug)]
pub struct WorkflowManagerBuilder<'r, S> {
    clock: ClockOrScheduler,
    module_creator: Box<dyn CreateModule + 'r>,
    storage: S,
}

impl<'r, S: for<'a> Storage<'a>> WorkflowManagerBuilder<'r, S> {
    /// Sets the wall clock to be used in the manager.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<impl Clock>) -> Self {
        self.clock = ClockOrScheduler::Clock(clock);
        self
    }

    /// Sets the scheduler to be used in the manager.
    #[must_use]
    pub fn with_scheduler(mut self, scheduler: Arc<impl Schedule>) -> Self {
        self.clock = ClockOrScheduler::Scheduler(scheduler);
        self
    }

    /// Specifies the [module creator](CreateModule) to use.
    #[must_use]
    pub fn with_module_creator(mut self, creator: impl CreateModule + 'r) -> Self {
        self.module_creator = Box::new(creator);
        self
    }

    /// Finishes building the manager.
    ///
    /// # Errors
    ///
    /// Returns an error if [module instantiation](CreateModule) fails.
    pub async fn build(self) -> anyhow::Result<WorkflowManager<S>> {
        WorkflowManager::new(self.clock, self.storage, self.module_creator.as_ref()).await
    }
}

impl<S> ManageInterfaces for WorkflowManager<S> {
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        Some(Cow::Borrowed(
            self.spawners.for_full_id(definition_id)?.interface(),
        ))
    }
}

impl<S> SpecifyWorkflowChannels for WorkflowManager<S> {
    type Inbound = ChannelId;
    type Outbound = ChannelId;
}

#[async_trait]
impl<'a, W: WorkflowFn, S: Storage<'a>> ManageWorkflows<'a, W> for WorkflowManager<S> {
    type Handle = WorkflowHandle<'a, W, S>;
    type Error = anyhow::Error;

    async fn create_workflow(
        &'a self,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<ChannelId>,
    ) -> Result<Self::Handle, Self::Error> {
        let mut new_workflows = NewWorkflows::new(None, self.shared());
        new_workflows.stash_workflow(0, definition_id, args, channels);
        let mut transaction = self.storage.transaction().await;
        let workflow_id = new_workflows.commit_external(&mut transaction).await?;
        transaction.commit().await;
        Ok(self
            .workflow(workflow_id)
            .await
            .unwrap()
            .downcast_unchecked())
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
    fn find_inbound_channel(&self, channel_id: ChannelId) -> (Option<WorkflowId>, String) {
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
