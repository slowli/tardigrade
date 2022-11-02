//! [`WorkflowManager`] and tightly related types.
//!
//! See `WorkflowManager` docs for an overview and examples of usage.

#![allow(clippy::missing_panics_doc)] // lots of `unwrap()`s on mutex locks

use anyhow::Context;
use chrono::{DateTime, Utc};
use tracing::field;
use tracing_tunnel::{LocalSpans, PersistedMetadata, PersistedSpans, TracingEventReceiver};

use std::{
    borrow::Cow,
    collections::HashMap,
    error, fmt, mem,
    sync::{Arc, Mutex, MutexGuard},
};

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub mod future;
mod handle;
mod persistence;
#[cfg(test)]
mod tests;
mod transaction;

pub use self::{
    handle::{MessageReceiver, MessageSender, TakenMessages, WorkflowHandle},
    persistence::PersistedWorkflows,
};

use self::transaction::Transaction;
use crate::{
    module::{Clock, Services, WorkflowAndChannelIds},
    receipt::{ChannelEvent, ChannelEventKind, ExecutionError, Receipt},
    utils::Message,
    workflow::Workflow,
    PersistedWorkflow, WorkflowSpawner,
};
use tardigrade::{
    channel::SendError,
    interface::{ChannelKind, Interface},
    spawn::{ChannelsConfig, ManageInterfaces, ManageWorkflows, SpecifyWorkflowChannels},
    workflow::WorkflowFn,
    ChannelId, WorkflowId,
};

/// Information about a channel managed by a [`WorkflowManager`].
#[derive(Debug)]
pub struct ChannelInfo {
    receiver_workflow_id: Option<WorkflowId>,
    is_closed: bool,
    message_count: usize,
    next_message_idx: usize,
}

impl ChannelInfo {
    /// Checks if the channel is closed (i.e., no new messages can be written into it).
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /// Returns the number of messages written to the channel.
    pub fn received_messages(&self) -> usize {
        self.next_message_idx
    }

    /// Returns the number of messages consumed by the channel receiver.
    pub fn flushed_messages(&self) -> usize {
        self.next_message_idx - self.message_count
    }

    /// Returns the ID of a workflow that holds the receiver end of this channel, or `None`
    /// if it is held by the host.
    pub fn receiver_workflow_id(&self) -> Option<WorkflowId> {
        self.receiver_workflow_id
    }
}

#[derive(Debug)]
struct DropMessageAction {
    channel_id: ChannelId,
}

impl DropMessageAction {
    fn execute(self, manager: &mut WorkflowManager) {
        let state = manager.state.get_mut().unwrap();
        state.take_message(self.channel_id);
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
pub struct Actions<'a> {
    manager: &'a mut WorkflowManager,
    abort_action: bool,
    drop_message_action: Option<DropMessageAction>,
}

impl<'a> Actions<'a> {
    fn new(manager: &'a mut WorkflowManager, abort_action: bool) -> Self {
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

impl TickResult<Actions<'_>> {
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
    pub fn abort_workflow(self) -> TickResult<()> {
        assert!(
            self.extra.abort_action,
            "cannot abort workflow because it did not error"
        );
        self.extra.manager.abort_workflow(self.workflow_id);
        self.drop_extra()
    }

    /// Drops a message that led to erroneous workflow execution.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::can_drop_erroneous_message()`] returns `false`.
    pub fn drop_erroneous_message(mut self) -> TickResult<()> {
        let action = self
            .extra
            .drop_message_action
            .take()
            .expect("cannot drop message");
        action.execute(self.extra.manager);
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
    tracing_metadata: PersistedMetadata,
}

impl<'a> WorkflowSeed<'a> {
    fn restore(
        self,
        transaction: &'a Transaction,
        spans: &'a mut PersistedSpans,
        local_spans: &'a mut LocalSpans,
    ) -> Workflow<'a> {
        let services = Services {
            clock: self.clock,
            workflows: transaction,
            tracer: Some(TracingEventReceiver::new(
                self.tracing_metadata,
                spans,
                local_spans,
            )),
        };
        self.persisted.restore(self.spawner, services).unwrap()
    }
}

/// Part of the manager used during workflow instantiation.
#[derive(Debug, Clone)]
struct Shared {
    clock: Arc<dyn Clock>,
    spawners: Arc<HashMap<String, WorkflowSpawner<()>>>,
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            clock: Arc::new(Utc::now),
            spawners: Arc::new(HashMap::new()),
        }
    }
}

impl Shared {
    #[tracing::instrument(level = "debug", skip(self, committed), fields(definition_id))]
    fn restore_workflow(
        &self,
        committed: &mut PersistedWorkflows,
        id: WorkflowId,
    ) -> (WorkflowSeed<'_>, PersistedSpans) {
        let persisted = committed
            .workflows
            .get_mut(&id)
            .unwrap_or_else(|| panic!("workflow with ID {} is not persisted", id));
        let definition_id = &persisted.definition_id;
        tracing::Span::current().record("definition_id", definition_id);
        let spawner = &self.spawners[definition_id];
        tracing::debug!(?spawner, "using spawner to restore workflow");

        let tracing_metadata = committed.spawners[definition_id].tracing_metadata.clone();
        let template = WorkflowSeed {
            clock: self.clock.as_ref(),
            spawner,
            persisted: persisted.workflow.clone(),
            tracing_metadata,
        };
        let spans = mem::take(&mut persisted.tracing_spans);
        (template, spans)
    }
}

#[derive(Debug, Clone, Copy)]
enum ChannelSide {
    HostSender,
    WorkflowSender(WorkflowId),
    Receiver,
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
#[derive(Debug)]
pub struct WorkflowManager {
    shared: Shared,
    state: Mutex<PersistedWorkflows>,
    local_spans: HashMap<WorkflowId, LocalSpans>,
}

impl WorkflowManager {
    /// Creates a manager builder.
    pub fn builder() -> WorkflowManagerBuilder {
        WorkflowManagerBuilder {
            shared: Shared::default(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, PersistedWorkflows> {
        self.state.lock().unwrap()
    }

    fn check_consistency(&self) -> anyhow::Result<()> {
        let state = self.lock();
        for definition_id in self.shared.spawners.keys() {
            if !state.spawners.contains_key(definition_id) {
                anyhow::bail!("missing spawner metadata for workflow definition `{definition_id}`");
            }
        }

        for (&id, workflow_record) in &state.workflows {
            let definition_id = &workflow_record.definition_id;
            let spawner = self.shared.spawners.get(definition_id).ok_or_else(|| {
                anyhow::anyhow!("missing spawner for workflow definition `{definition_id}`")
            })?;
            workflow_record
                .workflow
                .check_on_restore(spawner)
                .with_context(|| {
                    format!(
                        "mismatch between interface of workflow {id} and spawner \
                         for workflow definition `{definition_id}`"
                    )
                })?;
        }
        Ok(())
    }

    /// Returns current information about the channel with the specified ID, or `None` if a channel
    /// with this ID does not exist.
    pub fn channel(&self, channel_id: ChannelId) -> Option<ChannelInfo> {
        Some(self.lock().channels.get(&channel_id)?.info())
    }

    /// Returns a handle to a workflow with the specified ID.
    pub fn workflow(&self, workflow_id: WorkflowId) -> Option<WorkflowHandle<'_, ()>> {
        self.lock().channel_ids(workflow_id).map(|channel_ids| {
            WorkflowHandle::new(
                self,
                WorkflowAndChannelIds {
                    workflow_id,
                    channel_ids,
                },
            )
        })
    }

    /// Checks whether this manager does not contain active workflows (e.g., all workflows
    /// spawned into the manager have finished).
    pub fn is_empty(&self) -> bool {
        self.lock().workflows.is_empty() // finished workflows are removed
    }

    pub(crate) fn persisted_workflow(&self, workflow_id: WorkflowId) -> PersistedWorkflow {
        self.lock().workflows[&workflow_id].workflow.clone()
    }

    pub(crate) fn interface_for_workflow(&self, workflow_id: WorkflowId) -> Option<&Interface> {
        let state = self.lock();
        let persisted = state.workflows.get(&workflow_id)?;
        Some(self.shared.spawners[&persisted.definition_id].interface())
    }

    pub(crate) fn send_message(
        &self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), SendError> {
        self.lock().send_message(channel_id, message.into())
    }

    pub(crate) fn close_host_sender(&self, channel_id: ChannelId) {
        let mut state = self.lock();
        state.close_channel_side(channel_id, ChannelSide::HostSender);
    }

    pub(crate) fn close_host_receiver(&self, channel_id: ChannelId) {
        let mut state = self.lock();
        debug_assert!(
            state.channels[&channel_id].receiver_workflow_id.is_none(),
            "Attempted to close channel {} for which the host doesn't hold receiver",
            channel_id
        );
        state.close_channel_side(channel_id, ChannelSide::Receiver);
    }

    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(ret.start_idx, ret.count)
    )]
    pub(crate) fn take_outbound_messages(&self, channel_id: ChannelId) -> (usize, Vec<Message>) {
        let mut state = self.lock();
        let channel_state = state.channels.get_mut(&channel_id).unwrap();
        assert!(
            channel_state.receiver_workflow_id.is_none(),
            "cannot receive a message for a channel with the receiver connected to a workflow"
        );

        let (start_idx, messages) = channel_state.drain_messages();
        tracing::Span::current()
            .record("ret.start_idx", start_idx)
            .record("ret.count", messages.len());
        (start_idx, messages.into())
    }

    /// Atomically pops a message from a channel and feeds it to a listening workflow.
    /// If the workflow execution is successful, its results are committed to the manager state;
    /// otherwise, the results are reverted.
    #[tracing::instrument(skip(self), err)]
    fn feed_message_to_workflow(
        &mut self,
        channel_id: ChannelId,
        workflow_id: WorkflowId,
    ) -> Result<Receipt, ExecutionError> {
        let state = self.state.get_mut().unwrap();
        let (message, is_closed) = state.take_message(channel_id).expect("no message to feed");
        let workflow = &state.workflows[&workflow_id].workflow;
        let (child_id, channel_name) = workflow.find_inbound_channel(channel_id);

        let (template, mut spans) = self.shared.restore_workflow(state, workflow_id);
        let transaction = Transaction::new(state, Some(workflow_id), self.shared.clone());
        let local_spans = self.local_spans.entry(workflow_id).or_default();
        let mut workflow = template.restore(&transaction, &mut spans, local_spans);
        let result = Self::push_message(&mut workflow, child_id, &channel_name, message.clone());

        if let Ok(receipt) = &result {
            let mut is_consumed = true;
            if workflow.take_pending_inbound_message(child_id, &channel_name) {
                // The message was not consumed. We still persist the workflow in order to
                // consume wakers (otherwise, we would loop indefinitely), and place the message
                // back to the channel.
                tracing::warn!(
                    ?message,
                    "message was not consumed by workflow; placing the message back"
                );
                is_consumed = false;
                state.revert_taking_message(channel_id, message);
            }

            {
                let span = tracing::debug_span!("persist_workflow_and_messages", workflow_id);
                let _entered = span.enter();
                let messages = workflow.drain_messages();
                state.persist_workflow(workflow_id, workflow);
                state.persist_tracing_spans(workflow_id, spans);
                state.commit(transaction, receipt);
                state.push_messages(messages);
            }

            if is_consumed && is_closed {
                // Signal to the workflow that the channel is closed. This can be performed
                // on a persisted workflow, without executing it.
                let workflow_record = state.workflows.get_mut(&workflow_id).unwrap();
                workflow_record
                    .workflow
                    .close_inbound_channel(child_id, &channel_name);
            }
        } else {
            // Do not commit the execution result. Instead, put the message back to the channel.
            state.revert_taking_message(channel_id, message);
        }
        result
    }

    /// Returns `Ok(None)` if the message cannot be consumed right now (the workflow channel
    /// does not listen to it).
    #[tracing::instrument(level = "debug", skip(workflow))]
    fn push_message(
        workflow: &mut Workflow,
        child_id: Option<WorkflowId>,
        channel_name: &str,
        message: Message,
    ) -> Result<Receipt, ExecutionError> {
        workflow
            .push_inbound_message(child_id, channel_name, message.into())
            .unwrap();
        workflow.tick()
    }

    /// Sets the current time for this manager. This may expire timers in some of the contained
    /// workflows.
    #[tracing::instrument(skip(self))]
    pub fn set_current_time(&self, time: DateTime<Utc>) {
        let mut state = self.lock();
        for persisted in state.workflows.values_mut() {
            persisted.workflow.set_current_time(time);
        }
    }

    #[tracing::instrument(skip(self), err)]
    pub(crate) fn tick_workflow(
        &mut self,
        workflow_id: WorkflowId,
    ) -> Result<Receipt, ExecutionError> {
        let state = self.state.get_mut().unwrap();
        let (template, mut spans) = self.shared.restore_workflow(state, workflow_id);
        let transaction = Transaction::new(state, Some(workflow_id), self.shared.clone());
        let local_spans = self.local_spans.entry(workflow_id).or_default();
        let mut workflow = template.restore(&transaction, &mut spans, local_spans);

        let result = workflow.tick();
        if let Ok(receipt) = &result {
            let span = tracing::debug_span!("persist_workflow_and_messages", workflow_id);
            let _entered = span.enter();
            let messages = workflow.drain_messages();
            state.persist_workflow(workflow_id, workflow);
            state.persist_tracing_spans(workflow_id, spans);
            state.commit(transaction, receipt);
            state.push_messages(messages);
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
    pub fn abort_workflow(&mut self, workflow_id: WorkflowId) {
        let state = self.state.get_mut().unwrap();
        let persisted = state
            .workflows
            .get_mut(&workflow_id)
            .expect("workflow not found");
        persisted.workflow.abort();
        state.handle_workflow_update(workflow_id);
    }

    /// Accesses persisted views of the workflows managed by this manager.
    pub fn persist<T>(&self, persist_fn: impl FnOnce(&PersistedWorkflows) -> T) -> T {
        persist_fn(&self.lock())
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
    pub fn tick(&mut self) -> Result<TickResult<Actions<'_>>, WouldBlock> {
        let span = tracing::info_span!("tick", workflow_id = field::Empty, err = field::Empty);
        let _entered = span.enter();
        let state = self.state.get_mut().unwrap();

        let workflow_id = state.find_workflow_with_pending_tasks();
        if let Some(workflow_id) = workflow_id {
            span.record("workflow_id", workflow_id);

            let result = self.tick_workflow(workflow_id);
            return Ok(TickResult {
                workflow_id,
                extra: Actions::new(self, result.is_err()),
                result,
            });
        }

        let ids = state.find_consumable_channel();
        if let Some((channel_id, workflow_id)) = ids {
            span.record("workflow_id", workflow_id);

            let result = self.feed_message_to_workflow(channel_id, workflow_id);
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
            nearest_timer_expiration: state.nearest_timer_expiration(),
        };
        span.record("err", &err as &dyn error::Error);
        Err(err)
    }
}

/// Builder for a [`WorkflowManager`].
#[derive(Debug)]
pub struct WorkflowManagerBuilder {
    shared: Shared,
}

impl WorkflowManagerBuilder {
    /// Sets the wall clock to be used in the manager.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<impl Clock>) -> Self {
        self.shared.clock = clock;
        self
    }

    /// Inserts a workflow spawner into the manager.
    #[must_use]
    pub fn with_spawner<W: WorkflowFn>(mut self, id: &str, spawner: WorkflowSpawner<W>) -> Self {
        let spawners = Arc::get_mut(&mut self.shared.spawners).unwrap();
        spawners.insert(id.to_owned(), spawner.erase());
        self
    }

    /// Finishes building the manager.
    pub fn build(self) -> WorkflowManager {
        let spawner_ids = self.shared.spawners.keys().cloned();
        WorkflowManager {
            state: Mutex::new(PersistedWorkflows::new(spawner_ids)),
            shared: self.shared,
            local_spans: HashMap::new(),
        }
    }

    /// Restores the manager with the provided persisted state.
    ///
    /// # Errors
    ///
    /// Returns an error if the state cannot be restored (e.g., there are missing spawners,
    /// or some of spawners do not provide expected workflow interface).
    pub fn restore(self, workflows: PersistedWorkflows) -> anyhow::Result<WorkflowManager> {
        let manager = WorkflowManager {
            shared: self.shared,
            state: Mutex::new(workflows),
            local_spans: HashMap::new(),
        };
        manager.check_consistency()?;
        Ok(manager)
    }
}

impl ManageInterfaces for WorkflowManager {
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        Some(Cow::Borrowed(
            self.shared.spawners.get(definition_id)?.interface(),
        ))
    }
}

impl SpecifyWorkflowChannels for WorkflowManager {
    type Inbound = ChannelId;
    type Outbound = ChannelId;
}

impl<'a, W: WorkflowFn> ManageWorkflows<'a, W> for WorkflowManager {
    type Handle = WorkflowHandle<'a, W>;
    type Error = anyhow::Error;

    fn create_workflow(
        &'a self,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<ChannelId>,
    ) -> Result<Self::Handle, Self::Error> {
        let transaction = Transaction::new(&self.lock(), None, self.shared.clone());
        transaction.create_workflow(definition_id, args, channels)?;

        let workflow_id = transaction.single_new_workflow_id().unwrap();
        self.lock().commit(transaction, &Receipt::default());
        Ok(self.workflow(workflow_id).unwrap().downcast_unchecked())
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
