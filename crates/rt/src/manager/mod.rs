//! [`WorkflowManager`] and tightly related types.
//!
//! See `WorkflowManager` docs for an overview and examples of usage.

#![allow(clippy::missing_panics_doc)] // lots of `unwrap()`s on mutex locks

use chrono::{DateTime, Utc};

use std::{
    borrow::Cow,
    collections::HashMap,
    error, fmt, ops,
    sync::{Arc, Mutex},
};

mod persistence;
#[cfg(test)]
mod tests;
mod transaction;

pub use self::persistence::PersistedWorkflows;

use self::transaction::Transaction;
use crate::{
    handle::WorkflowHandle,
    module::{Clock, Services, WorkflowAndChannelIds},
    receipt::{ChannelEvent, ChannelEventKind, ExecutionError, Receipt},
    utils::Message,
    workflow::Workflow,
    PersistedWorkflow, WorkflowSpawner,
};
use tardigrade::{
    interface::{ChannelKind, Interface},
    spawn::{ChannelsConfig, ManageInterfaces, ManageWorkflows},
    workflow::WorkflowFn,
};
use tardigrade_shared::{ChannelId, SendError, WorkflowId};

/// Information about a channel managed by a [`WorkflowManager`].
#[derive(Debug)]
pub struct ChannelInfo {
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
}

#[derive(Debug)]
struct DropMessageAction<'a> {
    manager: &'a mut WorkflowManager,
    channel_id: ChannelId,
}

impl DropMessageAction<'_> {
    fn execute(self) {
        let state = self.manager.state.get_mut().unwrap();
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

/// Actions
#[derive(Debug)]
pub struct Actions<'a> {
    drop_message_action: Option<DropMessageAction<'a>>,
}

impl TickResult<Actions<'_>> {
    /// Returns true if the workflow execution failed and the execution was triggered
    /// by consuming a message from a certain channel.
    pub fn can_drop_erroneous_message(&self) -> bool {
        self.extra.drop_message_action.is_some()
    }

    /// Drops a message that led to erroneous workflow execution.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::can_drop_erroneous_message()`] returns `false`, or if called more than
    /// once on the same object.
    pub fn drop_erroneous_message(mut self) -> TickResult<()> {
        let action = self
            .extra
            .drop_message_action
            .take()
            .expect("cannot drop message");
        action.execute();

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
    fn services<'a>(&'a self, transaction: &'a Transaction) -> Services<'a> {
        Services {
            clock: self.clock.as_ref(),
            workflows: transaction,
        }
    }

    // FIXME: sane error handling
    fn restore_workflow<'a>(
        &'a self,
        committed: &PersistedWorkflows,
        id: WorkflowId,
        transaction: &'a Transaction,
    ) -> Workflow<'a> {
        let persisted = committed
            .workflows
            .get(&id)
            .unwrap_or_else(|| panic!("workflow with ID {} is not persisted", id));
        let spawner = &self.spawners[&persisted.definition_id];
        persisted
            .workflow
            .clone()
            .restore(spawner, self.services(transaction))
            .unwrap()
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
/// [`AsyncEnv`]: crate::handle::future::AsyncEnv
///
/// # Examples
///
/// ```
/// # use tardigrade_rt::{handle::WorkflowHandle, manager::WorkflowManager, WorkflowModule};
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
/// let workflow: WorkflowHandle<()> =
///     manager.new_workflow("test", args)?.build()?;
/// // Do something with `workflow`, e.g., write something to its channels...
///
/// // Initialize the workflow:
/// let receipt = manager.tick()?.into_inner()?;
/// println!("{:?}", receipt);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default)]
pub struct WorkflowManager {
    shared: Shared,
    state: Mutex<PersistedWorkflows>,
}

impl WorkflowManager {
    /// Creates a manager builder.
    pub fn builder() -> WorkflowManagerBuilder {
        WorkflowManagerBuilder {
            manager: Self::default(),
        }
    }

    fn lock(&self) -> impl ops::DerefMut<Target = PersistedWorkflows> + '_ {
        self.state.lock().unwrap()
    }

    /// Returns current information about the channel with the specified ID, or `None` if a channel
    /// with this ID does not exist.
    pub fn channel_info(&self, channel_id: ChannelId) -> Option<ChannelInfo> {
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

    /// Checks whether all workflows contained in this manager are finished.
    pub fn is_finished(&self) -> bool {
        self.lock().workflows.is_empty() // finished workflows are removed
    }

    pub(crate) fn persisted_workflow(&self, workflow_id: WorkflowId) -> PersistedWorkflow {
        self.lock().workflows[&workflow_id].workflow.clone()
    }

    pub(crate) fn interface_for_workflow(&self, workflow_id: WorkflowId) -> Option<&Interface<()>> {
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

    pub(crate) fn take_outbound_messages(&self, channel_id: ChannelId) -> (usize, Vec<Message>) {
        let mut state = self.lock();
        let channel_state = state.channels.get_mut(&channel_id).unwrap();
        assert!(
            channel_state.receiver_workflow_id.is_none(),
            "cannot receive a message for a channel with the receiver connected to a workflow"
        );
        let (start_idx, messages) = channel_state.drain_messages();
        (start_idx, messages.into())
    }

    /// Atomically pops a message from a channel and feeds it to a listening workflow.
    /// If the workflow execution is successful, its results are committed to the manager state;
    /// otherwise, the results are reverted.
    fn feed_message_to_workflow(
        &self,
        channel_id: ChannelId,
        workflow_id: WorkflowId,
    ) -> Result<Option<Receipt>, ExecutionError> {
        let mut state = self.lock();
        let (message, is_closed) = state.take_message(channel_id).expect("no message to feed");
        let workflow = &state.workflows[&workflow_id].workflow;
        let (child_id, channel_name) = workflow.find_inbound_channel(channel_id);

        let transaction = Transaction::new(&state, Some(workflow_id), self.shared.clone());
        let mut workflow = self
            .shared
            .restore_workflow(&state, workflow_id, &transaction);
        let result = Self::push_message(&mut workflow, child_id, &channel_name, message.clone());
        if let Ok(Some(receipt)) = &result {
            state.drain_and_persist_workflow(workflow_id, workflow);
            state.commit(transaction, receipt);

            if is_closed {
                // Signal to the workflow that the channel is closed. This can be performed
                // on a persisted workflow, without executing it.
                let workflow_record = state.workflows.get_mut(&workflow_id).unwrap();
                workflow_record
                    .workflow
                    .close_inbound_channel(child_id, &channel_name);
            }
        } else {
            // Do not commit the execution result. Instead, put the message back to the channel.
            // Note that this happens both in the case of `ExecutionError`, and if the message
            // was not consumed by the workflow.
            if result.is_ok() {
                crate::warn!(
                    "Message {:?} over channel {} was not consumed by workflow with ID {}; \
                     reverting workflow execution",
                    message,
                    channel_id,
                    workflow_id
                );
            }
            state.revert_taking_message(channel_id, message);
        }
        result
    }

    /// Returns `Ok(None)` if the message cannot be consumed right now (the workflow channel
    /// does not listen to it).
    fn push_message(
        workflow: &mut Workflow,
        child_id: Option<WorkflowId>,
        channel_name: &str,
        message: Message,
    ) -> Result<Option<Receipt>, ExecutionError> {
        let push_result = workflow.push_inbound_message(child_id, channel_name, message.into());
        if push_result.is_err() {
            Ok(None) // FIXME: panic here? (instead, return `None` if the message was not consumed)
        } else {
            workflow.tick().map(Some)
        }
    }

    /// Sets the current time for this manager. This may expire timers in some of the contained
    /// workflows.
    pub fn set_current_time(&self, time: DateTime<Utc>) {
        let mut state = self.lock();
        for persisted in state.workflows.values_mut() {
            persisted.workflow.set_current_time(time);
        }
    }

    pub(crate) fn tick_workflow(
        &mut self,
        workflow_id: WorkflowId,
    ) -> Result<Receipt, ExecutionError> {
        let state = self.state.get_mut().unwrap();
        let transaction = Transaction::new(state, Some(workflow_id), self.shared.clone());
        let mut workflow = self
            .shared
            .restore_workflow(state, workflow_id, &transaction);

        let result = workflow.tick();
        if let Ok(receipt) = &result {
            state.drain_and_persist_workflow(workflow_id, workflow);
            state.commit(transaction, receipt);
        }
        result
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
        let state = self.state.get_mut().unwrap();

        let workflow_id = state.find_workflow_with_pending_tasks();
        if let Some(workflow_id) = workflow_id {
            let result = self.tick_workflow(workflow_id);
            return Ok(TickResult {
                workflow_id,
                result,
                extra: Actions {
                    drop_message_action: None,
                },
            });
        }

        let ids = state.find_consumable_channel();
        if let Some((channel_id, workflow_id)) = ids {
            let result = self.feed_message_to_workflow(channel_id, workflow_id);
            return Ok(TickResult {
                workflow_id,
                extra: Actions {
                    drop_message_action: if result.is_err() {
                        Some(DropMessageAction {
                            manager: self,
                            channel_id,
                        })
                    } else {
                        None
                    },
                },
                result: result.map(|receipt| receipt.unwrap_or_else(Receipt::new)),
            });
        }

        Err(WouldBlock {
            nearest_timer_expiration: state.nearest_timer_expiration(),
        })
    }
}

/// Builder for a [`WorkflowManager`].
#[derive(Debug)]
pub struct WorkflowManagerBuilder {
    manager: WorkflowManager,
}

impl WorkflowManagerBuilder {
    /// Sets the initial state of the workflows managed by this manager.
    #[must_use]
    pub fn with_state(mut self, workflows: PersistedWorkflows) -> Self {
        *self.manager.state.get_mut().unwrap() = workflows;
        self
    }

    /// Sets the wall clock to be used in the manager.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<impl Clock>) -> Self {
        self.manager.shared.clock = clock;
        self
    }

    /// Inserts a workflow spawner into the manager.
    #[must_use]
    pub fn with_spawner<W: WorkflowFn>(mut self, id: &str, spawner: WorkflowSpawner<W>) -> Self {
        let spawners = Arc::get_mut(&mut self.manager.shared.spawners).unwrap();
        spawners.insert(id.to_owned(), spawner.erase());
        self
    }

    /// Finishes building the manager.
    pub fn build(self) -> WorkflowManager {
        self.manager
    }
}

impl ManageInterfaces for WorkflowManager {
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface<()>>> {
        Some(Cow::Borrowed(
            self.shared.spawners.get(definition_id)?.interface(),
        ))
    }
}

impl<'a, W: WorkflowFn> ManageWorkflows<'a, W> for WorkflowManager {
    type Handle = WorkflowHandle<'a, W>;
    type Error = anyhow::Error;

    fn create_workflow(
        &'a self,
        definition_id: &str,
        args: Vec<u8>,
        channels: &ChannelsConfig,
    ) -> Result<Self::Handle, Self::Error> {
        let transaction = Transaction::new(&self.lock(), None, self.shared.clone());
        let services = self.shared.services(&transaction);
        services
            .workflows
            .create_workflow(definition_id, args, channels)?;

        let workflow_id = transaction.single_new_workflow_id().unwrap();
        self.lock().commit(transaction, &Receipt::new());
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
                    ChannelEventKind::OutboundChannelClosed(channel_id) => {
                        Some((ChannelKind::Outbound, *channel_id))
                    }
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
