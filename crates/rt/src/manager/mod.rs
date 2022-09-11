//! `WorkflowManager` and tightly related types.

#![allow(missing_docs)] // FIXME
#![allow(clippy::missing_panics_doc)] // lots of `unwrap()`s on mutex locks

use chrono::{DateTime, Utc};

use std::{
    borrow::Cow,
    collections::HashMap,
    ops,
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
    interface::Interface,
    spawn::{ChannelHandles, ManageInterfaces, ManageWorkflows},
    workflow::WorkflowFn,
};
use tardigrade_shared::{ChannelId, SendError, SpawnError, WorkflowId};

#[derive(Debug)]
pub struct ChannelInfo {
    is_closed: bool,
    message_count: usize,
    next_message_idx: usize,
}

impl ChannelInfo {
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn received_messages(&self) -> usize {
        self.next_message_idx
    }

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

#[derive(Debug)]
#[must_use = "Result can contain an execution error which should be handled"]
pub struct TickResult<'a> {
    workflow_id: WorkflowId,
    result: Result<Receipt, ExecutionError>,
    drop_message_action: Option<DropMessageAction<'a>>,
}

impl TickResult<'_> {
    pub fn workflow_id(&self) -> WorkflowId {
        self.workflow_id
    }

    #[allow(clippy::missing_errors_doc)] // doesn't make sense semantically
    pub fn as_ref(&self) -> Result<&Receipt, &ExecutionError> {
        self.result.as_ref()
    }

    pub fn can_drop_erroneous_message(&self) -> bool {
        self.drop_message_action.is_some()
    }

    pub fn drop_erroneous_message(&mut self) {
        let action = self
            .drop_message_action
            .take()
            .expect("cannot drop message");
        action.execute();
    }

    /// Returns the underlying execution result.
    #[allow(clippy::missing_errors_doc)] // doesn't make sense semantically
    pub fn into_inner(self) -> Result<Receipt, ExecutionError> {
        self.result
    }
}

#[derive(Debug)]
pub struct WouldBlock {
    nearest_timer_expiration: Option<DateTime<Utc>>,
}

impl WouldBlock {
    pub fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>> {
        self.nearest_timer_expiration
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

/// Simple in-memory implementation of a workflow manager.
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

    fn services<'a>(&'a self, transaction: &'a Transaction) -> Services<'a> {
        Services {
            clock: self.shared.clock.as_ref(),
            workflows: transaction,
        }
    }

    pub(crate) fn channel_info(&self, channel_id: ChannelId) -> ChannelInfo {
        self.lock().channels[&channel_id].info()
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
        self.lock()
            .workflows
            .values()
            .all(|persisted| persisted.workflow.is_finished())
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

    pub(crate) fn close_channel_sender(&self, channel_id: ChannelId) {
        self.lock().close_channel_sender(channel_id);
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
        let mut workflow = self.restore_workflow(&state, workflow_id, &transaction);
        let result = Self::push_message(&mut workflow, child_id, &channel_name, message.clone());
        if let Ok(Some(receipt)) = &result {
            state.drain_and_persist_workflow(workflow_id, None, workflow);
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

    // TODO: clearly not efficient
    pub fn set_current_time(&self, time: DateTime<Utc>) {
        let mut state = self.lock();
        for persisted in state.workflows.values_mut() {
            persisted.workflow.set_current_time(time);
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
        let spawner = &self.shared.spawners[&persisted.definition_id];
        persisted
            .workflow
            .clone()
            .restore(spawner, self.services(transaction))
            .unwrap()
    }

    pub(crate) fn tick_workflow(&self, workflow_id: WorkflowId) -> Result<Receipt, ExecutionError> {
        let mut state = self.lock();
        let transaction = Transaction::new(&state, Some(workflow_id), self.shared.clone());
        let mut workflow = self.restore_workflow(&state, workflow_id, &transaction);

        let result = workflow.tick();
        if let Ok(receipt) = &result {
            state.drain_and_persist_workflow(workflow_id, None, workflow);
            state.commit(transaction, receipt);
        }
        result
    }

    pub fn persist<T>(&self, persist_fn: impl FnOnce(&PersistedWorkflows) -> T) -> T {
        persist_fn(&self.lock())
    }

    /// # Errors
    ///
    /// Returns an error if the manager cannot be progressed.
    pub fn tick(&mut self) -> Result<TickResult<'_>, WouldBlock> {
        let state = self.state.get_mut().unwrap();

        let workflow_id = state.find_workflow_with_pending_tasks();
        if let Some(workflow_id) = workflow_id {
            let result = self.tick_workflow(workflow_id);
            return Ok(TickResult {
                workflow_id,
                result,
                drop_message_action: None,
            });
        }

        let ids = state.find_consumable_channel();
        if let Some((channel_id, workflow_id)) = ids {
            let result = self.feed_message_to_workflow(channel_id, workflow_id);
            return Ok(TickResult {
                workflow_id,
                drop_message_action: if result.is_err() {
                    Some(DropMessageAction {
                        manager: self,
                        channel_id,
                    })
                } else {
                    None
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

    /// Sets the clock to be used in the manager.
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

    fn create_workflow(
        &'a self,
        definition_id: &str,
        args: Vec<u8>,
        handles: &ChannelHandles,
    ) -> Result<Self::Handle, SpawnError> {
        let transaction = Transaction::new(&self.lock(), None, self.shared.clone());
        let services = self.services(&transaction);
        services
            .workflows
            .create_workflow(definition_id, args, handles)?;

        let workflow_id = transaction.single_new_workflow_id().unwrap();
        self.lock().commit(transaction, &Receipt::new());
        Ok(self.workflow(workflow_id).unwrap().downcast_unchecked())
    }
}

impl Receipt {
    fn closed_channel_ids(&self) -> impl Iterator<Item = ChannelId> + '_ {
        self.executions().iter().flat_map(|execution| {
            execution.events.iter().filter_map(|event| {
                if let Some(ChannelEvent {
                    kind: ChannelEventKind::InboundChannelClosed(channel_id),
                    ..
                }) = event.as_channel_event()
                {
                    Some(*channel_id)
                } else {
                    None
                }
            })
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
