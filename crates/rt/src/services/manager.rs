//! `WorkflowManager` and tightly related types.

#![allow(missing_docs)] // FIXME
#![allow(clippy::missing_panics_doc)] // lots of `unwrap()`s on mutex locks

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    mem, ops,
    sync::{Arc, Mutex, Weak},
};

#[cfg(test)]
mod tests;

use crate::{
    handle::WorkflowEnv,
    receipt::{ChannelEvent, ChannelEventKind, ExecutionError, Receipt},
    services::{Clock, Services, WorkflowAndChannelIds},
    utils::Message,
    workflow::ChannelIds,
    PersistedWorkflow, Workflow, WorkflowSpawner,
};
use tardigrade::{
    interface::Interface,
    spawn::{ChannelHandles, ManageWorkflows, Spawner, WorkflowBuilder},
    workflow::{TakeHandle, WorkflowFn},
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

/// Simple implementation of a workflow manager.
#[derive(Debug)]
pub struct WorkflowManager {
    clock: Arc<dyn Clock>,
    arc: Weak<Self>,
    spawners: HashMap<String, WorkflowSpawner<()>>,
    state: Mutex<WorkflowManagerState>,
}

impl Default for WorkflowManager {
    fn default() -> Self {
        Self {
            clock: Arc::new(Utc::now),
            arc: Weak::new(),
            spawners: HashMap::new(),
            state: Mutex::default(),
        }
    }
}

impl WorkflowManager {
    /// Creates a manager builder.
    pub fn builder() -> WorkflowManagerBuilder {
        WorkflowManagerBuilder {
            manager: Self::default(),
        }
    }

    fn lock(&self) -> impl ops::DerefMut<Target = WorkflowManagerState> + '_ {
        self.state.lock().unwrap()
    }

    fn services(&self) -> Services {
        Services {
            clock: Arc::clone(&self.clock),
            workflows: self.arc.upgrade().unwrap(),
            // ^ `unwrap()` should be safe provided this method is called from `Arc<Self>`
        }
    }

    pub(crate) fn channel_info(&self, channel_id: ChannelId) -> ChannelInfo {
        self.lock().committed.channels[&channel_id].info()
    }

    /// Returns a handle to a workflow with the specified ID.
    pub fn workflow(&self, workflow_id: WorkflowId) -> Option<WorkflowEnv<'_, ()>> {
        self.lock()
            .committed
            .channel_ids(workflow_id)
            .map(|channel_ids| {
                WorkflowEnv::new(
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
            .committed
            .workflows
            .values()
            .all(|persisted| persisted.workflow.is_finished())
    }

    pub(crate) fn persisted_workflow(&self, workflow_id: WorkflowId) -> PersistedWorkflow {
        self.lock().committed.workflows[&workflow_id]
            .workflow
            .clone()
    }

    pub(crate) fn interface_for_workflow(&self, workflow_id: WorkflowId) -> Option<&Interface<()>> {
        let state = self.lock();
        let persisted = state.committed.workflows.get(&workflow_id)?;
        Some(self.spawners[&persisted.definition_id].interface())
    }

    pub(crate) fn send_message(
        &self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), SendError> {
        self.lock()
            .committed
            .send_message(channel_id, message.into())
    }

    pub(crate) fn close_channel_sender(&self, channel_id: ChannelId) {
        self.lock().committed.close_channel_sender(channel_id);
    }

    pub(crate) fn take_outbound_messages(&self, channel_id: ChannelId) -> (usize, Vec<Message>) {
        let mut state = self.lock();
        let channel_state = state.committed.channels.get_mut(&channel_id).unwrap();
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
        let message = state.committed.take_message(channel_id);
        let message = if let Some(message) = message {
            Some(message)
        } else if state.committed.channels[&channel_id].is_closed {
            None
        } else {
            return Ok(None);
        };

        let workflow = &state.committed.workflows[&workflow_id].workflow;
        let (child_id, channel_name) = workflow
            .inbound_channels()
            .find_map(|(child_id, name, state)| {
                if state.id() == channel_id {
                    Some((child_id, name.to_owned()))
                } else {
                    None
                }
            })
            .unwrap();

        let mut workflow = self.restore_workflow(&state.committed, workflow_id);
        state.executing_workflow_id = Some(workflow_id);
        let result = Self::push_message(&mut workflow, child_id, &channel_name, message.clone());
        if let Ok(Some(receipt)) = &result {
            state.commit(receipt);
            state
                .committed
                .drain_and_persist_workflow(workflow_id, None, &mut workflow);
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
            if let Some(message) = message {
                state.committed.revert_taking_message(channel_id, message);
            }
        }
        state.executing_workflow_id = None;
        result
    }

    /// Returns `Ok(None)` if the message cannot be consumed right now (the workflow channel
    /// does not listen to it).
    fn push_message(
        workflow: &mut Workflow<()>,
        child_id: Option<WorkflowId>,
        channel_name: &str,
        message: Option<Message>,
    ) -> Result<Option<Receipt>, ExecutionError> {
        let push_result = if let Some(message) = message {
            workflow.push_inbound_message(child_id, channel_name, message.into())
        } else {
            workflow.close_inbound_channel(child_id, channel_name)
        };
        if push_result.is_err() {
            Ok(None)
        } else {
            workflow.tick().map(Some)
        }
    }

    // TODO: clearly not efficient
    pub fn set_current_time(&self, time: DateTime<Utc>) {
        let mut state = self.lock();
        for persisted in state.committed.workflows.values_mut() {
            persisted.workflow.set_current_time(time);
        }
    }

    // FIXME: sane error handling
    fn restore_workflow(&self, committed: &PersistedWorkflows, id: WorkflowId) -> Workflow<()> {
        let persisted = committed
            .workflows
            .get(&id)
            .unwrap_or_else(|| panic!("workflow with ID {} is not persisted", id));
        let spawner = &self.spawners[&persisted.definition_id];
        persisted
            .workflow
            .clone()
            .restore(spawner, self.services())
            .unwrap()
    }

    pub(crate) fn tick_workflow(&self, workflow_id: WorkflowId) -> Result<Receipt, ExecutionError> {
        let mut state = self.lock();
        let mut workflow = self.restore_workflow(&state.committed, workflow_id);

        state.executing_workflow_id = Some(workflow_id);
        let result = workflow.tick();
        if let Ok(receipt) = &result {
            state.commit(receipt);
            state
                .committed
                .drain_and_persist_workflow(workflow_id, None, &mut workflow);
        }
        state.executing_workflow_id = None;
        result
    }

    pub fn persist<T>(&self, persist_fn: impl FnOnce(&PersistedWorkflows) -> T) -> T {
        persist_fn(&self.lock().committed)
    }

    /// # Errors
    ///
    /// Returns an error if the manager cannot be progressed.
    // FIXME: require `&mut self` or lock for the entire duration
    pub fn tick(&self) -> Result<TickResult<'_>, WouldBlock> {
        let workflow_id = self.lock().committed.find_workflow_with_pending_tasks();
        if let Some(workflow_id) = workflow_id {
            let result = self.tick_workflow(workflow_id);
            return Ok(TickResult {
                workflow_id,
                result,
                drop_message_action: None,
            });
        }

        let ids = self.lock().committed.find_consumable_channel();
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
            nearest_timer_expiration: self.lock().committed.nearest_timer_expiration(),
        })
    }
}

#[derive(Debug)]
struct DropMessageAction<'a> {
    manager: &'a WorkflowManager,
    channel_id: ChannelId,
}

impl DropMessageAction<'_> {
    fn execute(self) {
        self.manager.lock().committed.take_message(self.channel_id);
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

impl ManageWorkflows for WorkflowManager {
    type Handle = WorkflowAndChannelIds;

    fn interface(&self, id: &str) -> Option<Cow<'_, Interface<()>>> {
        Some(Cow::Borrowed(self.spawners.get(id)?.interface()))
    }

    fn create_workflow(
        &self,
        id: &str,
        args: Vec<u8>,
        handles: &ChannelHandles,
    ) -> Result<Self::Handle, SpawnError> {
        let spawner = self
            .spawners
            .get(id)
            .unwrap_or_else(|| panic!("workflow with ID `{}` is not defined", id));

        let channel_ids = {
            let mut state = self.state.lock().unwrap();
            ChannelIds::new(handles, || state.committed.allocate_channel_id())
        };
        let mut workflow = spawner
            .spawn(args, &channel_ids, self.services())
            .map_err(|err| SpawnError::new(err.to_string()))?;
        let workflow_id = self
            .state
            .lock()
            .unwrap()
            .stash_workflow(id.to_owned(), &mut workflow);
        Ok(WorkflowAndChannelIds {
            workflow_id,
            channel_ids,
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
        self.manager.state.get_mut().unwrap().committed = workflows;
        self
    }

    /// Sets the clock to be used in the manager.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<impl Clock>) -> Self {
        self.manager.clock = clock;
        self
    }

    /// Inserts a workflow spawner into the manager.
    #[must_use]
    pub fn with_spawner<W: WorkflowFn>(mut self, id: &str, spawner: WorkflowSpawner<W>) -> Self {
        self.manager.spawners.insert(id.to_owned(), spawner.erase());
        self
    }

    /// Finishes building the manager.
    pub fn build(self) -> Arc<WorkflowManager> {
        let mut manager = self.manager;
        Arc::new_cyclic(|arc| {
            manager.arc = Weak::clone(arc);
            manager
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ChannelState {
    receiver_workflow_id: Option<WorkflowId>, // `None` means external receiver
    is_closed: bool,
    messages: VecDeque<Message>,
    next_message_idx: usize,
}

impl ChannelState {
    fn new(receiver_workflow_id: Option<WorkflowId>) -> Self {
        Self {
            receiver_workflow_id,
            is_closed: false,
            messages: VecDeque::new(),
            next_message_idx: 0,
        }
    }

    fn info(&self) -> ChannelInfo {
        ChannelInfo {
            is_closed: self.is_closed,
            message_count: self.messages.len(),
            next_message_idx: self.next_message_idx,
        }
    }

    fn push_messages(
        &mut self,
        messages: impl IntoIterator<Item = Message>,
    ) -> Result<(), SendError> {
        if self.is_closed {
            return Err(SendError::Closed);
        }
        let old_len = self.messages.len();
        self.messages.extend(messages);
        self.next_message_idx += self.messages.len() - old_len;
        Ok(())
    }

    fn can_provide_message(&self) -> bool {
        !self.messages.is_empty() || self.is_closed
    }

    fn pop_message(&mut self) -> Option<Message> {
        self.messages.pop_front()
    }

    fn drain_messages(&mut self) -> (usize, VecDeque<Message>) {
        let start_idx = self.next_message_idx - self.messages.len();
        (start_idx, mem::take(&mut self.messages))
    }

    fn close(&mut self) {
        self.is_closed = true;
    }
}

/// Wrapper for either ongoing or persisted workflow.
#[derive(Debug, Serialize, Deserialize)]
struct WorkflowWithMeta {
    definition_id: String,
    workflow: PersistedWorkflow,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedWorkflows {
    workflows: HashMap<WorkflowId, WorkflowWithMeta>,
    next_workflow_id: WorkflowId,
    channels: HashMap<ChannelId, ChannelState>,
    next_channel_id: ChannelId,
}

impl Default for PersistedWorkflows {
    fn default() -> Self {
        Self {
            workflows: HashMap::new(),
            next_workflow_id: 0,
            channels: HashMap::new(),
            next_channel_id: 1, // avoid "pre-closed" channel ID
        }
    }
}

impl PersistedWorkflows {
    fn allocate_channel_id(&mut self) -> ChannelId {
        let channel_id = self.next_channel_id;
        self.next_channel_id += 1;
        channel_id
    }

    fn take_message(&mut self, channel_id: ChannelId) -> Option<Message> {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.pop_message()
    }

    fn revert_taking_message(&mut self, channel_id: ChannelId, message: Message) {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.messages.push_front(message);
    }

    fn send_message(&mut self, channel_id: ChannelId, message: Message) -> Result<(), SendError> {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.push_messages([message])
    }

    fn close_channel_sender(&mut self, channel_id: ChannelId) {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.close();
    }

    fn channel_ids(&self, workflow_id: WorkflowId) -> Option<ChannelIds> {
        let persisted = &self.workflows.get(&workflow_id)?.workflow;
        Some(persisted.channel_ids())
    }

    // TODO: clearly inefficient
    fn nearest_timer_expiration(&self) -> Option<DateTime<Utc>> {
        let timers = self
            .workflows
            .values()
            .flat_map(|persisted| persisted.workflow.timers());
        let expirations = timers.filter_map(|(_, state)| {
            if state.completed_at().is_none() {
                Some(state.definition().expires_at)
            } else {
                None
            }
        });
        expirations.min()
    }

    fn find_workflow_with_pending_tasks(&self) -> Option<WorkflowId> {
        self.workflows.iter().find_map(|(&workflow_id, persisted)| {
            let workflow = &persisted.workflow;
            if !workflow.is_initialized() || workflow.pending_events().next().is_some() {
                Some(workflow_id)
            } else {
                None
            }
        })
    }

    fn find_consumable_channel(&self) -> Option<(ChannelId, WorkflowId)> {
        let mut all_channels = self.workflows.iter().flat_map(|(&workflow_id, persisted)| {
            persisted
                .workflow
                .inbound_channels()
                .map(move |(_, _, state)| (workflow_id, state))
        });
        all_channels.find_map(|(workflow_id, state)| {
            if state.waits_for_message() {
                let channel_id = state.id();
                if self.channels[&channel_id].can_provide_message() {
                    return Some((channel_id, workflow_id));
                }
            }
            None
        })
    }

    fn commit(
        &mut self,
        executed_workflow_id: Option<WorkflowId>,
        child_workflows: HashMap<WorkflowId, WorkflowWithMeta>,
        receipt: &Receipt,
    ) {
        // Create new channels and write outbound messages for them when appropriate.
        for (child_id, child_workflow) in child_workflows {
            let channel_ids = child_workflow.workflow.channel_ids();
            for &channel_id in channel_ids.inbound.values() {
                self.channels
                    .entry(channel_id)
                    .or_insert_with(|| ChannelState::new(Some(child_id)));
            }
            for &channel_id in channel_ids.outbound.values() {
                self.channels
                    .entry(channel_id)
                    .or_insert_with(|| ChannelState::new(executed_workflow_id));
            }

            self.workflows.insert(child_id, child_workflow);
        }

        for channel_id in receipt.closed_channel_ids() {
            self.channels.get_mut(&channel_id).unwrap().close();
        }
    }

    fn drain_and_persist_workflow(
        &mut self,
        id: WorkflowId,
        definition_id: Option<String>,
        workflow: &mut Workflow<()>,
    ) {
        // Drain outbound messages generated by the executed workflow. At this point,
        // new channels are already created.
        for (channel_id, messages) in workflow.drain_messages() {
            let channel = self.channels.get_mut(&channel_id).unwrap();
            channel.push_messages(messages).ok(); // we're ok with messages getting dropped
        }

        // Since all outbound messages are drained, persisting the workflow is safe.
        let workflow = workflow.persist().unwrap();
        if let Some(persisted) = self.workflows.get_mut(&id) {
            persisted.workflow = workflow;
        } else {
            self.workflows.insert(
                id,
                WorkflowWithMeta {
                    definition_id: definition_id.expect("no `definition_id` provided"),
                    workflow,
                },
            );
        }
    }
}

#[derive(Debug, Default)]
struct WorkflowManagerState {
    committed: PersistedWorkflows,
    // Fields related to the current workflow execution.
    executing_workflow_id: Option<WorkflowId>,
    /// Successfully initialized workflows spawned by the current workflow execution.
    new_workflows: HashMap<WorkflowId, WorkflowWithMeta>,
}

impl WorkflowManagerState {
    fn stash_workflow(&mut self, definition_id: String, workflow: &mut Workflow<()>) -> WorkflowId {
        debug_assert!(!workflow.is_initialized());

        let id = self.committed.next_workflow_id;
        self.committed.next_workflow_id += 1;
        self.new_workflows.insert(
            id,
            WorkflowWithMeta {
                definition_id,
                workflow: workflow.persist().unwrap(),
            },
        );
        id
    }

    fn commit(&mut self, receipt: &Receipt) {
        let executed_workflow_id = self.executing_workflow_id.take();
        let child_workflows = mem::take(&mut self.new_workflows);
        self.committed
            .commit(executed_workflow_id, child_workflows, receipt);
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

pub trait WorkflowBuilderExt {
    type Output;

    /// # Errors
    ///
    /// Returns an error if the workflow cannot be spawned.
    fn build_and_commit(self) -> Result<Self::Output, SpawnError>;
}

impl<'a, W> WorkflowBuilderExt for WorkflowBuilder<'a, WorkflowManager, W>
where
    W: TakeHandle<Spawner>,
{
    type Output = WorkflowEnv<'a, W>;

    fn build_and_commit(self) -> Result<Self::Output, SpawnError> {
        let manager = self.manager();
        self.build_into_handle().map(|ids| {
            manager.state.lock().unwrap().commit(&Receipt::new());
            let workflow = manager.workflow(ids.workflow_id).unwrap();
            workflow.downcast_unchecked()
        })
    }
}
