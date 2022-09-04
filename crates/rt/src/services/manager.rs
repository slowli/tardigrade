//! `WorkflowManager` and tightly related types.

#![allow(missing_docs)] // FIXME
#![allow(clippy::missing_panics_doc)] // lots of `unwrap()`s on mutex locks

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    mem,
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

    fn services(&self) -> Services {
        Services {
            clock: Arc::clone(&self.clock),
            workflows: self.arc.upgrade().unwrap(),
            // ^ `unwrap()` should be safe provided this method is called from `Arc<Self>`
        }
    }

    pub(crate) fn channel_info(&self, channel_id: ChannelId) -> ChannelInfo {
        let state = self.state.lock().unwrap();
        state.committed.channels[&channel_id].info()
    }

    /// Returns a handle to a workflow with the specified ID.
    pub fn workflow(&self, workflow_id: WorkflowId) -> Option<WorkflowEnv<'_, ()>> {
        let state = self.state.lock().unwrap();
        state.committed.channel_ids(workflow_id).map(|channel_ids| {
            WorkflowEnv::new(
                self,
                WorkflowAndChannelIds {
                    workflow_id,
                    channel_ids,
                },
            )
        })
    }

    pub(crate) fn persisted_workflow(&self, workflow_id: WorkflowId) -> PersistedWorkflow {
        let state = self.state.lock().unwrap();
        state.committed.workflows[&workflow_id].workflow.clone()
    }

    pub(crate) fn interface_for_workflow(&self, workflow_id: WorkflowId) -> Option<&Interface<()>> {
        let state = self.state.lock().unwrap();
        let persisted = state.committed.workflows.get(&workflow_id)?;
        Some(self.spawners[&persisted.definition_id].interface())
    }

    pub(crate) fn send_message(
        &self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), SendError> {
        let mut state = self.state.lock().unwrap();
        state.committed.send_message(channel_id, message.into())
    }

    pub(crate) fn close_channel_sender(&self, channel_id: ChannelId) {
        self.state
            .lock()
            .unwrap()
            .committed
            .close_channel_sender(channel_id);
    }

    pub(crate) fn take_outbound_messages(&self, channel_id: ChannelId) -> (usize, Vec<Message>) {
        let mut state = self.state.lock().unwrap();
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
    pub(crate) fn feed_message_to_workflow(
        &self,
        channel_id: ChannelId,
    ) -> Result<Option<Receipt>, ExecutionError> {
        let mut state = self.state.lock().unwrap();
        let (workflow_id, message) = state.committed.take_message(channel_id);
        let workflow_id = workflow_id.expect("channel receiver not connected to workflow");
        let message = if let Some(message) = message {
            message
        } else {
            return Ok(None);
        };
        let mut workflow = self.restore_workflow(&state.committed, workflow_id);

        state.executing_workflow_id = Some(workflow_id);
        let result = Self::push_message(&mut workflow, message.clone(), channel_id);
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
            state.committed.revert_taking_message(channel_id, message);
        }
        state.executing_workflow_id = None;
        result
    }

    /// Returns `Ok(None)` if the message cannot be consumed right now (the workflow channel
    /// does not listen to it).
    fn push_message(
        workflow: &mut Workflow<()>,
        message: Message,
        channel_id: ChannelId,
    ) -> Result<Option<Receipt>, ExecutionError> {
        let (child_id, channel_name) = workflow.inbound_channel_by_id(channel_id).unwrap();
        let channel_name = channel_name.to_owned();

        workflow.rollback_on_error(|workflow| {
            let push_result =
                workflow.push_inbound_message(child_id, &channel_name, message.into());
            if push_result.is_err() {
                Ok(None)
            } else {
                workflow.tick().map(Some)
            }
        })
    }

    // FIXME: clearly not efficient
    pub fn set_current_time(&self, time: DateTime<Utc>) {
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        persist_fn(&self.state.lock().unwrap().committed)
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
        let workflow = spawner
            .spawn(args, &channel_ids, self.services())
            .map_err(|err| SpawnError::new(err.to_string()))?;
        let workflow_id = self
            .state
            .lock()
            .unwrap()
            .stash_workflow(id.to_owned(), workflow);
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
    pub fn with_clock(mut self, clock: impl Clock) -> Self {
        self.manager.clock = Arc::new(clock);
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
struct WithMeta<W> {
    definition_id: String,
    workflow: W,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedWorkflows {
    workflows: HashMap<WorkflowId, WithMeta<PersistedWorkflow>>,
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

    fn take_message(&mut self, channel_id: ChannelId) -> (Option<WorkflowId>, Option<Message>) {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        (
            channel_state.receiver_workflow_id,
            channel_state.pop_message(),
        )
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

    fn commit(
        &mut self,
        executed_workflow_id: Option<WorkflowId>,
        child_workflows: HashMap<WorkflowId, WithMeta<Workflow<()>>>,
        receipt: &Receipt,
    ) {
        // Create new channels and write outbound messages for them when appropriate.
        for (child_id, mut child_workflow) in child_workflows {
            for channel_id in child_workflow.workflow.inbound_channel_ids() {
                self.channels
                    .entry(channel_id)
                    .or_insert_with(|| ChannelState::new(Some(child_id)));
            }
            for channel_id in child_workflow.workflow.outbound_channel_ids() {
                self.channels
                    .entry(channel_id)
                    .or_insert_with(|| ChannelState::new(executed_workflow_id));
            }

            self.drain_and_persist_workflow(
                child_id,
                Some(child_workflow.definition_id),
                &mut child_workflow.workflow,
            );
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
                WithMeta {
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
    // FIXME: transaction API?
    executing_workflow_id: Option<WorkflowId>,
    /// Successfully initialized workflows spawned by the current workflow execution.
    new_workflows: HashMap<WorkflowId, WithMeta<Workflow<()>>>,
}

impl WorkflowManagerState {
    fn stash_workflow(&mut self, definition_id: String, workflow: Workflow<()>) -> WorkflowId {
        debug_assert!(!workflow.is_initialized());

        let id = self.committed.next_workflow_id;
        self.committed.next_workflow_id += 1;
        self.new_workflows.insert(
            id,
            WithMeta {
                definition_id,
                workflow,
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
