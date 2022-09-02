//! `WorkflowManager` and tightly related types.

#![allow(missing_docs)]

use chrono::Utc;

use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    mem,
    sync::{Arc, Mutex, Weak},
};

#[cfg(test)]
mod tests;

use crate::{
    receipt::{ExecutionError, Receipt},
    services::{Clock, Services, WorkflowAndChannelIds},
    utils::Message,
    workflow::ChannelIds,
    PersistedWorkflow, Workflow, WorkflowSpawner,
};
use tardigrade::{
    interface::Interface,
    spawn::{ChannelHandles, ChannelSpawnConfig, ManageWorkflows},
    workflow::WorkflowFn,
};
use tardigrade_shared::{ChannelId, SpawnError, WorkflowId};

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

// FIXME: add public API
//   - creating workflow
//   - initializing workflow
//   - sending message
//   - feeding message to a workflow (via queue?)
impl WorkflowManager {
    const CLOSED_CHANNEL: ChannelId = 0;

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

    fn send_message(&self, channel_id: ChannelId, message: Vec<u8>) {
        self.state
            .lock()
            .unwrap()
            .send_message(channel_id, message.into());
    }

    /// Atomically pops a message from a channel and feeds it to a listening workflow.
    /// If the workflow execution is successful, its results are committed to the manager state;
    /// otherwise, the results are reverted.
    ///
    /// # Preconditions
    ///
    /// - The channel must contain a message
    /// - The channel must be listened to by a workflow
    fn feed_message_to_workflow(&self, channel_id: ChannelId) -> Result<Receipt, ExecutionError> {
        let mut state = self.state.lock().unwrap();
        let (workflow_id, message) = state.take_message(channel_id);
        let workflow_id = workflow_id.unwrap();
        let mut workflow = self.restore_workflow(&state, workflow_id);

        state.executing_workflow_id = Some(workflow_id);
        let result = state.push_message(&mut workflow, message.clone(), channel_id);
        if result.is_ok() {
            state.commit();
            state.drain_and_persist_workflow(workflow_id, None, &mut workflow);
        } else {
            // Do not commit the execution result. Instead, put the message back to the channel.
            state.revert_taking_message(channel_id, message);
        }
        state.executing_workflow_id = None;
        result
    }

    // FIXME: sane error handling
    fn restore_workflow(&self, state: &WorkflowManagerState, id: WorkflowId) -> Workflow<()> {
        let persisted = state
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

    fn initialize_workflow(&self, workflow_id: WorkflowId) -> Result<Receipt, ExecutionError> {
        let mut state = self.state.lock().unwrap();
        let mut workflow = self.restore_workflow(&state, workflow_id);
        assert!(
            !workflow.is_initialized(),
            "workflow with ID `{}` is already initialized",
            workflow_id
        );

        state.executing_workflow_id = Some(workflow_id);
        let result = workflow.initialize();
        if result.is_ok() {
            state.commit();
            state.drain_and_persist_workflow(workflow_id, None, &mut workflow);
        }
        state.executing_workflow_id = None;
        result
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

        let channel_ids = self.state.lock().unwrap().allocate_channels(handles);
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

#[derive(Debug)]
struct ChannelState {
    receiver_workflow_id: Option<WorkflowId>, // `None` means external receiver
    messages: VecDeque<Message>,
}

impl ChannelState {
    fn new(receiver_workflow_id: Option<WorkflowId>) -> Self {
        Self {
            receiver_workflow_id,
            messages: VecDeque::new(),
        }
    }

    fn push_messages(&mut self, messages: impl IntoIterator<Item = Message>) {
        self.messages.extend(messages);
    }

    fn pop_message(&mut self) -> Option<Message> {
        self.messages.pop_front()
    }
}

/// Wrapper for either ongoing or persisted workflow.
#[derive(Debug)]
struct WithMeta<W> {
    definition_id: String,
    workflow: W,
}

#[derive(Debug)]
struct WorkflowManagerState {
    workflows: HashMap<WorkflowId, WithMeta<PersistedWorkflow>>,
    next_workflow_id: WorkflowId,
    channels: HashMap<ChannelId, ChannelState>,
    next_channel_id: ChannelId,

    // Fields related to the current workflow execution.
    // FIXME: transaction API?
    executing_workflow_id: Option<WorkflowId>,
    /// Successfully initialized workflows spawned by the current workflow execution.
    new_workflows: HashMap<WorkflowId, WithMeta<Workflow<()>>>,
}

impl Default for WorkflowManagerState {
    fn default() -> Self {
        Self {
            workflows: HashMap::new(),
            next_workflow_id: 0,
            channels: HashMap::new(),
            next_channel_id: 1, // avoid "pre-closed" channel ID
            executing_workflow_id: None,
            new_workflows: HashMap::new(),
        }
    }
}

impl WorkflowManagerState {
    fn allocate_channels(&mut self, handles: &ChannelHandles) -> ChannelIds {
        ChannelIds {
            inbound: self.map_channel_configs(&handles.inbound),
            outbound: self.map_channel_configs(&handles.outbound),
        }
    }

    fn map_channel_configs(
        &mut self,
        configs: &HashMap<String, ChannelSpawnConfig>,
    ) -> HashMap<String, ChannelId> {
        configs
            .iter()
            .map(|(name, config)| {
                let channel_id = match config {
                    ChannelSpawnConfig::New => self.allocate_channel_id(),
                    ChannelSpawnConfig::Closed => WorkflowManager::CLOSED_CHANNEL,
                    _ => panic!("Channel spawn config {:?} is not supported", config),
                };
                (name.clone(), channel_id)
            })
            .collect()
    }

    fn allocate_channel_id(&mut self) -> ChannelId {
        let channel_id = self.next_channel_id;
        self.next_channel_id += 1;
        channel_id
    }

    fn take_message(&mut self, channel_id: ChannelId) -> (Option<WorkflowId>, Message) {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        (
            channel_state.receiver_workflow_id,
            channel_state.pop_message().unwrap(),
        )
    }

    fn revert_taking_message(&mut self, channel_id: ChannelId, message: Message) {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.messages.push_front(message);
    }

    fn send_message(&mut self, channel_id: ChannelId, message: Message) {
        let channel_state = self.channels.get_mut(&channel_id).unwrap();
        channel_state.push_messages([message]);
    }

    /// # Preconditions
    ///
    /// - The workflow is ready to receive a message over `channel_id`
    fn push_message(
        &mut self,
        workflow: &mut Workflow<()>,
        message: Message,
        channel_id: ChannelId,
    ) -> Result<Receipt, ExecutionError> {
        let (child_id, channel_name) = workflow.inbound_channel_by_id(channel_id).unwrap();
        let channel_name = channel_name.to_owned();

        workflow.rollback_on_error(|workflow| {
            workflow
                .push_inbound_message(child_id, &channel_name, message.into())
                .unwrap();
            workflow.tick()
        })
    }

    fn stash_workflow(&mut self, definition_id: String, workflow: Workflow<()>) -> WorkflowId {
        debug_assert!(!workflow.is_initialized());

        let id = self.next_workflow_id;
        self.next_workflow_id += 1;
        self.new_workflows.insert(
            id,
            WithMeta {
                definition_id,
                workflow,
            },
        );
        id
    }

    fn commit(&mut self) {
        let executed_workflow_id = self.executing_workflow_id.take();

        // Create new channels and write outbound messages for them when appropriate.
        for (child_id, mut child_workflow) in mem::take(&mut self.new_workflows) {
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
            channel.push_messages(messages);
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
