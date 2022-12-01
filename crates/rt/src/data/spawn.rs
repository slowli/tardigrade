//! Spawning workflows.

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt, mem,
    task::Poll,
};

use crate::{
    data::{
        channel::{
            acquire_non_captured_channels, new_channel_mapping, ChannelMapping, ChannelStates,
            Channels,
        },
        helpers::{WakerPlacement, WorkflowPoll},
        PersistedWorkflowData, WorkflowData,
    },
    receipt::{ResourceEventKind, ResourceId, WakeUpCause},
    utils,
    workflow::ChannelIds,
};
use tardigrade::{
    interface::{Handle, HandleMapKey, Interface, ReceiverAt, SenderAt},
    spawn::{ChannelsConfig, HostError},
    task::JoinError,
    ChannelId, WakerId, WorkflowId,
};

type StubResult = Result<WorkflowId, HostError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChildWorkflowStub {
    #[serde(with = "utils::serde_poll_res")]
    result: Poll<Result<WorkflowId, HostError>>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    wakes_on_init: HashSet<WakerId>,
}

impl Default for ChildWorkflowStub {
    fn default() -> Self {
        Self {
            result: Poll::Pending,
            wakes_on_init: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct ChildWorkflowStubs {
    stubs: HashMap<WorkflowId, ChildWorkflowStub>,
    next_stub_id: WorkflowId,
}

impl ChildWorkflowStubs {
    fn create(&mut self) -> WorkflowId {
        let stub_id = self.next_stub_id;
        self.stubs.insert(stub_id, ChildWorkflowStub::default());
        self.next_stub_id += 1;
        stub_id
    }

    pub fn insert_waker(&mut self, stub_id: WorkflowId, waker: WakerId) {
        let stub = self.stubs.get_mut(&stub_id).unwrap();
        stub.wakes_on_init.insert(waker);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ChildWorkflowState {
    pub channels: ChannelMapping,
    #[serde(with = "utils::serde_poll_res")]
    completion_result: Poll<Result<(), JoinError>>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    wakes_on_completion: HashSet<WakerId>,
}

impl Clone for ChildWorkflowState {
    fn clone(&self) -> Self {
        Self {
            channels: self.channels.clone(),
            completion_result: utils::clone_completion_result(&self.completion_result),
            wakes_on_completion: self.wakes_on_completion.clone(),
        }
    }
}

impl ChildWorkflowState {
    fn new(channel_ids: ChannelIds) -> Self {
        Self {
            channels: new_channel_mapping(channel_ids),
            completion_result: Poll::Pending,
            wakes_on_completion: HashSet::new(),
        }
    }

    fn result(&self) -> Poll<Result<(), &JoinError>> {
        match &self.completion_result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => Poll::Ready(res.as_ref().copied()),
        }
    }

    pub(super) fn insert_waker(&mut self, waker_id: WakerId) {
        self.wakes_on_completion.insert(waker_id);
    }
}

/// State of child workflow as viewed by its parent.
#[derive(Debug, Clone, Copy)]
pub struct ChildWorkflow<'a> {
    state: &'a ChildWorkflowState,
    channels: Channels<'a>,
}

impl<'a> ChildWorkflow<'a> {
    fn new(state: &'a ChildWorkflowState, channel_states: &'a ChannelStates) -> Self {
        Self {
            state,
            channels: Channels::new(channel_states, &state.channels),
        }
    }

    /// Returns *local* channels connecting to this child workflow.
    pub fn channels(&self) -> Channels<'a> {
        self.channels
    }

    /// Returns the current poll state of this workflow.
    pub fn result(&self) -> Poll<Result<(), &'a JoinError>> {
        self.state.result()
    }
}

impl PersistedWorkflowData {
    pub fn contains_child_stub(&self, id: WorkflowId) -> bool {
        self.child_workflow_stubs.stubs.contains_key(&id)
    }

    pub fn child_workflows(&self) -> impl Iterator<Item = (WorkflowId, ChildWorkflow<'_>)> + '_ {
        self.child_workflows.iter().map(|(id, state)| {
            let child = ChildWorkflow::new(state, &self.channels);
            (*id, child)
        })
    }

    pub fn child_workflow(&self, id: WorkflowId) -> Option<ChildWorkflow<'_>> {
        let state = self.child_workflows.get(&id)?;
        Some(ChildWorkflow::new(state, &self.channels))
    }

    pub fn notify_on_child_init(
        &mut self,
        stub_id: WorkflowId,
        workflow_id: WorkflowId,
        config: &ChannelsConfig<ChannelId>,
        mut channel_ids: ChannelIds,
    ) {
        let stub = self.child_workflow_stubs.stubs.get_mut(&stub_id).unwrap();
        stub.result = Poll::Ready(Ok(workflow_id));
        let wakers = mem::take(&mut stub.wakes_on_init);
        self.schedule_wakers(wakers, WakeUpCause::InitWorkflow { stub_id });
        for spec in channel_ids.values_mut() {
            *spec = match spec {
                Handle::Sender(id) => Handle::Receiver(*id),
                Handle::Receiver(id) => Handle::Sender(*id),
            };
        }

        // TODO: what is the appropriate capacity?
        self.channels.insert_channels(&channel_ids, |_| Some(1));
        let mut child_state = ChildWorkflowState::new(channel_ids);
        acquire_non_captured_channels(&mut child_state.channels, config);
        self.child_workflows.insert(workflow_id, child_state);
    }

    pub fn notify_on_child_spawn_error(&mut self, stub_id: WorkflowId, err: HostError) {
        let stub = self.child_workflow_stubs.stubs.get_mut(&stub_id).unwrap();
        stub.result = Poll::Ready(Err(err));
        let wakers = mem::take(&mut stub.wakes_on_init);
        self.schedule_wakers(wakers, WakeUpCause::InitWorkflow { stub_id });
    }

    pub fn notify_on_child_completion(&mut self, id: WorkflowId, result: Result<(), JoinError>) {
        let state = self.child_workflows.get_mut(&id).unwrap();
        debug_assert!(state.completion_result.is_pending());
        state.completion_result = Poll::Ready(result);
        let wakers = mem::take(&mut state.wakes_on_completion);
        self.schedule_wakers(wakers, WakeUpCause::CompletedWorkflow(id));
    }
}

/// Handle allowing to manipulate a child stub.
pub struct ChildStubActions<'a> {
    data: &'a mut WorkflowData,
    id: WorkflowId,
}

impl fmt::Debug for ChildStubActions<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildStubActions")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl ChildStubActions<'_> {
    /// Polls this stub for initialization.
    #[tracing::instrument(level = "debug", ret)]
    pub fn poll_init(&mut self) -> WorkflowPoll<StubResult> {
        let stubs = &mut self.data.persisted.child_workflow_stubs.stubs;
        let stub = &stubs[&self.id];
        let poll_result = stub.result.clone();
        if poll_result.is_ready() {
            // We don't want to create aliased workflow resources; thus, we prevent repeated
            // stub polling after completion.
            stubs.remove(&self.id);
        }

        self.data.current_execution().push_resource_event(
            ResourceId::WorkflowStub(self.id),
            ResourceEventKind::Polled(utils::drop_value(&poll_result)),
        );
        if let Poll::Ready(Ok(workflow_id)) = &poll_result {
            self.data.current_execution().push_resource_event(
                ResourceId::Workflow(*workflow_id),
                ResourceEventKind::Created,
            );
        }
        WorkflowPoll::new(poll_result, WakerPlacement::WorkflowInit(self.id))
    }

    /// Drops this stub. Returns IDs of the wakers that were created polling the stub
    /// and should be dropped.
    #[must_use = "Returned wakers must be dropped"]
    #[tracing::instrument(level = "debug")]
    pub fn drop(self) -> HashSet<WakerId> {
        self.data.current_execution().push_resource_event(
            ResourceId::WorkflowStub(self.id),
            ResourceEventKind::Dropped,
        );
        let stubs = &mut self.data.persisted.child_workflow_stubs.stubs;
        stubs
            .get_mut(&self.id)
            .map_or_else(HashSet::default, |stub| mem::take(&mut stub.wakes_on_init))
    }
}

/// Handle allowing to manipulate an (initialized) child workflow.
pub struct ChildActions<'a> {
    data: &'a mut WorkflowData,
    id: WorkflowId,
}

impl fmt::Debug for ChildActions<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildActions")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl ChildActions<'_> {
    /// Polls this child for completion.
    #[tracing::instrument(level = "debug", ret)]
    pub fn poll_completion(&mut self) -> WorkflowPoll<Result<(), JoinError>> {
        let poll_result = self.data.persisted.child_workflows[&self.id]
            .result()
            .map_err(utils::clone_join_error);
        self.data.current_execution().push_resource_event(
            ResourceId::Workflow(self.id),
            ResourceEventKind::Polled(utils::drop_value(&poll_result)),
        );
        WorkflowPoll::new(poll_result, WakerPlacement::WorkflowCompletion(self.id))
    }

    /// Drops this child. Returns IDs of the wakers that were created polling the child
    /// and should be dropped.
    #[must_use = "Returned wakers must be dropped"]
    #[tracing::instrument(level = "debug")]
    pub fn drop(self) -> HashSet<WakerId> {
        self.data
            .current_execution()
            .push_resource_event(ResourceId::Workflow(self.id), ResourceEventKind::Dropped);
        let children = &mut self.data.persisted.child_workflows;
        let state = children.get_mut(&self.id).unwrap();
        mem::take(&mut state.wakes_on_completion)
    }
}

/// Spawning-related functionality.
impl WorkflowData {
    /// Returns the interface matching the specified definition ID.
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn workflow_interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        let workflows = self.services().workflows.as_deref();
        let interface = workflows.and_then(|workflows| workflows.interface(definition_id));
        tracing::debug!(ret.is_some = interface.is_some());
        interface
    }

    fn validate_handles(
        &self,
        definition_id: &str,
        channels: &ChannelsConfig<ChannelId>,
    ) -> anyhow::Result<()> {
        let workflows = self.services().workflows.as_deref();
        let interface = workflows.and_then(|workflows| workflows.interface(definition_id));
        if let Some(interface) = interface {
            for (path, spec) in interface.handles() {
                match spec {
                    Handle::Receiver(_) => ReceiverAt(path).get(channels)?,
                    Handle::Sender(_) => SenderAt(path).get(channels)?,
                };
            }

            if channels.len() != interface.handles().len() {
                let err = Self::extra_handles_error(&interface, channels);
                return Err(err);
            }
            Ok(())
        } else {
            Err(anyhow!("workflow with ID `{definition_id}` is not defined"))
        }
    }

    fn extra_handles_error(
        interface: &Interface,
        channels: &ChannelsConfig<ChannelId>,
    ) -> anyhow::Error {
        use std::fmt::Write as _;

        let mut extra_handles = channels
            .keys()
            .filter(|path| interface.handle(path.as_ref()).is_err())
            .fold(String::new(), |mut acc, name| {
                write!(acc, "`{}`, ", name).unwrap();
                acc
            });
        debug_assert!(!extra_handles.is_empty());
        extra_handles.truncate(extra_handles.len() - 2); // remove trailing ", "
        anyhow!("extra handles: {extra_handles}")
    }

    /// Creates a workflow stub with the specified parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if the provided `channels` config is not valid (e.g., doesn't contain
    /// precisely the same channels as specified in the spawned workflow interface).
    #[tracing::instrument(skip(args), ret, err, fields(args.len = args.len()))]
    pub fn create_workflow_stub(
        &mut self,
        definition_id: &str,
        args: Vec<u8>,
        channels: &ChannelsConfig<ChannelId>,
    ) -> anyhow::Result<WorkflowId> {
        self.validate_handles(definition_id, channels)?;

        let stub_id = self.persisted.child_workflow_stubs.create();
        let workflows = self
            .services_mut()
            .workflows
            .as_deref_mut()
            .ok_or_else(|| anyhow!("no capability to spawn workflows"))?;
        workflows.stash_workflow(stub_id, definition_id, args, channels.clone());

        self.current_execution().push_resource_event(
            ResourceId::WorkflowStub(stub_id),
            ResourceEventKind::Created,
        );
        Ok(stub_id)
    }

    /// Returns an action handle for the child stub with the specified ID.
    ///
    /// # Panics
    ///
    /// Panics if the stub with `id` does not exist in the workflow. This includes the case
    /// when the stub was initialized.
    pub fn child_stub(&mut self, id: WorkflowId) -> ChildStubActions<'_> {
        assert!(
            self.persisted.child_workflow_stubs.stubs.contains_key(&id),
            "stub not found"
        );
        ChildStubActions { data: self, id }
    }

    /// Returns an action handle for the child workflow with the specified ID.
    ///
    /// # Panics
    ///
    /// Panics if the child with `id` does not exist in the workflow.
    pub fn child(&mut self, id: WorkflowId) -> ChildActions<'_> {
        assert!(
            self.persisted.child_workflows.contains_key(&id),
            "child not found"
        );
        ChildActions { data: self, id }
    }
}
