//! Spawning workflows.

use anyhow::anyhow;
use futures::future::Aborted;
use serde::{Deserialize, Serialize};

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    mem,
    task::Poll,
};

use crate::{
    data::{
        channel::{ChannelMapping, ChannelStates, Channels},
        helpers::{WakerPlacement, WorkflowPoll},
        PersistedWorkflowData, WorkflowData,
    },
    receipt::{ResourceEventKind, ResourceId, WakeUpCause},
    utils,
    workflow::ChannelIds,
};
use tardigrade::{
    interface::{ChannelHalf, Interface},
    spawn::{ChannelsConfig, HostError},
    task::{JoinError, TaskError},
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
            channels: ChannelMapping::new(channel_ids),
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
        mem::swap(&mut channel_ids.receivers, &mut channel_ids.senders);

        // TODO: what is the appropriate capacity?
        self.channels.insert_channels(&channel_ids, |_| Some(1));
        let mut child_state = ChildWorkflowState::new(channel_ids);
        child_state.channels.acquire_non_captured_channels(config);
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

impl WorkflowData {
    fn validate_handles(
        &self,
        definition_id: &str,
        channels: &ChannelsConfig<ChannelId>,
    ) -> anyhow::Result<()> {
        let workflows = self.services().workflows.as_deref();
        let interface = workflows.and_then(|workflows| workflows.interface(definition_id));
        if let Some(interface) = interface {
            for (name, _) in interface.receivers() {
                if !channels.receivers.contains_key(name) {
                    let err = anyhow!("missing handle for channel receiver `{name}`");
                    return Err(err);
                }
            }
            for (name, _) in interface.senders() {
                if !channels.senders.contains_key(name) {
                    let err = anyhow!("missing handle for channel sender `{name}`");
                    return Err(err);
                }
            }

            if channels.receivers.len() != interface.receivers().len() {
                let err = Self::extra_handles_error(&interface, channels, ChannelHalf::Receiver);
                return Err(err);
            }
            if channels.senders.len() != interface.senders().len() {
                let err = Self::extra_handles_error(&interface, channels, ChannelHalf::Sender);
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
        channel_kind: ChannelHalf,
    ) -> anyhow::Error {
        use std::fmt::Write as _;

        let (closure_in, closure_out);
        let (handle_keys, channel_filter) = match channel_kind {
            ChannelHalf::Receiver => {
                closure_in = |name| interface.receiver(name).is_none();
                (channels.receivers.keys(), &closure_in as &dyn Fn(_) -> _)
            }
            ChannelHalf::Sender => {
                closure_out = |name| interface.sender(name).is_none();
                (channels.senders.keys(), &closure_out as &dyn Fn(_) -> _)
            }
        };

        let mut extra_handles = handle_keys
            .filter(|name| channel_filter(name.as_str()))
            .fold(String::new(), |mut acc, name| {
                write!(acc, "`{}`, ", name).unwrap();
                acc
            });
        debug_assert!(!extra_handles.is_empty());
        extra_handles.truncate(extra_handles.len() - 2);
        anyhow!("extra {channel_kind} handles: {extra_handles}")
    }

    /// Creates a workflow stub with the specified parameters.
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

    /// Polls workflow stub initialization.
    #[tracing::instrument(level = "debug", ret, err, skip(self))]
    pub fn poll_workflow_init(
        &mut self,
        stub_id: WorkflowId,
    ) -> anyhow::Result<WorkflowPoll<StubResult>> {
        let stubs = &mut self.persisted.child_workflow_stubs.stubs;
        let stub = stubs
            .get(&stub_id)
            .ok_or_else(|| anyhow!("stub {stub_id} polled after completion"))?;
        let poll_result = stub.result.clone();
        if poll_result.is_ready() {
            // We don't want to create aliased workflow resources; thus, we prevent repeated
            // stub polling after completion.
            stubs.remove(&stub_id);
        }

        self.current_execution().push_resource_event(
            ResourceId::WorkflowStub(stub_id),
            ResourceEventKind::Polled(utils::drop_value(&poll_result)),
        );
        if let Poll::Ready(Ok(workflow_id)) = &poll_result {
            self.current_execution().push_resource_event(
                ResourceId::Workflow(*workflow_id),
                ResourceEventKind::Created,
            );
        }
        Ok(WorkflowPoll::new(
            poll_result,
            WakerPlacement::WorkflowInit(stub_id),
        ))
    }

    /// Polls workflow completion.
    #[tracing::instrument(level = "debug", ret, skip(self))]
    pub fn poll_workflow_completion(
        &mut self,
        workflow_id: WorkflowId,
    ) -> WorkflowPoll<Result<(), Aborted>> {
        let poll_result = self.persisted.child_workflows[&workflow_id]
            .result()
            .map(utils::extract_task_poll_result);
        self.current_execution().push_resource_event(
            ResourceId::Workflow(workflow_id),
            ResourceEventKind::Polled(utils::drop_value(&poll_result)),
        );
        WorkflowPoll::new(poll_result, WakerPlacement::WorkflowCompletion(workflow_id))
    }

    /// Returns task error for the specified child.
    pub fn workflow_task_error(&self, child_id: WorkflowId) -> Option<&TaskError> {
        let result = &self.persisted.child_workflows[&child_id].completion_result;
        if let Poll::Ready(Err(JoinError::Err(err))) = result {
            Some(err)
        } else {
            None
        }
    }

    /// Drops the specified child workflow stub.
    #[must_use = "Returned wakers must be dropped"]
    pub fn drop_child_stub(&mut self, stub_id: WorkflowId) -> HashSet<WakerId> {
        self.current_execution().push_resource_event(
            ResourceId::WorkflowStub(stub_id),
            ResourceEventKind::Dropped,
        );
        let stubs = &mut self.persisted.child_workflow_stubs.stubs;
        stubs
            .get_mut(&stub_id)
            .map_or_else(HashSet::default, |stub| mem::take(&mut stub.wakes_on_init))
    }

    /// Handles dropping the child workflow handle from the workflow side. Returns wakers
    /// that should be dropped.
    ///
    /// # Panics
    ///
    /// Panics if the child workflow with the specified ID is not defined.
    #[must_use = "Returned wakers must be dropped"]
    pub fn drop_child(&mut self, workflow_id: WorkflowId) -> HashSet<WakerId> {
        self.current_execution().push_resource_event(
            ResourceId::Workflow(workflow_id),
            ResourceEventKind::Dropped,
        );
        let state = self
            .persisted
            .child_workflows
            .get_mut(&workflow_id)
            .unwrap();
        mem::take(&mut state.wakes_on_completion)
    }

    /// Returns the interface matching the specified definition ID.
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn workflow_interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        let workflows = self.services().workflows.as_deref();
        let interface = workflows.and_then(|workflows| workflows.interface(definition_id));
        tracing::debug!(ret.is_some = interface.is_some());
        interface
    }
}
