//! Spawning workflows.

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use std::{borrow::Cow, collections::HashSet, fmt, mem, task::Poll};

use crate::workflow::WorkflowAndChannelIds;
use crate::{
    data::{
        channel::{ChannelStates, Channels},
        helpers::{WakerPlacement, WorkflowPoll},
        PersistedWorkflowData, WorkflowData,
    },
    receipt::{ResourceEventKind, ResourceId, StubEventKind, StubId, WakeUpCause},
    utils,
    workflow::ChannelIds,
};
use tardigrade::spawn::HostError;
use tardigrade::{
    handle::{Handle, HandleMapKey, ReceiverAt, SenderAt},
    interface::Interface,
    task::JoinError,
    WakerId, WorkflowId,
};

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ChildWorkflowState {
    pub channels: ChannelIds,
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
            channels: channel_ids,
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

    pub fn notify_on_child_completion(&mut self, id: WorkflowId, result: Result<(), JoinError>) {
        let state = self.child_workflows.get_mut(&id).unwrap();
        debug_assert!(state.completion_result.is_pending());
        state.completion_result = Poll::Ready(result);
        let wakers = mem::take(&mut state.wakes_on_completion);
        self.schedule_wakers(wakers, WakeUpCause::CompletedWorkflow(id));
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
        let workflows = self.services().stubs.as_deref();
        let interface = workflows.and_then(|workflows| workflows.interface(definition_id));
        tracing::debug!(ret.is_some = interface.is_some());
        interface
    }

    fn validate_handles(&self, definition_id: &str, channels: &ChannelIds) -> anyhow::Result<()> {
        let workflows = self.services().stubs.as_deref();
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

    fn extra_handles_error(interface: &Interface, channel_ids: &ChannelIds) -> anyhow::Error {
        use std::fmt::Write as _;

        let mut extra_handles = channel_ids
            .keys()
            .filter(|path| interface.handle(path.as_ref()).is_err())
            .fold(String::new(), |mut acc, path| {
                write!(acc, "`{path}`, ").unwrap();
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
        stub_id: WorkflowId,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelIds,
    ) -> anyhow::Result<()> {
        self.validate_handles(definition_id, &channels)?;

        let stubs = self
            .services_mut()
            .stubs
            .as_deref_mut()
            .ok_or_else(|| anyhow!("no capability to spawn workflows"))?;
        stubs.stash_workflow(stub_id, definition_id, args, channels)?;

        self.current_execution()
            .push_stub_event(StubId::Workflow(stub_id), StubEventKind::Created);
        Ok(())
    }

    pub(crate) fn notify_on_child_init(
        &mut self,
        local_id: WorkflowId,
        result: Result<WorkflowAndChannelIds, HostError>,
    ) {
        let result = result.map(|mut ids| {
            for spec in ids.channel_ids.values_mut() {
                *spec = match spec {
                    Handle::Sender(id) => Handle::Receiver(*id),
                    Handle::Receiver(id) => Handle::Sender(*id),
                };
            }
            let child_state = ChildWorkflowState::new(ids.channel_ids);
            self.persisted
                .child_workflows
                .insert(ids.workflow_id, child_state);
            ids.workflow_id
        });

        if let Ok(workflow_id) = &result {
            self.current_execution().push_resource_event(
                ResourceId::Workflow(*workflow_id),
                ResourceEventKind::Created,
            );
        }
        self.current_execution()
            .push_stub_event(StubId::Workflow(local_id), StubEventKind::Mapped(result));
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
