//! Workflow persistence.

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use std::task::Poll;

use crate::{
    data::{
        Channels, ChildWorkflow, ConsumeError, PersistError, PersistedWorkflowData, ReceiverState,
        SenderState, TaskState, TimerState, Wakers,
    },
    engine::{CreateWorkflow, PersistWorkflow, RunWorkflow, WorkflowSpawner},
    manager::Services,
    receipt::WakeUpCause,
    utils::Message,
    workflow::{ChannelIds, Workflow},
};
use tardigrade::{
    spawn::{ChannelsConfig, HostError},
    task::JoinError,
    ChannelId, TaskId, TimerId, WorkflowId,
};

/// Persisted version of a workflow containing the state of its external dependencies
/// (channels and timers), and its linear WASM memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedWorkflow {
    data: PersistedWorkflowData,
    engine_data: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    args: Option<Message>,
}

impl PersistedWorkflow {
    pub(super) fn new<T>(workflow: &mut Workflow<T>) -> Result<Self, PersistError>
    where
        T: RunWorkflow + PersistWorkflow,
    {
        workflow.inner.data().check_persistence()?;
        let engine_data = workflow.inner.persist();
        let engine_data = serde_json::to_value(&engine_data).expect("cannot serialize engine data");
        Ok(Self {
            data: workflow.inner.data().persist(),
            engine_data,
            args: workflow.args.clone(),
        })
    }

    pub(crate) fn receivers(&self) -> impl Iterator<Item = (ChannelId, &ReceiverState)> + '_ {
        self.data.receivers()
    }

    pub(crate) fn receiver(&self, channel_id: ChannelId) -> Option<&ReceiverState> {
        self.data.receiver(channel_id)
    }

    pub(crate) fn senders(&self) -> impl Iterator<Item = (ChannelId, &SenderState)> + '_ {
        self.data.senders()
    }

    /// Returns information about channels defined in this workflow interface.
    pub fn channels(&self) -> Channels<'_> {
        self.data.channels()
    }

    #[tracing::instrument(level = "debug", skip(self, message), err, fields(message.len = message.len()))]
    pub(crate) fn push_message_for_receiver(
        &mut self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), ConsumeError> {
        self.data.push_message_for_receiver(channel_id, message)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn drop_message_for_receiver(&mut self, channel_id: ChannelId) {
        self.data.drop_message_for_receiver(channel_id);
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn close_receiver(&mut self, channel_id: ChannelId) {
        self.data.close_receiver(channel_id);
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn close_sender(&mut self, channel_id: ChannelId) {
        self.data.close_sender(channel_id);
    }

    /// Returns the current state of a task with the specified ID.
    pub fn task(&self, task_id: TaskId) -> Option<&TaskState> {
        self.data.task(task_id)
    }

    /// Returns the current state of the main task (the task that initializes the workflow),
    /// or `None` if the workflow is not initialized.
    pub fn main_task(&self) -> Option<&TaskState> {
        self.data.main_task()
    }

    /// Lists all tasks in this workflow.
    pub fn tasks(&self) -> impl Iterator<Item = (TaskId, &TaskState)> + '_ {
        self.data.tasks()
    }

    /// Enumerates child workflows.
    pub fn child_workflows(&self) -> impl Iterator<Item = (WorkflowId, ChildWorkflow<'_>)> + '_ {
        self.data.child_workflows()
    }

    /// Returns the local state of the child workflow with the specified ID, or `None`
    /// if a workflow with such ID was not spawned by this workflow.
    pub fn child_workflow(&self, id: WorkflowId) -> Option<ChildWorkflow<'_>> {
        self.data.child_workflow(id)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn notify_on_child_completion(
        &mut self,
        id: WorkflowId,
        result: Result<(), JoinError>,
    ) {
        self.data.notify_on_child_completion(id, result);
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn notify_on_child_init(
        &mut self,
        stub_id: WorkflowId,
        id: WorkflowId,
        channels: &ChannelsConfig<ChannelId>,
        channel_ids: ChannelIds,
    ) {
        self.data
            .notify_on_child_init(stub_id, id, channels, channel_ids);
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn notify_on_child_spawn_error(&mut self, stub_id: WorkflowId, err: HostError) {
        self.data.notify_on_child_spawn_error(stub_id, err);
    }

    /// Checks whether the workflow is initialized.
    pub fn is_initialized(&self) -> bool {
        self.args.is_none()
    }

    /// Returns the result of executing this workflow, which is the output of its main task.
    pub fn result(&self) -> Poll<Result<(), &JoinError>> {
        self.data.result()
    }

    /// Aborts the workflow by changing the result of its main task.
    pub(crate) fn abort(&mut self) {
        self.data.abort();
    }

    /// Returns the current time for the workflow.
    pub fn current_time(&self) -> DateTime<Utc> {
        self.data.timers.last_known_time()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn set_current_time(&mut self, time: DateTime<Utc>) {
        self.data.set_current_time(time);
    }

    /// Returns a timer with the specified `id`.
    pub fn timer(&self, id: TimerId) -> Option<&TimerState> {
        self.data.timers.get(id)
    }

    /// Enumerates all timers together with their states.
    pub fn timers(&self) -> impl Iterator<Item = (TimerId, &TimerState)> + '_ {
        self.data.timers.iter()
    }

    /// Iterates over pending [`WakeUpCause`]s.
    pub fn pending_wakeup_causes(&self) -> impl Iterator<Item = &WakeUpCause> + '_ {
        self.data.waker_queue.iter().map(Wakers::cause)
    }

    /// Restores a workflow from the persisted state and the `spawner` defining the workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow definition from `module` and the `persisted` state
    /// do not match (e.g., differ in defined channels).
    pub(crate) fn restore<S: CreateWorkflow>(
        self,
        spawner: &WorkflowSpawner<S>,
        services: Services,
    ) -> anyhow::Result<Workflow<S::Spawned>> {
        let interface = spawner.interface();
        let data = self
            .data
            .restore(interface, services)
            .context("failed restoring workflow state")?;

        let engine_data =
            serde_json::from_value(self.engine_data).context("failed deserializing engine data")?;
        let mut workflow = Workflow::new(spawner, data, self.args)?;
        workflow.inner.restore(engine_data)?;
        Ok(workflow)
    }
}
