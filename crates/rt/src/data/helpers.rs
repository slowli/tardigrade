//! Various helpers for workflow state.

use serde::{Deserialize, Serialize};

use std::{collections::HashSet, mem, task::Poll};

use crate::{
    data::{PersistedWorkflowData, WorkflowData},
    engine::{CreateWaker, RunWorkflow},
    receipt::{
        ChannelEvent, ChannelEventKind, Event, ExecutedFunction, PanicInfo, ResourceEvent,
        ResourceEventKind, ResourceId, StubEvent, StubEventKind, StubId, WakeUpCause,
    },
};
use tardigrade::{
    abi::PollMessage,
    channel::SendError,
    handle::Handle,
    task::{JoinError, TaskError},
    ChannelId, TaskId, TimerId, WakerId, WorkflowId,
};

#[derive(Debug)]
pub(super) enum WakerPlacement {
    Receiver(ChannelId),
    Sender(ChannelId),
    Timer(TimerId),
    TaskCompletion(TaskId),
    WorkflowCompletion(WorkflowId),
}

/// Polling context for workflows.
#[derive(Debug)]
#[must_use = "Needs to be converted to a waker"]
pub struct WorkflowPoll<T> {
    inner: Poll<T>,
    placement: Option<WakerPlacement>,
}

impl<T> WorkflowPoll<T> {
    pub(super) fn new(inner: Poll<T>, waker_placement: WakerPlacement) -> Self {
        Self {
            placement: if inner.is_pending() {
                Some(waker_placement)
            } else {
                None
            },
            inner,
        }
    }

    /// Saves the waker potentially contained in this context.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`CreateWaker::create_waker()`].
    pub fn into_inner(self, cx: &mut impl CreateWaker) -> anyhow::Result<Poll<T>> {
        if let Some(placement) = &self.placement {
            let waker_id = cx.create_waker()?;
            cx.data_mut().place_waker(placement, waker_id);
        }
        Ok(self.inner)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum WakerOrTask {
    Waker(WakerId),
    Task(TaskId),
}

#[must_use = "Wakers need to be actually woken up"]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Wakers {
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    ids: HashSet<WakerId>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    task_ids: HashSet<TaskId>,
    cause: WakeUpCause,
}

impl Wakers {
    pub fn new(ids: HashSet<WakerId>, cause: WakeUpCause) -> Self {
        Self {
            ids,
            task_ids: HashSet::new(),
            cause,
        }
    }

    pub fn from_task(task_id: TaskId, cause: WakeUpCause) -> Self {
        Self {
            ids: HashSet::new(),
            task_ids: HashSet::from_iter([task_id]),
            cause,
        }
    }

    pub fn cause(&self) -> &WakeUpCause {
        &self.cause
    }

    pub fn into_iter(self) -> impl Iterator<Item = (WakerOrTask, WakeUpCause)> {
        let cause = self.cause.clone();
        let wakers = self
            .ids
            .into_iter()
            .map(move |waker_id| (WakerOrTask::Waker(waker_id), cause.clone()));

        let cause = self.cause;
        let tasks = self
            .task_ids
            .into_iter()
            .map(move |task_id| (WakerOrTask::Task(task_id), cause.clone()));
        tasks.chain(wakers)
    }
}

#[derive(Debug)]
pub(super) struct CurrentExecution {
    /// ID of the executed task, if any.
    pub task_id: Option<TaskId>,
    /// Wakeup cause for wakers created by this execution.
    wake_up_cause: WakeUpCause,
    /// Tasks to be awaken after the task finishes polling.
    tasks_to_be_awoken: HashSet<TaskId>,
    /// Tasks to be aborted after the task finishes polling.
    tasks_to_be_aborted: HashSet<TaskId>,
    /// Information about a panic that has occurred during execution.
    panic_info: Option<PanicInfo>,
    /// Information about a task error that has occurred during execution.
    task_error: Option<TaskError>,
    /// Wakers created during execution, together with their placement.
    new_wakers: HashSet<WakerId>,
    /// Log of events.
    events: Vec<Event>,
}

impl CurrentExecution {
    pub fn new(function: &ExecutedFunction) -> Self {
        let task_id = function.task_id();
        let wake_up_cause = match function {
            ExecutedFunction::Waker { wake_up_cause, .. } => {
                // Copy the cause; we're not really interested in
                // `WakeUpCause::Function { task_id: None }` that would result otherwise.
                wake_up_cause.clone()
            }
            ExecutedFunction::StubInitialization => WakeUpCause::StubInitialized,
            _ => WakeUpCause::Function { task_id },
        };

        Self {
            task_id,
            wake_up_cause,
            tasks_to_be_awoken: HashSet::new(),
            tasks_to_be_aborted: HashSet::new(),
            panic_info: None,
            task_error: None,
            new_wakers: HashSet::new(),
            events: Vec::new(),
        }
    }

    fn push_event(&mut self, event: impl Into<Event>) {
        self.events.push(event.into());
    }

    pub fn register_task_wakeup(&mut self, task_id: TaskId) {
        self.tasks_to_be_awoken.insert(task_id);
    }

    pub fn push_receiver_event(&mut self, channel_id: ChannelId, result: &PollMessage) {
        self.push_event(ChannelEvent {
            kind: ChannelEventKind::ReceiverPolled {
                result: match result {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(maybe_message) => Poll::Ready(maybe_message.as_ref().map(Vec::len)),
                },
            },
            channel_id,
        });
    }

    pub fn push_channel_closure(&mut self, id_handle: Handle<ChannelId>) {
        self.push_event(ChannelEvent {
            kind: match id_handle {
                Handle::Receiver(_) => ChannelEventKind::ReceiverClosed,
                Handle::Sender(_) => ChannelEventKind::SenderClosed,
            },
            channel_id: id_handle.factor(),
        });
    }

    pub fn push_sender_poll_event(
        &mut self,
        channel_id: ChannelId,
        flush: bool,
        result: Poll<Result<(), SendError>>,
    ) {
        self.push_event(ChannelEvent {
            kind: if flush {
                ChannelEventKind::SenderFlushed { result }
            } else {
                ChannelEventKind::SenderReady { result }
            },
            channel_id,
        });
    }

    pub fn push_outbound_message_event(&mut self, channel_id: ChannelId, message_len: usize) {
        self.push_event(ChannelEvent {
            kind: ChannelEventKind::OutboundMessageSent { message_len },
            channel_id,
        });
    }

    pub fn push_resource_event(&mut self, resource_id: ResourceId, kind: ResourceEventKind) {
        if let (ResourceId::Task(id), ResourceEventKind::Dropped) = (resource_id, kind) {
            if !self.tasks_to_be_aborted.insert(id) {
                return; // The task is already scheduled for drop
            }
        }
        self.push_event(ResourceEvent { resource_id, kind });
    }

    pub fn push_stub_event(&mut self, stub_id: StubId, kind: StubEventKind) {
        self.push_event(StubEvent { kind, stub_id });
    }

    pub fn set_panic(&mut self, info: PanicInfo) {
        tracing::warn!(?info, "execution led to a panic");
        self.panic_info = Some(info);
    }

    pub fn push_task_error(&mut self, info: PanicInfo) {
        tracing::warn!(?info, "execution led to a task error");
        if let Some(err) = &mut self.task_error {
            let (message, location) = info.into_parts();
            err.push_context_from_parts(message, location);
        } else {
            self.task_error = Some(info.into());
        }
    }

    pub fn take_task_error(&mut self) -> Option<TaskError> {
        self.task_error.take()
    }

    fn resource_events(events: &[Event]) -> impl Iterator<Item = &ResourceEvent> {
        events.iter().filter_map(Event::as_resource_event)
    }

    fn channel_events(events: &[Event]) -> impl Iterator<Item = &ChannelEvent> {
        events.iter().filter_map(Event::as_channel_event)
    }

    #[tracing::instrument(level = "debug")]
    pub fn commit(self, state: &mut WorkflowData) -> Vec<Event> {
        use self::ResourceEventKind::{Created, Dropped};

        if let Some(err) = &self.task_error {
            tracing::warn!(
                ?err,
                "task error was reported, but the task was not completed"
            );
        }

        let cause = self.wake_up_cause;
        for event in Self::resource_events(&self.events) {
            match (event.kind, event.resource_id) {
                (Created, ResourceId::Task(task_id)) => {
                    state.task_queue.insert_task(task_id, &cause);
                }
                (Dropped, ResourceId::Timer(timer_id)) => {
                    state.persisted.timers.remove(timer_id);
                }
                (Dropped, ResourceId::Task(task_id)) => {
                    state
                        .persisted
                        .complete_task(task_id, Err(JoinError::Aborted));
                }
                _ => { /* Do nothing */ }
            }
        }

        for task_id in self.tasks_to_be_awoken {
            state.task_queue.insert_task(task_id, &cause);
        }
        self.events
    }

    #[tracing::instrument(level = "debug")]
    pub fn revert(self, state: &mut WorkflowData) -> (Vec<Event>, Option<PanicInfo>) {
        use self::ResourceEventKind::Created;

        for event in Self::resource_events(&self.events) {
            match (event.kind, event.resource_id) {
                (Created, ResourceId::Task(task_id)) => {
                    state.persisted.tasks.remove(&task_id);
                    // Since new tasks can only be mentioned in `self.tasks_to_be_awoken`, not in
                    // `state.task_queue`, cleaning up the queue is not needed.
                }
                (Created, ResourceId::Timer(timer_id)) => {
                    state.persisted.timers.remove(timer_id);
                }
                _ => { /* Do nothing */ }
            }
        }

        state.remove_wakers(&self.new_wakers);

        for event in Self::channel_events(&self.events) {
            if matches!(event.kind, ChannelEventKind::OutboundMessageSent { .. }) {
                let channel = state.persisted.sender_mut(event.channel_id).unwrap();
                channel.messages.pop();
            }
        }

        (self.events, self.panic_info)
    }
}

/// Waker-related `State` functionality.
impl WorkflowData {
    #[tracing::instrument(level = "debug")]
    fn place_waker(&mut self, placement: &WakerPlacement, waker: WakerId) {
        if let Some(execution) = &mut self.current_execution {
            execution.new_wakers.insert(waker);
        }

        let persisted = &mut self.persisted;
        match placement {
            WakerPlacement::Receiver(channel_id) => {
                let channel_state = persisted.receiver_mut(*channel_id).unwrap();
                channel_state.wakes_on_next_element.insert(waker);
            }
            WakerPlacement::Sender(channel_id) => {
                let channel_state = persisted.sender_mut(*channel_id).unwrap();
                channel_state.wakes_on_flush.insert(waker);
            }
            WakerPlacement::Timer(id) => {
                persisted.timers.place_waker(*id, waker);
            }
            WakerPlacement::TaskCompletion(task) => {
                persisted.tasks.get_mut(task).unwrap().insert_waker(waker);
            }
            WakerPlacement::WorkflowCompletion(workflow) => {
                persisted
                    .child_workflows
                    .get_mut(workflow)
                    .unwrap()
                    .insert_waker(waker);
            }
        }
    }

    fn remove_wakers(&mut self, wakers: &HashSet<WakerId>) {
        for state in self.persisted.receivers_mut() {
            state
                .wakes_on_next_element
                .retain(|waker_id| !wakers.contains(waker_id));
        }
        for (_, state) in self.persisted.senders_mut() {
            state
                .wakes_on_flush
                .retain(|waker_id| !wakers.contains(waker_id));
        }
        self.persisted.timers.remove_wakers(wakers);
    }

    pub(crate) fn take_wakers(&mut self) -> impl Iterator<Item = (WakerOrTask, WakeUpCause)> {
        let wakers = mem::take(&mut self.persisted.waker_queue);
        wakers.into_iter().flat_map(Wakers::into_iter)
    }

    pub(crate) fn wake<T: RunWorkflow, R>(
        workflow: &mut T,
        cause: WakeUpCause,
        action: impl FnOnce(&mut T) -> R,
    ) -> R {
        workflow.data_mut().current_wakeup_cause = Some(cause);
        let result = action(workflow);
        workflow.data_mut().current_wakeup_cause = None;
        result
    }
}

impl PersistedWorkflowData {
    #[tracing::instrument(level = "debug")]
    pub(super) fn schedule_wakers(&mut self, wakers: HashSet<WakerId>, cause: WakeUpCause) {
        if wakers.is_empty() {
            return; // no need to schedule anything
        }
        self.waker_queue.push(Wakers::new(wakers, cause));
    }
}
