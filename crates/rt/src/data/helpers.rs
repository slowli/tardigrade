//! Various helpers for workflow state.

use serde::{Deserialize, Serialize};
use wasmtime::{AsContextMut, ExternRef, Store, StoreContextMut, Trap};

use std::{collections::HashSet, mem, task::Poll};

use crate::{
    data::{spawn::SharedChannelHandles, PersistedWorkflowData, WorkflowData},
    receipt::{
        ChannelEvent, ChannelEventKind, Event, ExecutedFunction, PanicInfo, ResourceEvent,
        ResourceEventKind, ResourceId, WakeUpCause,
    },
};
use tardigrade::{
    abi::PollMessage,
    channel::SendError,
    interface::ChannelKind,
    task::{JoinError, TaskError},
    ChannelId, TaskId, TimerId, WakerId, WorkflowId,
};

/// Unique reference to a channel within a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ChannelRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum HostResource {
    InboundChannel(ChannelRef),
    OutboundChannel(ChannelRef),
    #[serde(skip)] // FIXME: why is this allowed?
    ChannelHandles(SharedChannelHandles),
    Workflow(WorkflowId),
    WorkflowStub(WorkflowId),
}

impl HostResource {
    pub fn inbound_channel(workflow_id: Option<WorkflowId>, name: String) -> Self {
        Self::InboundChannel(ChannelRef { workflow_id, name })
    }

    pub fn outbound_channel(workflow_id: Option<WorkflowId>, name: String) -> Self {
        Self::OutboundChannel(ChannelRef { workflow_id, name })
    }

    pub fn from_ref(reference: Option<&ExternRef>) -> Result<&Self, Trap> {
        let reference = reference.ok_or_else(|| Trap::new("null reference provided to runtime"))?;
        reference
            .data()
            .downcast_ref::<Self>()
            .ok_or_else(|| Trap::new("reference of unexpected type"))
    }

    pub fn into_ref(self) -> ExternRef {
        ExternRef::new(self)
    }

    pub fn as_inbound_channel(&self) -> Result<&ChannelRef, Trap> {
        if let Self::InboundChannel(channel_ref) = self {
            Ok(channel_ref)
        } else {
            let message = format!(
                "unexpected reference type: expected inbound channel, got {:?}",
                self
            );
            Err(Trap::new(message))
        }
    }

    pub fn as_outbound_channel(&self) -> Result<&ChannelRef, Trap> {
        if let Self::OutboundChannel(channel_ref) = self {
            Ok(channel_ref)
        } else {
            let message = format!(
                "unexpected reference type: expected outbound channel, got {:?}",
                self
            );
            Err(Trap::new(message))
        }
    }

    pub fn as_channel_handles(&self) -> Result<&SharedChannelHandles, Trap> {
        if let Self::ChannelHandles(handles) = self {
            Ok(handles)
        } else {
            let message = format!(
                "unexpected reference type: expected workflow spawn config, got {:?}",
                self
            );
            Err(Trap::new(message))
        }
    }

    pub fn as_workflow(&self) -> Result<WorkflowId, Trap> {
        if let Self::Workflow(id) = self {
            Ok(*id)
        } else {
            let message = format!(
                "unexpected reference type: expected workflow handle, got {:?}",
                self
            );
            Err(Trap::new(message))
        }
    }

    pub fn as_workflow_stub(&self) -> Result<WorkflowId, Trap> {
        if let Self::WorkflowStub(id) = self {
            Ok(*id)
        } else {
            let message = format!(
                "unexpected reference type: expected workflow stub handle, got {:?}",
                self
            );
            Err(Trap::new(message))
        }
    }
}

impl From<SharedChannelHandles> for HostResource {
    fn from(handles: SharedChannelHandles) -> Self {
        Self::ChannelHandles(handles)
    }
}

/// Pointer to the WASM `Context`, i.e., `*mut Context<'_>`.
pub(crate) type WasmContextPtr = u32;

#[derive(Debug)]
pub(super) enum WakerPlacement {
    InboundChannel(ChannelRef),
    OutboundChannel(ChannelRef),
    Timer(TimerId),
    TaskCompletion(TaskId),
    WorkflowInit(WorkflowId),
    WorkflowCompletion(WorkflowId),
}

#[derive(Debug)]
#[must_use = "Needs to be converted to a waker"]
pub(super) struct WasmContext {
    ptr: WasmContextPtr,
    placement: Option<WakerPlacement>,
}

impl WasmContext {
    pub fn new(ptr: WasmContextPtr) -> Self {
        Self {
            ptr,
            placement: None,
        }
    }

    pub fn save_waker(self, ctx: &mut StoreContextMut<'_, WorkflowData>) -> Result<(), Trap> {
        if let Some(placement) = &self.placement {
            let waker_id = ctx
                .data()
                .exports()
                .create_waker(ctx.as_context_mut(), self.ptr)?;
            ctx.data_mut().place_waker(placement, waker_id);
        }
        Ok(())
    }
}

/// Helper extension trait to deal with wakers more fluently.
pub(super) trait WakeIfPending {
    fn wake_if_pending(
        self,
        cx: &mut WasmContext,
        waker_placement: impl FnOnce() -> WakerPlacement,
    ) -> Self;
}

impl<T> WakeIfPending for Poll<T> {
    fn wake_if_pending(
        self,
        cx: &mut WasmContext,
        waker_placement: impl FnOnce() -> WakerPlacement,
    ) -> Self {
        if self.is_pending() {
            debug_assert!(cx.placement.is_none());
            cx.placement = Some(waker_placement());
        }
        self
    }
}

#[must_use = "Wakers need to be actually woken up"]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Wakers {
    ids: HashSet<WakerId>,
    cause: WakeUpCause,
}

impl Wakers {
    pub fn new(ids: HashSet<WakerId>, cause: WakeUpCause) -> Self {
        Self { ids, cause }
    }

    pub fn cause(&self) -> &WakeUpCause {
        &self.cause
    }

    pub fn into_iter(self) -> impl Iterator<Item = (WakerId, WakeUpCause)> {
        let cause = self.cause;
        self.ids
            .into_iter()
            .map(move |waker_id| (waker_id, cause.clone()))
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
        let wake_up_cause = if let ExecutedFunction::Waker { wake_up_cause, .. } = function {
            // Copy the cause; we're not really interested in
            // `WakeUpCause::Function { task_id: None }` that would result otherwise.
            wake_up_cause.clone()
        } else {
            WakeUpCause::Function { task_id }
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

    pub fn push_inbound_channel_event(&mut self, channel_ref: &ChannelRef, result: &PollMessage) {
        self.push_event(ChannelEvent {
            kind: ChannelEventKind::InboundChannelPolled {
                result: match result {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(maybe_message) => Poll::Ready(maybe_message.as_ref().map(Vec::len)),
                },
            },
            channel_name: channel_ref.name.clone(),
            workflow_id: channel_ref.workflow_id,
        });
    }

    pub fn push_channel_closure(
        &mut self,
        kind: ChannelKind,
        channel_ref: &ChannelRef,
        channel_id: ChannelId,
        remaining_alias_count: usize,
    ) {
        self.push_event(ChannelEvent {
            kind: match kind {
                ChannelKind::Inbound => ChannelEventKind::InboundChannelClosed(channel_id),
                ChannelKind::Outbound => ChannelEventKind::OutboundChannelClosed {
                    channel_id,
                    remaining_alias_count,
                },
            },
            channel_name: channel_ref.name.clone(),
            workflow_id: channel_ref.workflow_id,
        });
    }

    pub fn push_outbound_poll_event(
        &mut self,
        channel_ref: &ChannelRef,
        flush: bool,
        result: Poll<Result<(), SendError>>,
    ) {
        self.push_event(ChannelEvent {
            kind: if flush {
                ChannelEventKind::OutboundChannelFlushed { result }
            } else {
                ChannelEventKind::OutboundChannelReady { result }
            },
            channel_name: channel_ref.name.clone(),
            workflow_id: channel_ref.workflow_id,
        });
    }

    pub fn push_outbound_message_event(&mut self, channel_ref: &ChannelRef, message_len: usize) {
        self.push_event(ChannelEvent {
            kind: ChannelEventKind::OutboundMessageSent { message_len },
            channel_name: channel_ref.name.clone(),
            workflow_id: channel_ref.workflow_id,
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
                let channel = state
                    .persisted
                    .outbound_channel_mut(&ChannelRef {
                        workflow_id: event.workflow_id,
                        name: event.channel_name.clone(), // TODO: avoid cloning here
                    })
                    .unwrap();
                channel.messages.pop();
            }
        }

        (self.events, self.panic_info)
    }
}

/// Waker-related `State` functionality.
impl WorkflowData<'_> {
    #[tracing::instrument(level = "debug")]
    fn place_waker(&mut self, placement: &WakerPlacement, waker: WakerId) {
        if let Some(execution) = &mut self.current_execution {
            execution.new_wakers.insert(waker);
        }

        let persisted = &mut self.persisted;
        match placement {
            WakerPlacement::InboundChannel(channel_ref) => {
                let channel_state = persisted.inbound_channel_mut(channel_ref).unwrap();
                channel_state.wakes_on_next_element.insert(waker);
            }
            WakerPlacement::OutboundChannel(channel_ref) => {
                let channel_state = persisted.outbound_channel_mut(channel_ref).unwrap();
                channel_state.wakes_on_flush.insert(waker);
            }
            WakerPlacement::Timer(id) => {
                persisted.timers.place_waker(*id, waker);
            }
            WakerPlacement::TaskCompletion(task) => {
                persisted.tasks.get_mut(task).unwrap().insert_waker(waker);
            }
            WakerPlacement::WorkflowInit(stub) => {
                persisted.child_workflow_stubs.insert_waker(*stub, waker);
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
        for state in self.persisted.inbound_channels_mut() {
            state
                .wakes_on_next_element
                .retain(|waker_id| !wakers.contains(waker_id));
        }
        for (_, _, state) in self.persisted.outbound_channels_mut() {
            state
                .wakes_on_flush
                .retain(|waker_id| !wakers.contains(waker_id));
        }
        self.persisted.timers.remove_wakers(wakers);
    }

    pub(crate) fn take_wakers(&mut self) -> impl Iterator<Item = (WakerId, WakeUpCause)> {
        let wakers = mem::take(&mut self.persisted.waker_queue);
        wakers.into_iter().flat_map(Wakers::into_iter)
    }

    pub(crate) fn wake(
        store: &mut Store<Self>,
        waker_id: WakerId,
        cause: WakeUpCause,
    ) -> Result<(), Trap> {
        store.data_mut().current_wakeup_cause = Some(cause);
        let result = store
            .data()
            .exports()
            .wake_waker(store.as_context_mut(), waker_id);
        store.data_mut().current_wakeup_cause = None;
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
