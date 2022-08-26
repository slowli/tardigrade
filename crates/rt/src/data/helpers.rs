//! Various helpers for workflow state.

use serde::{Deserialize, Serialize};
use wasmtime::{AsContextMut, ExternRef, Store, StoreContextMut, Trap};

use std::{collections::HashSet, fmt, mem, task::Poll};

use crate::{
    data::{spawn::SpawnConfig, WorkflowData},
    receipt::{
        ChannelEvent, ChannelEventKind, Event, ExecutedFunction, PanicInfo, ResourceEvent,
        ResourceEventKind, ResourceId, WakeUpCause,
    },
    utils::serde_b64,
    TaskId, TimerId, WakerId, WorkflowId,
};
use tardigrade_shared::{JoinError, PollMessage, SendError};

/// Thin wrapper around `Vec<u8>`.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct Message(#[serde(with = "serde_b64")] Vec<u8>);

impl fmt::Debug for Message {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Message")
            .field("len", &self.0.len())
            .finish()
    }
}

impl AsRef<[u8]> for Message {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Message {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<Message> for Vec<u8> {
    fn from(message: Message) -> Self {
        message.0
    }
}

/// Unique reference to a channel within a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ChannelRef {
    pub workflow_id: Option<WorkflowId>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum HostResource {
    InboundChannel(ChannelRef),
    OutboundChannel(ChannelRef),
    SpawnConfig(SpawnConfig),
    Workflow(WorkflowId),
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

    pub fn as_spawn_config(&self) -> Result<&SpawnConfig, Trap> {
        if let Self::SpawnConfig(config) = self {
            Ok(config)
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
}

impl From<SpawnConfig> for HostResource {
    fn from(config: SpawnConfig) -> Self {
        Self::SpawnConfig(config)
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
#[derive(Debug)]
pub(super) struct Wakers {
    inner: HashSet<WakerId>,
    cause: WakeUpCause,
}

impl Wakers {
    pub fn new(wakers: HashSet<WakerId>, cause: WakeUpCause) -> Self {
        Self {
            inner: wakers,
            cause,
        }
    }

    pub fn into_iter(self) -> impl Iterator<Item = (WakerId, WakeUpCause)> {
        let cause = self.cause;
        self.inner
            .into_iter()
            .map(move |waker_id| (waker_id, cause.clone()))
    }
}

#[derive(Debug)]
pub(super) struct CurrentExecution {
    /// Executed function.
    pub function: ExecutedFunction,
    /// Tasks to be awaken after the task finishes polling.
    tasks_to_be_awoken: HashSet<TaskId>,
    /// Tasks to be aborted after the task finishes polling.
    tasks_to_be_aborted: HashSet<TaskId>,
    /// Information about a panic that has occurred during execution.
    panic_info: Option<PanicInfo>,
    /// Wakers created during execution, together with their placement.
    new_wakers: HashSet<WakerId>,
    /// Log of events.
    events: Vec<Event>,
}

impl CurrentExecution {
    pub fn new(function: ExecutedFunction) -> Self {
        Self {
            function,
            tasks_to_be_awoken: HashSet::new(),
            tasks_to_be_aborted: HashSet::new(),
            panic_info: None,
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

    pub fn set_panic(&mut self, panic_info: PanicInfo) {
        crate::warn!(
            "Execution {:?} led to a panic: {:?}",
            self.function,
            panic_info
        );
        self.panic_info = Some(panic_info);
    }

    fn resource_events(events: &[Event]) -> impl Iterator<Item = &ResourceEvent> {
        events.iter().filter_map(Event::as_resource_event)
    }

    fn channel_events(events: &[Event]) -> impl Iterator<Item = &ChannelEvent> {
        events.iter().filter_map(Event::as_channel_event)
    }

    pub fn commit(self, state: &mut WorkflowData) -> Vec<Event> {
        use self::ResourceEventKind::{Created, Dropped};

        crate::trace!("Committing {:?} onto {:?}", self, state);
        for event in Self::resource_events(&self.events) {
            match (event.kind, event.resource_id) {
                (Created, ResourceId::Task(task_id)) => {
                    let cause = WakeUpCause::Function(Box::new(self.function.clone()));
                    state.task_queue.insert_task(task_id, &cause);
                }
                (Dropped, ResourceId::Timer(timer_id)) => {
                    state.timers.remove(timer_id);
                }
                (Dropped, ResourceId::Task(task_id)) => {
                    state.complete_task(task_id, Err(JoinError::Aborted));
                }
                _ => { /* Do nothing */ }
            }
        }

        let cause = WakeUpCause::Function(Box::new(self.function));
        for task_id in self.tasks_to_be_awoken {
            state.task_queue.insert_task(task_id, &cause);
        }
        crate::trace!("Committed CurrentTask onto {:?}", state);
        self.events
    }

    pub fn revert(self, state: &mut WorkflowData) -> (Vec<Event>, Option<PanicInfo>) {
        use self::ResourceEventKind::Created;

        crate::trace!("Reverting {:?} from {:?}", self, state);
        for event in Self::resource_events(&self.events) {
            match (event.kind, event.resource_id) {
                (Created, ResourceId::Task(task_id)) => {
                    state.tasks.remove(&task_id);
                    // Since new tasks can only be mentioned in `self.tasks_to_be_awoken`, not in
                    // `state.task_queue`, cleaning up the queue is not needed.
                }
                (Created, ResourceId::Timer(timer_id)) => {
                    state.timers.remove(timer_id);
                }
                _ => { /* Do nothing */ }
            }
        }

        state.remove_wakers(&self.new_wakers);

        for event in Self::channel_events(&self.events) {
            if matches!(event.kind, ChannelEventKind::OutboundMessageSent { .. }) {
                let channel = state
                    .outbound_channel_mut(&ChannelRef {
                        workflow_id: event.workflow_id,
                        name: event.channel_name.clone(), // TODO: avoid cloning here
                    })
                    .unwrap();
                channel.messages.pop();
            }
        }

        crate::trace!("Reverted CurrentTask from {:?}", state);
        (self.events, self.panic_info)
    }
}

/// Waker-related `State` functionality.
impl WorkflowData {
    fn place_waker(&mut self, placement: &WakerPlacement, waker: WakerId) {
        if let Some(execution) = &mut self.current_execution {
            execution.new_wakers.insert(waker);
        }

        crate::trace!("Placing waker {} in {:?}", waker, placement);
        match placement {
            WakerPlacement::InboundChannel(channel_ref) => {
                let channel_state = self.inbound_channel_mut(channel_ref).unwrap();
                channel_state.wakes_on_next_element.insert(waker);
            }
            WakerPlacement::OutboundChannel(channel_ref) => {
                let channel_state = self.outbound_channel_mut(channel_ref).unwrap();
                channel_state.wakes_on_flush.insert(waker);
            }
            WakerPlacement::Timer(id) => {
                self.timers.place_waker(*id, waker);
            }
            WakerPlacement::TaskCompletion(task) => {
                self.tasks.get_mut(task).unwrap().insert_waker(waker);
            }
            WakerPlacement::WorkflowCompletion(workflow) => {
                self.child_workflows
                    .get_mut(workflow)
                    .unwrap()
                    .insert_waker(waker);
            }
        }
    }

    fn remove_wakers(&mut self, wakers: &HashSet<WakerId>) {
        for state in self.inbound_channels_mut() {
            state
                .wakes_on_next_element
                .retain(|waker_id| !wakers.contains(waker_id));
        }
        for state in self.outbound_channels_mut() {
            state
                .wakes_on_flush
                .retain(|waker_id| !wakers.contains(waker_id));
        }
        self.timers.remove_wakers(wakers);
    }

    pub(super) fn schedule_wakers(&mut self, wakers: HashSet<WakerId>, cause: WakeUpCause) {
        crate::trace!("Scheduled wakers {:?} with cause {:?}", wakers, cause);
        self.waker_queue.push(Wakers::new(wakers, cause));
    }

    pub(crate) fn take_wakers(&mut self) -> impl Iterator<Item = (WakerId, WakeUpCause)> {
        let wakers = mem::take(&mut self.waker_queue);
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
