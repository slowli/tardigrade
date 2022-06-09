//! Various helpers for workflow state.

use wasmtime::{Caller, Store, Trap};

use std::{collections::HashSet, fmt, mem, task::Poll};

use crate::{
    state::State, ExecutedFunction, ResourceEvent, ResourceEventKind, ResourceId, TaskId, TimerId,
    WakeUpCause, WakerId,
};

/// Thin wrapper around `Vec<u8>`.
#[derive(Clone, PartialEq)]
pub(super) struct Message(Vec<u8>);

impl fmt::Debug for Message {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Message")
            .field("len", &self.0.len())
            .finish()
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

impl Message {
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.clone()
    }
}

/// Pointer to the WASM `Context`, i.e., `*mut Context<'_>`.
pub(crate) type WasmContextPtr = u32;

#[derive(Debug)]
pub(super) enum WakerPlacement {
    InboundChannel(String),
    OutboundChannel(String),
    Timer(TimerId),
    TaskCompletion(TaskId),
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

    pub fn save_waker(self, caller: &mut Caller<'_, State>) -> Result<(), Trap> {
        if let Some(placement) = &self.placement {
            let waker_id = caller
                .data()
                .exports()
                .create_waker(&mut *caller, self.ptr)?;
            caller.data_mut().place_waker(placement, waker_id);
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

// FIXME: encapsulate channel progress as well
#[derive(Debug)]
pub(super) struct CurrentExecution {
    /// Executed function.
    pub function: ExecutedFunction,
    /// Tasks to be awaken after the task finishes polling.
    tasks_to_be_awoken: HashSet<TaskId>,
    /// Tasks to be aborted after the task finishes polling.
    tasks_to_be_aborted: HashSet<TaskId>,
    /// Log of resource events.
    resource_events: Vec<ResourceEvent>,
}

impl CurrentExecution {
    pub fn new(function: ExecutedFunction) -> Self {
        Self {
            function,
            tasks_to_be_awoken: HashSet::new(),
            tasks_to_be_aborted: HashSet::new(),
            resource_events: Vec::new(),
        }
    }

    pub fn register_task(&mut self, task_id: TaskId) {
        self.resource_events.push(ResourceEvent {
            resource_id: ResourceId::Task(task_id),
            kind: ResourceEventKind::Created,
        });
    }

    pub fn register_task_drop(&mut self, task_id: TaskId) {
        if self.tasks_to_be_aborted.insert(task_id) {
            self.resource_events.push(ResourceEvent {
                resource_id: ResourceId::Task(task_id),
                kind: ResourceEventKind::Dropped,
            });
        }
    }

    pub fn register_task_wakeup(&mut self, task_id: TaskId) {
        self.tasks_to_be_awoken.insert(task_id);
    }

    pub fn register_timer(&mut self, timer_id: TimerId) {
        self.resource_events.push(ResourceEvent {
            resource_id: ResourceId::Timer(timer_id),
            kind: ResourceEventKind::Created,
        });
    }

    pub fn register_timer_drop(&mut self, timer_id: TimerId) {
        self.resource_events.push(ResourceEvent {
            resource_id: ResourceId::Timer(timer_id),
            kind: ResourceEventKind::Dropped,
        });
    }

    pub fn commit(self, state: &mut State) -> Vec<ResourceEvent> {
        use self::ResourceEventKind::*;

        crate::trace!("Committing {:?} onto {:?}", self, state);
        for event in &self.resource_events {
            match (event.kind, event.resource_id) {
                (Created, ResourceId::Task(task_id)) => {
                    let cause = WakeUpCause::Spawned(Box::new(self.function.clone()));
                    state.task_queue.insert_task(task_id, &cause);
                }
                (Dropped, ResourceId::Timer(timer_id)) => {
                    state.timers.remove(timer_id);
                }
                _ => { /* Do nothing */ }
            }
        }

        for task_id in self.tasks_to_be_awoken {
            state.task_queue.insert_task(task_id, &WakeUpCause::Task(0)); // FIXME: ???
        }
        crate::trace!("Committed CurrentTask onto {:?}", state);
        self.resource_events
    }

    pub fn revert(self, state: &mut State) -> Vec<ResourceEvent> {
        use self::ResourceEventKind::*;

        crate::trace!("Reverting {:?} from {:?}", self, state);
        for event in &self.resource_events {
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
        crate::trace!("Reverted CurrentTask from {:?}", state);
        self.resource_events
    }
}

/// Waker-related `State` functionality.
impl State {
    fn place_waker(&mut self, placement: &WakerPlacement, waker: WakerId) {
        crate::trace!("Placing waker {} in {:?}", waker, placement);
        match placement {
            WakerPlacement::InboundChannel(name) => {
                let channel_state = self.inbound_channels.get_mut(name).unwrap();
                channel_state.wakes_on_next_element.insert(waker);
            }
            WakerPlacement::OutboundChannel(name) => {
                let channel_state = self.outbound_channels.get_mut(name).unwrap();
                channel_state.wakes_on_flush.insert(waker);
            }
            WakerPlacement::Timer(id) => {
                self.timers.place_waker(*id, waker);
            }
            WakerPlacement::TaskCompletion(task) => {
                self.tasks.get_mut(task).unwrap().insert_waker(waker);
            }
        }
    }

    pub(super) fn schedule_wakers(&mut self, wakers: HashSet<WakerId>, cause: WakeUpCause) {
        crate::trace!("Scheduled wakers {:?} with cause {:?}", wakers, cause);
        self.waker_queue.push(Wakers::new(wakers, cause));
    }

    pub fn take_wakers(&mut self) -> impl Iterator<Item = (WakerId, WakeUpCause)> {
        let wakers = mem::take(&mut self.waker_queue);
        wakers.into_iter().flat_map(Wakers::into_iter)
    }

    pub fn wake(
        store: &mut Store<Self>,
        waker_id: WakerId,
        cause: WakeUpCause,
    ) -> Result<(), Trap> {
        store.data_mut().current_wakeup_cause = Some(cause);
        let result = store.data().exports().wake_waker(&mut *store, waker_id);
        store.data_mut().current_wakeup_cause = None;
        result
    }
}
