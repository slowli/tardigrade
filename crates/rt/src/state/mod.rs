//! Workflow state.

use chrono::{DateTime, Utc};
use wasmtime::{Caller, Store, Trap};

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt, mem,
    ops::Range,
    task::Poll,
};

mod persistence;
pub use self::persistence::{PersistError, WorkflowState};

use crate::{
    module::ModuleExports,
    time::{Timer, TimerId, Timers},
    ExecutedFunction, ResourceEvent, ResourceEventKind, ResourceId, TaskId,
};
use tardigrade_shared::{workflow::Interface, ChannelErrorKind, JoinError, PollMessage};

/// Thin wrapper around `Vec<u8>`.
#[derive(Clone, PartialEq)]
struct Message(Vec<u8>);

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

/// WASM waker ID. Equal to a pointer to the waker instance.
pub type WakerId = u32;

pub(crate) type WasmContextPointer = u32;

#[derive(Debug)]
enum WakerPlacement {
    InboundChannel(String),
    OutboundChannel(String),
    Timer(TimerId),
    TaskCompletion(TaskId),
}

#[derive(Debug)]
#[must_use = "Needs to be converted to a waker"]
pub(crate) struct WasmContext {
    ptr: WasmContextPointer,
    placement: Option<WakerPlacement>,
}

impl WasmContext {
    pub fn new(ptr: WasmContextPointer) -> Self {
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
trait WakeIfPending {
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
struct Wakers {
    inner: HashSet<WakerId>,
    cause: WakeUpCause,
}

impl Wakers {
    fn new(wakers: HashSet<WakerId>, cause: WakeUpCause) -> Self {
        Self {
            inner: wakers,
            cause,
        }
    }

    fn into_iter(self) -> impl Iterator<Item = (WakerId, WakeUpCause)> {
        let cause = self.cause;
        self.inner
            .into_iter()
            .map(move |waker_id| (waker_id, cause.clone()))
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ConsumeErrorKind {
    /// Channel is not registered in the workflow interface.
    UnknownChannel,
    /// No tasks listen to the channel.
    NotListened,
}

impl fmt::Display for ConsumeErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnknownChannel => "channel is not registered in the workflow interface",
            Self::NotListened => "channel not currently listened to",
        })
    }
}

#[derive(Debug)]
pub struct ConsumeError {
    channel_name: String,
    message: Vec<u8>,
    kind: ConsumeErrorKind,
}

impl fmt::Display for ConsumeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot push message ({} bytes) into channel `{}`: {}",
            self.message.len(),
            self.channel_name,
            self.kind
        )
    }
}

impl Error for ConsumeError {}

impl ConsumeError {
    pub fn kind(&self) -> &ConsumeErrorKind {
        &self.kind
    }

    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }

    pub fn into_message(self) -> Vec<u8> {
        self.message
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WakeUpCause {
    /// Woken up by an inbound message.
    InboundMessage {
        channel_name: String,
        message_index: usize,
    },
    /// Woken up by flushing an outbound channel.
    Flush {
        channel_name: String,
        /// Indexes of flushed messages.
        message_indexes: Range<usize>,
    },

    /// Initial task enqueuing after it was spawned.
    Spawned(Box<ExecutedFunction>),
    /// Woken up by another task (e.g., due to internal channels or other sync primitives).
    Task(TaskId),
    /// Woken up by task completion.
    CompletedTask(TaskId),
    /// Woken up by a timer.
    Timer {
        /// Timer ID.
        id: TimerId,
    },
}

/// Priority queue for tasks.
#[derive(Debug, Default)]
struct TaskQueue {
    inner: HashMap<TaskId, WakeUpCause>,
}

impl TaskQueue {
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn insert_task(&mut self, task: TaskId, cause: &WakeUpCause) {
        self.inner.entry(task).or_insert_with(|| cause.clone());
    }

    fn take_task(&mut self) -> Option<(TaskId, WakeUpCause)> {
        let key = *self.inner.keys().next()?;
        let value = self.inner.remove(&key)?;
        Some((key, value))
    }
}

#[derive(Debug, Default)]
struct InboundChannelState {
    is_acquired: bool,
    received_messages: usize,
    pending_message: Option<Message>,
    wakes_on_next_element: HashSet<WakerId>,
}

impl InboundChannelState {
    fn poll_next(&mut self) -> Poll<Option<Vec<u8>>> {
        if let Some(message) = self.pending_message.take() {
            Poll::Ready(Some(message.into()))
        } else {
            Poll::Pending
        }
    }
}

#[derive(Debug, Default)]
struct OutboundChannelState {
    flushed_messages: usize,
    messages: Vec<Message>,
    wakes_on_flush: HashSet<WakerId>,
}

#[derive(Debug, Clone)]
struct TaskState {
    _name: String,
    completion_result: Option<Result<(), JoinError>>,
    _spawned_by: ExecutedFunction,
    wakes_on_completion: HashSet<WakerId>,
}

impl TaskState {
    fn new(name: String, spawned_by: ExecutedFunction) -> Self {
        Self {
            _name: name,
            completion_result: None,
            _spawned_by: spawned_by,
            wakes_on_completion: HashSet::new(),
        }
    }

    fn is_completed(&self) -> bool {
        self.completion_result.is_some()
    }
}

// FIXME: encapsulate channel progress as well
#[derive(Debug)]
pub(crate) struct CurrentExecution {
    /// Executed function.
    function: ExecutedFunction,
    /// Tasks to be awaken after the task finishes polling.
    tasks_to_be_awoken: HashSet<TaskId>,
    /// Tasks to be aborted after the task finishes polling.
    tasks_to_be_aborted: HashSet<TaskId>,
    /// Log of resource events.
    resource_events: Vec<ResourceEvent>,
}

impl CurrentExecution {
    fn new(function: ExecutedFunction) -> Self {
        Self {
            function,
            tasks_to_be_awoken: HashSet::new(),
            tasks_to_be_aborted: HashSet::new(),
            resource_events: Vec::new(),
        }
    }

    fn register_task(&mut self, task_id: TaskId) {
        self.resource_events.push(ResourceEvent {
            resource_id: ResourceId::Task(task_id),
            kind: ResourceEventKind::Created,
        });
    }

    fn register_task_drop(&mut self, task_id: TaskId) {
        if self.tasks_to_be_aborted.insert(task_id) {
            self.resource_events.push(ResourceEvent {
                resource_id: ResourceId::Task(task_id),
                kind: ResourceEventKind::Dropped,
            });
        }
    }

    fn register_timer(&mut self, timer_id: TimerId) {
        self.resource_events.push(ResourceEvent {
            resource_id: ResourceId::Timer(timer_id),
            kind: ResourceEventKind::Created,
        });
    }

    fn register_timer_drop(&mut self, timer_id: TimerId) {
        self.resource_events.push(ResourceEvent {
            resource_id: ResourceId::Timer(timer_id),
            kind: ResourceEventKind::Dropped,
        });
    }

    fn commit(self, state: &mut State) -> Vec<ResourceEvent> {
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

    fn revert(self, state: &mut State) -> Vec<ResourceEvent> {
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

#[derive(Debug)]
pub(crate) struct State {
    /// Functions exported by the `Instance`. Instantiated immediately after instance.
    exports: Option<ModuleExports>,

    inbound_channels: HashMap<String, InboundChannelState>,
    outbound_channels: HashMap<String, OutboundChannelState>,
    data_inputs: HashMap<String, Message>,
    timers: Timers,

    /// All tasks together with relevant info.
    tasks: HashMap<TaskId, TaskState>,
    /// Data related to the currently executing WASM call.
    current_execution: Option<CurrentExecution>,
    /// Tasks that should be polled after `current_task`.
    task_queue: TaskQueue,
    /// Wakers that need to be woken up.
    waker_queue: Vec<Wakers>,
    /// Wakeup cause set when waking up tasks.
    current_wakeup_cause: Option<WakeUpCause>,
}

impl State {
    pub fn from_interface<W>(
        interface: &Interface<W>,
        data_inputs: HashMap<String, Vec<u8>>,
    ) -> Self {
        // Sanity-check correspondence of inputs to the interface.
        debug_assert_eq!(
            data_inputs
                .keys()
                .map(String::as_str)
                .collect::<HashSet<_>>(),
            interface
                .data_inputs()
                .map(|(name, _)| name)
                .collect::<HashSet<_>>()
        );

        let inbound_channels = interface
            .inbound_channels()
            .map(|(name, _)| (name.to_owned(), InboundChannelState::default()))
            .collect();
        let outbound_channels = interface
            .outbound_channels()
            .map(|(name, _)| (name.to_owned(), OutboundChannelState::default()))
            .collect();
        let data_inputs = data_inputs
            .into_iter()
            .map(|(name, bytes)| (name, bytes.into()))
            .collect();

        Self {
            exports: None,
            inbound_channels,
            outbound_channels,
            data_inputs,
            timers: Timers::new(),
            tasks: HashMap::new(),
            current_execution: None,
            task_queue: TaskQueue::default(),
            waker_queue: Vec::new(),
            current_wakeup_cause: None,
        }
    }

    pub fn exports(&self) -> ModuleExports {
        self.exports
            .expect("exports accessed before `State` is fully initialized")
    }

    pub fn set_exports(&mut self, exports: ModuleExports) {
        self.exports = Some(exports);
    }

    pub fn set_current_execution(&mut self, function: ExecutedFunction) {
        self.current_execution = Some(CurrentExecution::new(function));
    }

    /// Returns tasks that need to be dropped.
    pub fn remove_current_execution(&mut self, revert: bool) -> Vec<ResourceEvent> {
        let current_execution = self.current_execution.take().expect("no current task");
        if revert {
            current_execution.revert(self)
        } else {
            current_execution.commit(self)
        }
    }

    pub fn complete_current_task(&mut self) {
        let current_execution = self.current_execution.as_ref().unwrap();
        let task_id = if let ExecutedFunction::Task { task_id, .. } = current_execution.function {
            task_id
        } else {
            unreachable!("`complete_current_task` called when task isn't executing")
        };
        self.complete_task(task_id, Ok(()));

        let current_execution = self.current_execution.as_mut().unwrap();
        current_execution.register_task_drop(task_id);
    }

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
                self.tasks
                    .get_mut(task)
                    .unwrap()
                    .wakes_on_completion
                    .insert(waker);
            }
        }
    }

    fn schedule_wakers(&mut self, wakers: HashSet<WakerId>, cause: WakeUpCause) {
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

    pub fn push_inbound_message(
        &mut self,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<(), ConsumeError> {
        let channel_state = match self.inbound_channels.get_mut(channel_name) {
            Some(state) => state,
            None => {
                return Err(ConsumeError {
                    channel_name: channel_name.to_owned(),
                    message,
                    kind: ConsumeErrorKind::UnknownChannel,
                });
            }
        };
        if channel_state.wakes_on_next_element.is_empty() {
            return Err(ConsumeError {
                channel_name: channel_name.to_owned(),
                message,
                kind: ConsumeErrorKind::NotListened,
            });
        }

        debug_assert!(
            channel_state.pending_message.is_none(),
            "Multiple messages inserted for inbound channel `{}`",
            channel_name
        );
        let message_index = channel_state.received_messages;
        channel_state.pending_message = Some(message.into());
        channel_state.received_messages += 1;

        let wakers = mem::take(&mut channel_state.wakes_on_next_element);
        self.schedule_wakers(
            wakers,
            WakeUpCause::InboundMessage {
                channel_name: channel_name.to_owned(),
                message_index,
            },
        );
        Ok(())
    }

    pub fn acquire_inbound_channel(&mut self, channel_name: &str) -> Result<(), ChannelErrorKind> {
        let channel_state = self
            .inbound_channels
            .get_mut(channel_name)
            .ok_or(ChannelErrorKind::Unknown)?;
        if mem::replace(&mut channel_state.is_acquired, true) {
            Err(ChannelErrorKind::AlreadyAcquired)
        } else {
            Ok(())
        }
    }

    fn inbound_channel(&mut self, channel_name: &str) -> Result<&mut InboundChannelState, Trap> {
        self.inbound_channels.get_mut(channel_name).ok_or_else(|| {
            let message = format!("No inbound channel `{}`", channel_name);
            Trap::new(message)
        })
    }

    pub fn has_outbound_channel(&self, channel_name: &str) -> bool {
        self.outbound_channels.contains_key(channel_name)
    }

    fn outbound_channel(&mut self, channel_name: &str) -> Result<&mut OutboundChannelState, Trap> {
        self.outbound_channels.get_mut(channel_name).ok_or_else(|| {
            let message = format!("No outbound channel `{}`", channel_name);
            Trap::new(message)
        })
    }

    pub fn outbound_message_indices(&self, channel_name: &str) -> Range<usize> {
        let channel_state = &self.outbound_channels[channel_name];
        let start = channel_state.flushed_messages;
        let end = start + channel_state.messages.len();
        start..end
    }

    pub fn push_outbound_message(
        &mut self,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<(), Trap> {
        let channel_state = self.outbound_channel(channel_name)?;
        channel_state.messages.push(message.into());
        Ok(())
    }

    pub fn poll_inbound_channel(
        &mut self,
        channel_name: &str,
        cx: &mut WasmContext,
    ) -> Result<PollMessage, Trap> {
        let channel_state = self.inbound_channel(channel_name)?;
        Ok(channel_state.poll_next().wake_if_pending(cx, || {
            WakerPlacement::InboundChannel(channel_name.to_owned())
        }))
    }

    pub fn poll_outbound_channel(
        &mut self,
        channel_name: &str,
        flush: bool,
        cx: &mut WasmContext,
    ) -> Result<Poll<()>, Trap> {
        let needs_flushing = self.needs_flushing();
        let channel_state = self.outbound_channel(channel_name)?;

        let poll_result = if needs_flushing || (flush && !channel_state.messages.is_empty()) {
            Poll::Pending
        } else {
            Poll::Ready(())
        };
        Ok(poll_result.wake_if_pending(cx, || {
            WakerPlacement::OutboundChannel(channel_name.to_owned())
        }))
    }

    pub fn data_input(&self, input_name: &str) -> Option<Vec<u8>> {
        self.data_inputs.get(input_name).map(|data| data.0.clone())
    }

    pub fn needs_flushing(&self) -> bool {
        self.outbound_channels
            .values()
            .any(|channel_state| !channel_state.wakes_on_flush.is_empty())
    }

    pub fn take_outbound_messages(&mut self, channel_name: &str) -> Vec<Vec<u8>> {
        let channel_state = self
            .outbound_channels
            .get_mut(channel_name)
            .unwrap_or_else(|| panic!("No outbound channel `{}`", channel_name));

        let wakers = mem::take(&mut channel_state.wakes_on_flush);
        let start_message_idx = channel_state.flushed_messages;
        let messages = mem::take(&mut channel_state.messages);
        channel_state.flushed_messages += messages.len();

        self.schedule_wakers(
            wakers,
            WakeUpCause::Flush {
                channel_name: channel_name.to_owned(),
                message_indexes: start_message_idx..(start_message_idx + messages.len()),
            },
        );
        messages.into_iter().map(Into::into).collect()
    }

    pub fn spawn_task(&mut self, task_id: TaskId, task_name: String) -> Result<(), Trap> {
        if self.tasks.contains_key(&task_id) {
            let message = format!("ABI misuse: task ID {} is reused", task_id);
            return Err(Trap::new(message));
        }

        let current_task = self.current_execution.as_mut().unwrap();
        current_task.register_task(task_id);
        let task_state = TaskState::new(task_name, current_task.function.clone());
        self.tasks.insert(task_id, task_state);
        Ok(())
    }

    pub fn spawn_main_task(&mut self, task_id: TaskId) {
        debug_assert!(self.tasks.is_empty());
        debug_assert!(self.task_queue.is_empty());
        debug_assert!(self.current_execution.is_none());

        let task_state = TaskState::new("_main".to_owned(), ExecutedFunction::Entry);
        self.tasks.insert(task_id, task_state);
        self.task_queue.insert_task(
            task_id,
            &WakeUpCause::Spawned(Box::new(ExecutedFunction::Entry)),
        );
    }

    pub fn poll_task_completion(
        &mut self,
        task: TaskId,
        cx: &mut WasmContext,
    ) -> Poll<Result<(), JoinError>> {
        let poll_result = if let Some(result) = &self.tasks[&task].completion_result {
            Poll::Ready(result.as_ref().copied().map_err(JoinError::clone))
        } else {
            Poll::Pending
        };
        poll_result.wake_if_pending(cx, || WakerPlacement::TaskCompletion(task))
    }

    pub fn schedule_task_wakeup(&mut self, task_id: TaskId) -> Result<(), Trap> {
        if !self.tasks.contains_key(&task_id) {
            let message = format!("unknown task ID {} scheduled for wakeup", task_id);
            return Err(Trap::new(message));
        }

        if let Some(current_task) = &mut self.current_execution {
            current_task.tasks_to_be_awoken.insert(task_id);
        } else {
            let cause = self
                .current_wakeup_cause
                .as_ref()
                .expect("cannot determine wakeup cause");
            self.task_queue.insert_task(task_id, cause);
        }
        Ok(())
    }

    pub fn schedule_task_abortion(&mut self, task_id: TaskId) -> Result<(), Trap> {
        if !self.tasks.contains_key(&task_id) {
            let message = format!("unknown task {} scheduled for abortion", task_id);
            return Err(Trap::new(message));
        }

        let current_task = self
            .current_execution
            .as_mut()
            .expect("task aborted outside event loop");
        current_task.tasks_to_be_aborted.insert(task_id);
        Ok(())
    }

    pub fn queued_tasks(&self) -> impl Iterator<Item = (TaskId, &WakeUpCause)> + '_ {
        self.task_queue
            .inner
            .iter()
            .map(|(task, cause)| (*task, cause))
    }

    pub fn take_next_task(&mut self) -> Option<(TaskId, WakeUpCause)> {
        loop {
            let (task, wake_up_cause) = self.task_queue.take_task()?;
            if !self.tasks[&task].is_completed() {
                return Some((task, wake_up_cause));
            }
        }
    }

    pub fn complete_task(&mut self, task_id: TaskId, result: Result<(), JoinError>) {
        let result = crate::log_result!(result, "Completed task {}", task_id);
        let task_state = self.tasks.get_mut(&task_id).unwrap();
        task_state.completion_result = Some(result);

        let wakers = mem::take(&mut task_state.wakes_on_completion);
        self.schedule_wakers(wakers, WakeUpCause::CompletedTask(task_id));
    }

    pub fn timers(&self) -> &Timers {
        &self.timers
    }

    pub fn create_timer(&mut self, name: String, definition: Timer) -> TimerId {
        let timer_id = self.timers.insert(name, definition);
        let current_task = self.current_execution.as_mut().unwrap();
        current_task.register_timer(timer_id);
        timer_id
    }

    pub fn drop_timer(&mut self, timer_id: TimerId) -> Result<(), Trap> {
        if self.timers.get(timer_id).is_none() {
            let message = format!("Timer ID {} is not defined", timer_id);
            return Err(Trap::new(message));
        }
        let current_task = self.current_execution.as_mut().unwrap();
        current_task.register_timer_drop(timer_id);
        Ok(())
    }

    pub fn set_current_time(&mut self, time: DateTime<Utc>) {
        let wakers_by_timer = self.timers.set_current_time(time);
        for (id, wakers) in wakers_by_timer {
            let cause = WakeUpCause::Timer { id };
            crate::trace!("Scheduled wakers {:?} with cause {:?}", wakers, cause);
            self.waker_queue.push(Wakers::new(wakers, cause));
        }
    }

    pub fn poll_timer(
        &mut self,
        timer_id: TimerId,
        cx: &mut WasmContext,
    ) -> Result<Poll<()>, Trap> {
        let poll_result = self.timers.poll(timer_id)?;
        Ok(poll_result.wake_if_pending(cx, || WakerPlacement::Timer(timer_id)))
    }
}
