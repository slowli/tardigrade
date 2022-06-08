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
    time::{TimerId, Timers},
    Execution, Receipt, TaskId,
};
use tardigrade_shared::{workflow::Interface, ChannelErrorKind, JoinError, PollMessage};

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
            let create_waker = caller.data().exports().create_waker;
            let waker_id = create_waker.call(&mut *caller, self.ptr)?;
            dbg!(waker_id);
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
pub(crate) struct Wakers {
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

    pub fn wake_all(self, store: &mut Store<State>, receipt: &mut Receipt) {
        let wake_waker = store.data().exports().wake_waker;
        store.data_mut().current_wakeup_cause = Some(self.cause);
        for waker_id in self.inner {
            let result = wake_waker.call(&mut *store, waker_id);
            receipt
                .executions
                .push(Execution::Waker { waker_id, result });
        }
        store.data_mut().current_wakeup_cause = None;
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
    Spawned(Option<TaskId>),
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
    pending_message: Option<Vec<u8>>,
    wakes_on_next_element: HashSet<WakerId>,
}

impl InboundChannelState {
    fn poll_next(&mut self) -> Poll<Option<Vec<u8>>> {
        if let Some(message) = self.pending_message.take() {
            Poll::Ready(Some(message))
        } else {
            Poll::Pending
        }
    }
}

#[derive(Debug, Default)]
struct OutboundChannelState {
    flushed_messages: usize,
    messages: Vec<Vec<u8>>,
    wakes_on_flush: HashSet<WakerId>,
}

#[derive(Debug, Clone)]
struct TaskState {
    _name: String,
    completion_result: Option<Result<(), JoinError>>,
    _spawned_by: Option<TaskId>,
    wakes_on_completion: HashSet<WakerId>,
}

impl TaskState {
    fn new(name: String, spawned_by: Option<TaskId>) -> Self {
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
pub(crate) struct CurrentTask {
    /// ID of the current task.
    task_id: TaskId,
    /// Newly spawned tasks.
    spawned_tasks: HashSet<TaskId>,
    /// Tasks to be awaken after the task finishes polling.
    tasks_to_be_awoken: HashSet<TaskId>,
    /// Tasks to be aborted after the task finishes polling.
    tasks_to_be_aborted: HashSet<TaskId>,
}

impl CurrentTask {
    fn new(task_id: TaskId) -> Self {
        Self {
            task_id,
            spawned_tasks: HashSet::new(),
            tasks_to_be_awoken: HashSet::new(),
            tasks_to_be_aborted: HashSet::new(),
        }
    }

    /// Returns aborted tasks.
    fn commit(self, state: &mut State) -> Vec<TaskId> {
        for task_id in self.spawned_tasks {
            let cause = WakeUpCause::Spawned(Some(self.task_id));
            state.task_queue.insert_task(task_id, &cause);
        }
        for task_id in self.tasks_to_be_awoken {
            state
                .task_queue
                .insert_task(task_id, &WakeUpCause::Task(self.task_id));
        }

        let mut aborted_tasks = Vec::with_capacity(self.tasks_to_be_aborted.len());
        for task_id in &self.tasks_to_be_aborted {
            let task_state = state.tasks.get_mut(task_id).unwrap();
            if task_state.completion_result.is_none() {
                task_state.completion_result = Some(Err(JoinError::Aborted));
                let wakers = mem::take(&mut task_state.wakes_on_completion);
                state.schedule_wakers(wakers, WakeUpCause::CompletedTask(*task_id));
                aborted_tasks.push(*task_id);
            }
        }
        aborted_tasks
    }

    fn revert(self, state: &mut State) {
        for task_id in self.spawned_tasks {
            state.tasks.remove(&task_id);
            // Since new tasks can only be mentioned in `self.tasks_to_be_awoken`, not in
            // `state.task_queue`, cleaning up the queue is not needed.
        }
    }
}

#[derive(Debug)]
pub(crate) struct State {
    /// Functions exported by the `Instance`. Instantiated immediately after instance.
    exports: Option<ModuleExports>,

    inbound_channels: HashMap<String, InboundChannelState>,
    outbound_channels: HashMap<String, OutboundChannelState>,
    data_inputs: HashMap<String, Vec<u8>>,
    timers: Timers,

    /// All tasks together with relevant info.
    tasks: HashMap<TaskId, TaskState>,
    /// Currently polled task.
    current_task: Option<CurrentTask>,
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

        Self {
            exports: None,
            inbound_channels,
            outbound_channels,
            data_inputs,
            timers: Timers::new(),
            tasks: HashMap::new(),
            current_task: None,
            task_queue: TaskQueue::default(),
            waker_queue: Vec::new(),
            current_wakeup_cause: None,
        }
    }

    pub fn exports(&self) -> &ModuleExports {
        self.exports
            .as_ref()
            .expect("exports accessed before `State` is fully initialized")
    }

    pub fn set_exports(&mut self, exports: ModuleExports) {
        self.exports = Some(exports);
    }

    pub fn set_current_task(&mut self, task_id: TaskId) {
        self.current_task = Some(CurrentTask::new(task_id));
    }

    /// Returns tasks that need to be dropped.
    pub fn remove_current_task(&mut self, poll_result: Result<Poll<()>, &Trap>) -> Vec<TaskId> {
        let current_task = self.current_task.take().expect("no current task");
        let current_task_id = current_task.task_id;
        let mut dropped_tasks = Vec::new();
        if poll_result.is_ok() {
            dropped_tasks = current_task.commit(self);
        } else {
            // TODO: is dropping tasks still relevant in this case? (Probably not.)
            current_task.revert(self);
        }

        match poll_result {
            Ok(Poll::Pending) => { /* Nothing to do here */ }
            Ok(Poll::Ready(())) => {
                self.complete_task(current_task_id, Ok(()));
                dropped_tasks.push(current_task_id);
            }
            Err(_) => {
                self.complete_task(current_task_id, Err(JoinError::Trapped));
                dropped_tasks.push(current_task_id);
            }
        }

        debug_assert_eq!(
            dropped_tasks.iter().collect::<HashSet<_>>().len(),
            dropped_tasks.len(),
            "Dropped tasks contain duplicates: {:?}",
            dropped_tasks
        );
        dropped_tasks
    }

    fn place_waker(&mut self, placement: &WakerPlacement, waker: WakerId) {
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
        self.waker_queue.push(Wakers::new(wakers, cause));
    }

    pub fn take_wakers(&mut self) -> Vec<Wakers> {
        mem::take(&mut self.waker_queue)
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
        channel_state.pending_message = Some(message);
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

    pub fn push_outbound_message(
        &mut self,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<(), Trap> {
        let channel_state = self.outbound_channel(channel_name)?;
        channel_state.messages.push(message);
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
        self.data_inputs.get(input_name).cloned()
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
        messages
    }

    pub fn spawn_task(&mut self, task_id: TaskId, task_name: String) -> Result<(), Trap> {
        if self.tasks.contains_key(&task_id) {
            let message = format!("ABI misuse: task ID {} is reused", task_id);
            return Err(Trap::new(message));
        }

        let current_task = self
            .current_task
            .as_mut()
            .expect("task spawned outside event loop");
        current_task.spawned_tasks.insert(task_id);
        let task_state = TaskState::new(task_name, Some(current_task.task_id));
        self.tasks.insert(task_id, task_state);
        Ok(())
    }

    pub fn spawn_main_task(&mut self, task_id: TaskId) {
        debug_assert!(self.tasks.is_empty());
        debug_assert!(self.task_queue.is_empty());
        debug_assert!(self.current_task.is_none());

        let task_state = TaskState::new("_main".to_owned(), None);
        self.tasks.insert(task_id, task_state);
        self.task_queue
            .insert_task(task_id, &WakeUpCause::Spawned(None));
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

        if let Some(current_task) = &mut self.current_task {
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
            .current_task
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

    pub fn complete_task(&mut self, task: TaskId, result: Result<(), JoinError>) {
        let task_state = self.tasks.get_mut(&task).unwrap();
        task_state.completion_result = Some(result);
        let wakers = mem::take(&mut task_state.wakes_on_completion);
        self.schedule_wakers(wakers, WakeUpCause::CompletedTask(task));
    }

    pub fn timers(&self) -> &Timers {
        &self.timers
    }

    pub fn timers_mut(&mut self) -> &mut Timers {
        &mut self.timers
    }

    pub fn set_current_time(&mut self, time: DateTime<Utc>) {
        let wakers_by_timer = self.timers.set_current_time(time);
        for (id, wakers) in wakers_by_timer {
            self.waker_queue
                .push(Wakers::new(wakers, WakeUpCause::Timer { id }));
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
