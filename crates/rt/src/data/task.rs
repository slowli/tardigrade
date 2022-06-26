//! Functionality to manage tasks.

use wasmtime::{StoreContextMut, Trap};

use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem,
    task::Poll,
};

use super::{
    helpers::{CurrentExecution, WakeIfPending, WakerPlacement, WasmContext, WasmContextPtr},
    WorkflowData, WorkflowFunctions,
};
use crate::{
    receipt::{Event, ExecutedFunction, ResourceEventKind, ResourceId, WakeUpCause},
    utils::{copy_string_from_wasm, drop_value, WasmAllocator},
    TaskId, WakerId,
};
use tardigrade_shared::{abi::IntoWasm, JoinError};

/// Priority queue for tasks.
#[derive(Debug, Default)]
pub(super) struct TaskQueue {
    inner: VecDeque<TaskId>,
    causes: HashMap<TaskId, WakeUpCause>,
}

impl TaskQueue {
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn insert_task(&mut self, task: TaskId, cause: &WakeUpCause) {
        self.causes.entry(task).or_insert_with(|| {
            self.inner.push_back(task);
            cause.clone()
        });
    }

    fn take_task(&mut self) -> Option<(TaskId, WakeUpCause)> {
        let key = self.inner.pop_front()?;
        let value = self.causes.remove(&key).unwrap();
        Some((key, value))
    }
}

#[derive(Debug, Clone)]
pub struct TaskState {
    name: String,
    completion_result: Poll<Result<(), JoinError>>,
    spawned_by: ExecutedFunction,
    wakes_on_completion: HashSet<WakerId>,
}

impl TaskState {
    fn new(name: String, spawned_by: ExecutedFunction) -> Self {
        Self {
            name,
            completion_result: Poll::Pending,
            spawned_by,
            wakes_on_completion: HashSet::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn spawned_by(&self) -> &ExecutedFunction {
        &self.spawned_by
    }

    pub fn result(&self) -> Poll<Result<(), &JoinError>> {
        match &self.completion_result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => Poll::Ready(res.as_ref().copied()),
        }
    }

    pub(super) fn insert_waker(&mut self, waker_id: WakerId) {
        self.wakes_on_completion.insert(waker_id);
    }
}

impl WorkflowData {
    pub(super) fn current_execution(&mut self) -> &mut CurrentExecution {
        self.current_execution
            .as_mut()
            .expect("called outside event loop")
    }

    pub(crate) fn set_current_execution(&mut self, function: ExecutedFunction) {
        self.current_execution = Some(CurrentExecution::new(function));
    }

    /// Returns tasks that need to be dropped.
    pub(crate) fn remove_current_execution(&mut self, revert: bool) -> Vec<Event> {
        let current_execution = self.current_execution.take().unwrap();
        if revert {
            current_execution.revert(self)
        } else {
            current_execution.commit(self)
        }
    }

    pub(crate) fn complete_current_task(&mut self) {
        let current_execution = self.current_execution();
        let task_id = if let ExecutedFunction::Task { task_id, .. } = current_execution.function {
            task_id
        } else {
            unreachable!("`complete_current_task` called when task isn't executing")
        };
        self.complete_task(task_id, Ok(()));
        self.current_execution()
            .push_resource_event(ResourceId::Task(task_id), ResourceEventKind::Dropped);
    }

    fn spawn_task(&mut self, task_id: TaskId, task_name: String) -> Result<(), Trap> {
        if self.tasks.contains_key(&task_id) {
            let message = format!("ABI misuse: task ID {} is reused", task_id);
            return Err(Trap::new(message));
        }

        let execution = self.current_execution();
        execution.push_resource_event(ResourceId::Task(task_id), ResourceEventKind::Created);
        let task_state = TaskState::new(task_name, execution.function.clone());
        self.tasks.insert(task_id, task_state);
        Ok(())
    }

    pub(crate) fn spawn_main_task(&mut self, task_id: TaskId) {
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

    fn poll_task_completion(
        &mut self,
        task_id: TaskId,
        cx: &mut WasmContext,
    ) -> Poll<Result<(), JoinError>> {
        let poll_result = self.tasks[&task_id].result().map_err(JoinError::clone);
        let empty_result = drop_value(&poll_result);
        self.current_execution().push_resource_event(
            ResourceId::Task(task_id),
            ResourceEventKind::Polled(empty_result),
        );
        poll_result.wake_if_pending(cx, || WakerPlacement::TaskCompletion(task_id))
    }

    fn schedule_task_wakeup(&mut self, task_id: TaskId) -> Result<(), Trap> {
        if !self.tasks.contains_key(&task_id) {
            let message = format!("unknown task ID {} scheduled for wakeup", task_id);
            return Err(Trap::new(message));
        }

        if let Some(current_task) = &mut self.current_execution {
            current_task.register_task_wakeup(task_id);
        } else {
            let cause = self
                .current_wakeup_cause
                .as_ref()
                .expect("cannot determine wakeup cause");
            self.task_queue.insert_task(task_id, cause);
        }
        Ok(())
    }

    fn schedule_task_abortion(&mut self, task_id: TaskId) -> Result<(), Trap> {
        if !self.tasks.contains_key(&task_id) {
            let message = format!("unknown task {} scheduled for abortion", task_id);
            return Err(Trap::new(message));
        }
        self.current_execution()
            .push_resource_event(ResourceId::Task(task_id), ResourceEventKind::Dropped);
        Ok(())
    }

    pub(crate) fn task(&self, task_id: TaskId) -> Option<&TaskState> {
        self.tasks.get(&task_id)
    }

    pub(crate) fn tasks(&self) -> impl Iterator<Item = (TaskId, &TaskState)> + '_ {
        self.tasks.iter().map(|(id, state)| (*id, state))
    }

    pub(crate) fn take_next_task(&mut self) -> Option<(TaskId, WakeUpCause)> {
        loop {
            let (task, wake_up_cause) = self.task_queue.take_task()?;
            if self.tasks[&task].completion_result.is_pending() {
                return Some((task, wake_up_cause));
            }
        }
    }

    pub(crate) fn complete_task(&mut self, task_id: TaskId, result: Result<(), JoinError>) {
        let result = crate::log_result!(result, "Completed task {}", task_id);
        let task_state = self.tasks.get_mut(&task_id).unwrap();
        task_state.completion_result = Poll::Ready(result);
        let wakers = mem::take(&mut task_state.wakes_on_completion);
        self.schedule_wakers(wakers, WakeUpCause::CompletedTask(task_id));
    }
}

/// Task-related functions exported to WASM.
impl WorkflowFunctions {
    pub fn poll_task_completion(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        task_id: TaskId,
        poll_cx: WasmContextPtr,
    ) -> Result<i64, Trap> {
        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx.data_mut().poll_task_completion(task_id, &mut poll_cx);
        crate::trace!(
            "Polled completion for task {} with context {:?}: {:?}",
            task_id,
            poll_cx,
            poll_result
        );
        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn spawn_task(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        task_name_ptr: u32,
        task_name_len: u32,
        task_id: TaskId,
    ) -> Result<(), Trap> {
        let memory = ctx.data().exports().memory;
        let task_name = copy_string_from_wasm(&ctx, &memory, task_name_ptr, task_name_len)?;
        let result = ctx.data_mut().spawn_task(task_id, task_name.clone());
        crate::log_result!(result, "Spawned task {} with name `{}`", task_id, task_name)
    }

    pub fn wake_task(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        task_id: TaskId,
    ) -> Result<(), Trap> {
        let result = ctx.data_mut().schedule_task_wakeup(task_id);
        crate::log_result!(result, "Scheduled task {} wakeup", task_id)
    }

    pub fn schedule_task_abortion(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        task_id: TaskId,
    ) -> Result<(), Trap> {
        let result = ctx.data_mut().schedule_task_abortion(task_id);
        crate::log_result!(result, "Scheduled task {} to be aborted", task_id)
    }
}
