//! Functionality to manage tasks.

use wasmtime::Trap;

use std::{
    collections::{HashMap, HashSet},
    mem,
    task::Poll,
};

use super::{
    helpers::{CurrentExecution, WakeIfPending, WakerPlacement, WasmContext},
    State,
};
use crate::{ExecutedFunction, ResourceEvent, TaskId, WakeUpCause, WakerId};
use tardigrade_shared::JoinError;

/// Priority queue for tasks.
#[derive(Debug, Default)]
pub(super) struct TaskQueue {
    inner: HashMap<TaskId, WakeUpCause>,
}

impl TaskQueue {
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn insert_task(&mut self, task: TaskId, cause: &WakeUpCause) {
        self.inner.entry(task).or_insert_with(|| cause.clone());
    }

    fn take_task(&mut self) -> Option<(TaskId, WakeUpCause)> {
        let key = *self.inner.keys().next()?;
        let value = self.inner.remove(&key)?;
        Some((key, value))
    }
}

#[derive(Debug, Clone)]
pub(super) struct TaskState {
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

    pub fn insert_waker(&mut self, waker_id: WakerId) {
        self.wakes_on_completion.insert(waker_id);
    }
}

impl State {
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

    pub fn schedule_task_abortion(&mut self, task_id: TaskId) -> Result<(), Trap> {
        if !self.tasks.contains_key(&task_id) {
            let message = format!("unknown task {} scheduled for abortion", task_id);
            return Err(Trap::new(message));
        }

        let current_task = self
            .current_execution
            .as_mut()
            .expect("task aborted outside event loop");
        current_task.register_task_drop(task_id);
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
}
