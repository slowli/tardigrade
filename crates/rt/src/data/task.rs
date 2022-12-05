//! Functionality to manage tasks.

use anyhow::anyhow;
use futures::future::Aborted;
use serde::{Deserialize, Serialize};

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt, mem,
    task::Poll,
};

use super::{
    helpers::{CurrentExecution, WakerPlacement, Wakers, WorkflowPoll},
    PersistedWorkflowData, WorkflowData,
};
use crate::{
    receipt::{Event, ExecutedFunction, PanicInfo, ResourceEventKind, ResourceId, WakeUpCause},
    utils,
};
use tardigrade::{
    task::{JoinError, TaskResult},
    TaskId, WakerId,
};

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

    fn clear(&mut self) {
        self.inner.clear();
        self.causes.clear();
    }

    fn take_task(&mut self) -> Option<(TaskId, WakeUpCause)> {
        let key = self.inner.pop_front()?;
        let value = self.causes.remove(&key).unwrap();
        Some((key, value))
    }

    fn drain(&mut self) -> impl Iterator<Item = (TaskId, WakeUpCause)> {
        self.inner.clear();
        mem::take(&mut self.causes).into_iter()
    }
}

/// State of a task within a workflow.
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskState {
    name: String,
    #[serde(with = "utils::serde_poll_res")]
    completion_result: Poll<Result<(), JoinError>>,
    spawned_by: Option<TaskId>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    wakes_on_completion: HashSet<WakerId>,
}

impl Clone for TaskState {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            completion_result: utils::clone_completion_result(&self.completion_result),
            spawned_by: self.spawned_by,
            wakes_on_completion: self.wakes_on_completion.clone(),
        }
    }
}

impl TaskState {
    fn new(name: String, spawned_by: Option<TaskId>) -> Self {
        Self {
            name,
            completion_result: Poll::Pending,
            spawned_by,
            wakes_on_completion: HashSet::new(),
        }
    }

    /// Returns human-readable task name that was specified when spawning the task.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns ID of a task that has spawned this task. If this task was not spawned by
    /// a task, returns `None`.
    pub fn spawned_by(&self) -> Option<TaskId> {
        self.spawned_by
    }

    /// Returns the current poll state of this task.
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

impl PersistedWorkflowData {
    pub(crate) fn task(&self, task_id: TaskId) -> Option<&TaskState> {
        self.tasks.get(&task_id)
    }

    pub(crate) fn main_task(&self) -> Option<&TaskState> {
        debug_assert_eq!(
            self.tasks
                .values()
                .filter(|state| state.spawned_by.is_none())
                .count(),
            1,
            "Cannot find main task in workflow: {:#?}",
            self
        );
        self.tasks.values().find(|state| state.spawned_by.is_none())
    }

    pub(crate) fn tasks(&self) -> impl Iterator<Item = (TaskId, &TaskState)> + '_ {
        self.tasks.iter().map(|(id, state)| (*id, state))
    }

    #[tracing::instrument(level = "debug")]
    pub(crate) fn complete_task(&mut self, task_id: TaskId, result: Result<(), JoinError>) {
        let task_state = self.tasks.get_mut(&task_id).unwrap();
        if task_state.completion_result.is_ready() {
            // Task is already completed.
            return;
        }
        task_state.completion_result = Poll::Ready(result);
        let wakers = mem::take(&mut task_state.wakes_on_completion);
        self.schedule_wakers(wakers, WakeUpCause::CompletedTask(task_id));
    }

    pub(crate) fn result(&self) -> Poll<Result<(), &JoinError>> {
        self.main_task().map_or(Poll::Pending, TaskState::result)
    }

    pub(crate) fn abort(&mut self) {
        let main_task = self
            .tasks
            .values_mut()
            .find(|state| state.spawned_by.is_none());
        let main_task = if let Some(task) = main_task {
            task
        } else {
            // Create a surrogate main task (may be necessary if the workflow is aborted
            // before initialization, e.g., if it panics in init code).
            self.tasks
                .entry(0)
                .or_insert_with(|| TaskState::new("_main".to_owned(), None))
        };
        main_task.completion_result = Poll::Ready(Err(JoinError::Aborted));
    }
}

/// Handle allowing to manipulate a workflow task.
pub struct TaskActions<'a> {
    data: &'a mut WorkflowData,
    id: TaskId,
}

impl fmt::Debug for TaskActions<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskActions")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl TaskActions<'_> {
    /// Polls this task for completion.
    #[tracing::instrument(level = "debug", ret)]
    pub fn poll_completion(&mut self) -> WorkflowPoll<Result<(), Aborted>> {
        let task_state = &self.data.persisted.tasks[&self.id];
        let poll_result = task_state.result().map(utils::extract_task_poll_result);
        let empty_result = utils::drop_value(&poll_result);
        self.data.current_execution().push_resource_event(
            ResourceId::Task(self.id),
            ResourceEventKind::Polled(empty_result),
        );
        WorkflowPoll::new(poll_result, WakerPlacement::TaskCompletion(self.id))
    }

    /// Schedules wakeup of this task.
    #[tracing::instrument(level = "debug")]
    pub fn schedule_wakeup(&mut self) {
        if let Some(current_task) = &mut self.data.current_execution {
            current_task.register_task_wakeup(self.id);
        } else {
            let cause = self
                .data
                .current_wakeup_cause
                .as_ref()
                .expect("cannot determine wakeup cause");
            self.data.task_queue.insert_task(self.id, cause);
        }
    }

    /// Schedules this task to be aborted.
    #[tracing::instrument(level = "debug")]
    pub fn schedule_abortion(&mut self) {
        self.data
            .current_execution()
            .push_resource_event(ResourceId::Task(self.id), ResourceEventKind::Dropped);
    }
}

/// Task-related functionality.
impl WorkflowData {
    pub(crate) fn result(&self) -> Poll<Result<(), &JoinError>> {
        self.persisted.result()
    }

    pub(super) fn current_execution(&mut self) -> &mut CurrentExecution {
        self.current_execution
            .as_mut()
            .expect("called outside event loop")
    }

    pub(crate) fn set_current_execution(&mut self, function: &ExecutedFunction) {
        self.current_execution = Some(CurrentExecution::new(function));
    }

    /// Returns tasks that need to be dropped, and panic info, if any.
    pub(crate) fn remove_current_execution(
        &mut self,
        revert: bool,
    ) -> (Vec<Event>, Option<PanicInfo>) {
        let current_execution = self.current_execution.take().unwrap();
        if revert {
            current_execution.revert(self)
        } else {
            (current_execution.commit(self), None)
        }
    }

    /// Returns the result of task completion.
    pub(crate) fn complete_current_task(&mut self) -> TaskResult {
        let task_id = self
            .current_execution()
            .task_id
            .expect("`complete_current_task` called when task isn't executing");
        let result: TaskResult = self
            .current_execution()
            .take_task_error()
            .map_or(Ok(()), Err);

        let join_result = result
            .as_ref()
            .copied()
            .map_err(|err| JoinError::Err(err.clone_boxed()));
        self.persisted.complete_task(task_id, join_result);
        self.current_execution()
            .push_resource_event(ResourceId::Task(task_id), ResourceEventKind::Dropped);
        result
    }

    /// Records a spawned task.
    #[tracing::instrument(level = "debug", skip(self), err)]
    pub fn spawn_task(&mut self, task_id: TaskId, task_name: String) -> anyhow::Result<()> {
        if self.persisted.tasks.contains_key(&task_id) {
            let err = anyhow!("ABI misuse: task ID {task_id} is reused");
            return Err(err);
        }

        let execution = self.current_execution();
        execution.push_resource_event(ResourceId::Task(task_id), ResourceEventKind::Created);
        let task_state = TaskState::new(task_name, execution.task_id);
        self.persisted.tasks.insert(task_id, task_state);
        Ok(())
    }

    /// Returns an action handle for the task with the specified ID.
    ///
    /// # Panics
    ///
    /// Panics if the task with `id` does not exist in the workflow.
    pub fn task(&mut self, id: TaskId) -> TaskActions<'_> {
        assert!(self.persisted.tasks.contains_key(&id), "task not found");
        TaskActions { data: self, id }
    }

    pub(crate) fn spawn_main_task(&mut self, task_id: TaskId) {
        // Patch task ID mentions in other tasks (if any). This may be necessary
        // if any initialization code is executed before the main task is created.
        for state in self.persisted.tasks.values_mut() {
            debug_assert_eq!(state.spawned_by, None);
            state.spawned_by = Some(task_id);
        }

        let task_state = TaskState::new("_main".to_owned(), None);
        self.persisted.tasks.insert(task_id, task_state);
        self.task_queue.insert_task(task_id, &WakeUpCause::Spawned);
    }

    pub(crate) fn enqueue_task(&mut self, id: TaskId, cause: &WakeUpCause) {
        self.task_queue.insert_task(id, cause);
    }

    pub(crate) fn take_next_task(&mut self) -> Option<(TaskId, WakeUpCause)> {
        loop {
            let (task, wake_up_cause) = self.task_queue.take_task()?;
            if self.persisted.tasks[&task].completion_result.is_pending() {
                return Some((task, wake_up_cause));
            }
        }
    }

    pub(crate) fn clear_task_queue(&mut self) {
        self.task_queue.clear();
    }

    pub(crate) fn move_task_queue_to_wakers(&mut self) {
        for (task_id, cause) in self.task_queue.drain() {
            self.persisted
                .waker_queue
                .push(Wakers::from_task(task_id, cause));
        }
    }
}
