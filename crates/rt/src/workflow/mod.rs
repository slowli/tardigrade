//! `Workflow` and tightly related types.

use anyhow::Context;

use std::{collections::HashMap, mem};

mod persistence;

pub use self::persistence::PersistedWorkflow;

use crate::{
    data::{WakerOrTask, WorkflowData},
    engine::{DefineWorkflow, PersistWorkflow, RunWorkflow},
    manager::Services,
    receipt::{
        Event, ExecutedFunction, Execution, ExecutionError, Receipt, ResourceEventKind, ResourceId,
        WakeUpCause,
    },
    utils::Message,
};
use tardigrade::{
    interface::HandleMap, spawn::HostError, task::TaskResult, ChannelId, TaskId, WorkflowId,
};

#[derive(Debug, Default)]
struct ExecutionOutput {
    main_task_id: Option<TaskId>,
    task_result: Option<TaskResult>,
}

pub(crate) type ChannelIds = HandleMap<ChannelId>;

#[derive(Debug)]
pub(crate) struct WorkflowAndChannelIds {
    pub workflow_id: WorkflowId,
    pub channel_ids: ChannelIds,
}

#[derive(Debug)]
enum ExecutedFunctionArgs<'a> {
    WorkflowArgs(&'a [u8]),
    NewChannel {
        local_id: ChannelId,
        id: ChannelId,
    },
    NewChild {
        local_id: WorkflowId,
        result: Result<WorkflowAndChannelIds, HostError>,
    },
}

/// Workflow instance.
#[derive(Debug)]
pub(crate) struct Workflow<T> {
    inner: T,
    args: Option<Message>,
}

impl<T: RunWorkflow> Workflow<T> {
    pub(crate) fn new<D>(
        definition: &D,
        data: WorkflowData,
        args: Option<Message>,
    ) -> anyhow::Result<Self>
    where
        D: DefineWorkflow<Instance = T>,
    {
        let inner = definition
            .create_workflow(data)
            .context("failed creating workflow")?;
        Ok(Self { inner, args })
    }

    #[cfg(test)]
    pub(crate) fn data(&self) -> &WorkflowData {
        self.inner.data()
    }

    #[cfg(test)]
    pub(crate) fn data_mut(&mut self) -> &mut WorkflowData {
        self.inner.data_mut()
    }

    pub(crate) fn is_initialized(&self) -> bool {
        self.args.is_none()
    }

    pub(crate) fn initialize(&mut self) -> Result<Receipt, ExecutionError> {
        let raw_args = self.args.take().expect("workflow is already initialized");
        let mut spawn_receipt = self.spawn_main_task(raw_args.as_ref())?;
        let tick_receipt = self.tick()?;
        spawn_receipt.extend(tick_receipt);
        Ok(spawn_receipt)
    }

    fn spawn_main_task(&mut self, raw_args: &[u8]) -> Result<Receipt, ExecutionError> {
        let args = ExecutedFunctionArgs::WorkflowArgs(raw_args);
        let mut receipt = Receipt::default();
        let task_id = self
            .execute(ExecutedFunction::Entry, Some(args), &mut receipt)?
            .main_task_id
            .expect("main task ID not set");
        self.inner.data_mut().spawn_main_task(task_id);
        Ok(receipt)
    }

    fn do_execute(
        &mut self,
        function: &ExecutedFunction,
        args: Option<ExecutedFunctionArgs<'_>>,
    ) -> anyhow::Result<ExecutionOutput> {
        let mut output = ExecutionOutput::default();
        match function {
            ExecutedFunction::Task { task_id, .. } => {
                let exec_result = self.inner.poll_task(*task_id);
                exec_result.map(|poll| {
                    if poll.is_ready() {
                        output.task_result = Some(self.inner.data_mut().complete_current_task());
                    }
                })?;
            }
            ExecutedFunction::Waker {
                waker_id,
                wake_up_cause,
            } => {
                WorkflowData::wake(&mut self.inner, wake_up_cause.clone(), |workflow| {
                    workflow.wake_waker(*waker_id)
                })?;
            }
            ExecutedFunction::TaskDrop { task_id } => {
                self.inner.drop_task(*task_id)?;
            }
            ExecutedFunction::Entry => {
                let Some(ExecutedFunctionArgs::WorkflowArgs(args)) = args else { unreachable!() };
                output.main_task_id = Some(self.inner.create_main_task(args)?);
            }

            ExecutedFunction::StubInitialization => {
                let cause = WakeUpCause::StubInitialized;
                match args.unwrap() {
                    ExecutedFunctionArgs::NewChannel { local_id, id } => {
                        self.inner.data_mut().notify_on_channel_init(local_id, id);
                        WorkflowData::wake(&mut self.inner, cause, |workflow| {
                            workflow.initialize_channel(local_id, Ok(id));
                        });
                    }
                    ExecutedFunctionArgs::NewChild { local_id, result } => {
                        let narrow_result = result
                            .as_ref()
                            .map(|ids| ids.workflow_id)
                            .map_err(HostError::clone);
                        self.inner.data_mut().notify_on_child_init(local_id, result);
                        WorkflowData::wake(&mut self.inner, cause, |workflow| {
                            workflow.initialize_child(local_id, narrow_result);
                        });
                    }
                    ExecutedFunctionArgs::WorkflowArgs(_) => unreachable!(),
                }
            }
        }
        Ok(output)
    }

    fn execute(
        &mut self,
        function: ExecutedFunction,
        args: Option<ExecutedFunctionArgs<'_>>,
        receipt: &mut Receipt,
    ) -> Result<ExecutionOutput, ExecutionError> {
        self.inner.data_mut().set_current_execution(&function);

        let mut output = self.do_execute(&function, args);
        let (mut events, panic_info) = self
            .inner
            .data_mut()
            .remove_current_execution(output.is_err());
        let dropped_tasks = Self::dropped_tasks(&events);
        let task_result = output
            .as_mut()
            .map_or(None, |output| output.task_result.take());

        // Compress initialization executions: the fact that they are executed separately
        // is an implementation detail.
        let mut is_compressed = false;
        if function.is_stub_initialization() {
            debug_assert!(task_result.is_none());
            if let Some(last_execution) = receipt.executions.last_mut() {
                if last_execution.function.is_stub_initialization() {
                    last_execution.events.extend(mem::take(&mut events));
                    is_compressed = true;
                }
            }
        }
        if !is_compressed {
            receipt.executions.push(Execution {
                function,
                events,
                task_result,
            });
        }

        // On error, we don't drop tasks mentioned in `resource_events`.
        let output =
            output.map_err(|trap| ExecutionError::new(trap, panic_info, mem::take(receipt)))?;

        for task_id in dropped_tasks {
            let function = ExecutedFunction::TaskDrop { task_id };
            self.execute(function, None, receipt)?;
        }
        Ok(output)
    }

    fn dropped_tasks(events: &[Event]) -> Vec<TaskId> {
        let task_ids = events.iter().filter_map(|event| {
            let event = event.as_resource_event()?;
            if let (ResourceEventKind::Dropped, ResourceId::Task(task_id)) =
                (event.kind, event.resource_id)
            {
                Some(task_id)
            } else {
                None
            }
        });
        task_ids.collect()
    }

    #[tracing::instrument(level = "debug", skip(self, receipt), err)]
    fn poll_task(
        &mut self,
        task_id: TaskId,
        wake_up_cause: WakeUpCause,
        receipt: &mut Receipt,
    ) -> Result<(), ExecutionError> {
        let function = ExecutedFunction::Task {
            task_id,
            wake_up_cause,
        };
        self.execute(function, None, receipt).map(drop)
    }

    fn wake_tasks(&mut self, receipt: &mut Receipt) -> Result<(), ExecutionError> {
        let wakers = self.inner.data_mut().take_wakers();
        for (waker_or_task, wake_up_cause) in wakers {
            match waker_or_task {
                WakerOrTask::Task(task_id) => {
                    self.inner.data_mut().enqueue_task(task_id, &wake_up_cause);
                }
                WakerOrTask::Waker(waker_id) => {
                    let function = ExecutedFunction::Waker {
                        waker_id,
                        wake_up_cause,
                    };
                    self.execute(function, None, receipt)?;
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self), err)]
    pub(crate) fn tick(&mut self) -> Result<Receipt, ExecutionError> {
        if !self.is_initialized() {
            return self.initialize();
        }

        let mut receipt = Receipt::default();
        self.do_tick(&mut receipt).map(|()| receipt)
    }

    fn do_tick(&mut self, receipt: &mut Receipt) -> Result<(), ExecutionError> {
        self.wake_tasks(receipt)?;

        while let Some((task_id, wake_up_cause)) = self.inner.data_mut().take_next_task() {
            self.poll_task(task_id, wake_up_cause, receipt)?;
            if self.inner.data_mut().result().is_ready() {
                tracing::debug!("workflow is completed; clearing task queue");
                self.inner.data_mut().clear_task_queue();
                break;
            }

            self.wake_tasks(receipt)?;
        }
        Ok(())
    }

    pub(crate) fn take_pending_inbound_message(&mut self, channel_id: ChannelId) -> bool {
        self.inner
            .data_mut()
            .take_pending_inbound_message(channel_id)
    }

    pub(crate) fn take_services(&mut self) -> Services {
        self.inner.data_mut().take_services()
    }

    pub(crate) fn notify_on_channel_init(
        &mut self,
        local_id: ChannelId,
        id: ChannelId,
        receipt: &mut Receipt,
    ) {
        let function = ExecutedFunction::StubInitialization;
        let args = ExecutedFunctionArgs::NewChannel { local_id, id };
        self.execute(function, Some(args), receipt).unwrap();
        // ^ `unwrap()` should be safe; no user-generated code is executed
    }

    pub(crate) fn notify_on_child_init(
        &mut self,
        local_id: WorkflowId,
        result: Result<WorkflowAndChannelIds, HostError>,
        receipt: &mut Receipt,
    ) {
        let function = ExecutedFunction::StubInitialization;
        let args = ExecutedFunctionArgs::NewChild { local_id, result };
        self.execute(function, Some(args), receipt).unwrap();
        // ^ `unwrap()` should be safe; no user-generated code is executed
    }

    #[tracing::instrument(level = "debug", skip(self), ret)]
    pub(crate) fn drain_messages(&mut self) -> HashMap<ChannelId, Vec<Message>> {
        self.inner.data_mut().drain_messages()
    }
}

impl<T: PersistWorkflow> Workflow<T> {
    /// Persists this workflow.
    ///
    /// # Panics
    ///
    /// Panics if the workflow is in such a state that it cannot be persisted right now.
    pub(crate) fn persist(&mut self) -> PersistedWorkflow {
        self.inner.data_mut().move_task_queue_to_wakers();
        PersistedWorkflow::new(self).unwrap()
    }
}
