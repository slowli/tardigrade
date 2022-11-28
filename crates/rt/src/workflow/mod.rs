//! `Workflow` and tightly related types.

use anyhow::Context;
use serde::{Deserialize, Serialize};

use std::{collections::HashMap, mem};

mod persistence;

pub use self::persistence::PersistedWorkflow;

use crate::{
    data::WorkflowData,
    engine::{CreateWorkflow, PersistWorkflow, RunWorkflow, WorkflowSpawner},
    manager::Services,
    receipt::{
        Event, ExecutedFunction, Execution, ExecutionError, Receipt, ResourceEventKind, ResourceId,
        WakeUpCause,
    },
    utils::Message,
};
use tardigrade::{task::TaskResult, ChannelId, TaskId};

#[derive(Debug, Default)]
struct ExecutionOutput {
    main_task_id: Option<TaskId>,
    task_result: Option<TaskResult>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[allow(clippy::unsafe_derive_deserialize)] // false positive
pub(crate) struct ChannelIds {
    pub receivers: HashMap<String, ChannelId>,
    pub senders: HashMap<String, ChannelId>,
}

/// Workflow instance.
#[derive(Debug)]
pub(crate) struct Workflow<T> {
    inner: T,
    args: Option<Message>,
}

impl<T: RunWorkflow + PersistWorkflow> Workflow<T> {
    pub(crate) fn new<S>(
        spawner: &WorkflowSpawner<S>,
        data: WorkflowData,
        args: Option<Message>,
    ) -> anyhow::Result<Self>
    where
        S: CreateWorkflow<Spawned = T>,
    {
        let inner = spawner
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

    fn spawn_main_task(&mut self, raw_data: &[u8]) -> Result<Receipt, ExecutionError> {
        let mut receipt = Receipt::default();
        let task_id = self
            .execute(ExecutedFunction::Entry, Some(raw_data), &mut receipt)?
            .main_task_id
            .expect("main task ID not set");
        self.inner.data_mut().spawn_main_task(task_id);
        Ok(receipt)
    }

    fn do_execute(
        &mut self,
        function: &ExecutedFunction,
        data: Option<&[u8]>,
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
                WorkflowData::wake(&mut self.inner, *waker_id, wake_up_cause.clone())?;
            }
            ExecutedFunction::TaskDrop { task_id } => {
                self.inner.drop_task(*task_id)?;
            }
            ExecutedFunction::Entry => {
                output.main_task_id = Some(self.inner.create_main_task(data.unwrap())?);
            }
        }
        Ok(output)
    }

    fn execute(
        &mut self,
        function: ExecutedFunction,
        data: Option<&[u8]>,
        receipt: &mut Receipt,
    ) -> Result<ExecutionOutput, ExecutionError> {
        self.inner.data_mut().set_current_execution(&function);

        let mut output = self.do_execute(&function, data);
        let (events, panic_info) = self
            .inner
            .data_mut()
            .remove_current_execution(output.is_err());
        let dropped_tasks = Self::dropped_tasks(&events);
        let task_result = output
            .as_mut()
            .map_or(None, |output| output.task_result.take());
        receipt.executions.push(Execution {
            function,
            events,
            task_result,
        });

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
        for (waker_id, wake_up_cause) in wakers {
            let function = ExecutedFunction::Waker {
                waker_id,
                wake_up_cause,
            };
            self.execute(function, None, receipt)?;
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

    #[tracing::instrument(level = "debug", skip(self), ret)]
    pub(crate) fn drain_messages(&mut self) -> HashMap<ChannelId, Vec<Message>> {
        self.inner.data_mut().drain_messages()
    }

    /// Persists this workflow.
    ///
    /// # Panics
    ///
    /// Panics if the workflow is in such a state that it cannot be persisted right now.
    pub(crate) fn persist(&mut self) -> PersistedWorkflow {
        PersistedWorkflow::new(self).unwrap()
    }
}
