//! `Workflow` and tightly related types.

use anyhow::Context;
use chrono::{DateTime, Utc};
use wasmtime::{AsContextMut, Linker, Store, Trap};

use std::{fmt, sync::Arc, task::Poll};

mod persistence;
#[cfg(test)]
mod tests;

pub use self::persistence::PersistedWorkflow;

use crate::{
    data::{ConsumeError, PersistError, TaskState, TimerState, WorkflowData},
    handle::{WorkflowEnv, WorkflowHandle},
    module::{DataSection, ModuleExports, WorkflowModule},
    receipt::{
        Event, ExecutedFunction, Execution, ExecutionError, Receipt, ResourceEventKind, ResourceId,
        WakeUpCause,
    },
    TaskId, TimerId,
};
use tardigrade::{
    interface::Interface,
    workflow::{Initialize, Inputs, TakeHandle},
};

/// Workflow instance.
pub struct Workflow<W> {
    store: Store<WorkflowData>,
    interface: Interface<W>,
    data_section: Option<Arc<DataSection>>,
}

impl<W> fmt::Debug for Workflow<W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Workflow")
            .field("store", &self.store)
            .field("interface", &self.interface)
            .finish()
    }
}

impl<W: Initialize<Id = ()>> Workflow<W> {
    /// Instantiates a new workflow from the `module` and the provided `inputs`.
    ///
    /// # Errors
    ///
    /// - Returns an error in case preparations for instantiation (e.g., extending the WASM linker
    ///   with imports) fails.
    /// - Returns an error if a trap occurs when spawning the main task for the workflow
    ///   or polling it.
    pub fn new(module: &WorkflowModule<W>, inputs: W::Init) -> anyhow::Result<Receipt<Self>> {
        let raw_inputs = Inputs::for_interface(module.interface(), inputs);
        let state = WorkflowData::from_interface(module.interface(), raw_inputs.into_inner());
        let mut this = Self::from_state(module, state)?;
        this.spawn_main_task()
            .context("failed spawning main task")?;
        let receipt = this.tick().context("failed polling main task")?;
        Ok(receipt.map(|()| this))
    }
}

impl<'a, W> Workflow<W>
where
    W: TakeHandle<WorkflowEnv<'a, W>, Id = ()> + 'a,
{
    /// Gets a handle for this workflow allowing to interact with its channels.
    #[allow(clippy::missing_panics_doc)] // TODO: is `unwrap()` really safe here?
    pub fn handle(&'a mut self) -> WorkflowHandle<'a, W> {
        WorkflowHandle::new(self).unwrap()
    }
}

impl<W> Workflow<W> {
    fn from_state(module: &WorkflowModule<W>, state: WorkflowData) -> anyhow::Result<Self> {
        let mut linker = Linker::new(module.inner.engine());
        let mut store = Store::new(linker.engine(), state);
        module
            .extend_linker(&mut store, &mut linker)
            .context("failed extending `Linker` for module")?;

        let instance = linker
            .instantiate(&mut store, &module.inner)
            .context("failed instantiating module")?;
        let exports = ModuleExports::new(&mut store, &instance);
        store.data_mut().set_exports(exports);
        let data_section = module.cache_data_section(&store);
        Ok(Self {
            store,
            interface: module.interface().clone(),
            data_section,
        })
    }

    pub(crate) fn interface(&self) -> &Interface<W> {
        &self.interface
    }

    fn spawn_main_task(&mut self) -> Result<(), Trap> {
        let exports = self.store.data().exports();
        let task_ptr = exports.create_main_task(self.store.as_context_mut())?;
        self.store.data_mut().spawn_main_task(task_ptr);
        Ok(())
    }

    /// Returns the current state of a task with the specified ID.
    pub fn task(&self, task_id: TaskId) -> Option<&TaskState> {
        self.store.data().task(task_id)
    }

    /// Lists all tasks in this workflow.
    pub fn tasks(&self) -> impl Iterator<Item = (TaskId, &TaskState)> + '_ {
        self.store.data().tasks()
    }

    fn do_execute(&mut self, function: &mut ExecutedFunction) -> Result<(), Trap> {
        match function {
            ExecutedFunction::Task {
                task_id,
                poll_result,
                ..
            } => {
                let exports = self.store.data().exports();
                let exec_result = exports.poll_task(self.store.as_context_mut(), *task_id);
                if let Ok(Poll::Ready(())) = exec_result {
                    self.store.data_mut().complete_current_task();
                }
                exec_result.map(|poll| {
                    *poll_result = poll;
                })
            }

            ExecutedFunction::Waker {
                waker_id,
                wake_up_cause,
            } => {
                crate::trace!(
                    "Waking up waker {} with cause {:?}",
                    waker_id,
                    wake_up_cause
                );
                WorkflowData::wake(&mut self.store, *waker_id, wake_up_cause.clone())
            }
            ExecutedFunction::TaskDrop { task_id } => {
                let exports = self.store.data().exports();
                exports.drop_task(self.store.as_context_mut(), *task_id)
            }
            ExecutedFunction::Entry => unreachable!(),
        }
    }

    fn execute(
        &mut self,
        mut function: ExecutedFunction,
        receipt: &mut Receipt,
    ) -> Result<(), Trap> {
        self.store
            .data_mut()
            .set_current_execution(function.clone());

        let output = self.do_execute(&mut function);
        let events = self
            .store
            .data_mut()
            .remove_current_execution(output.is_err());
        let dropped_tasks = Self::dropped_tasks(&events);
        receipt.executions.push(Execution { function, events });

        // On error, we don't drop tasks mentioned in `resource_events`.
        output?;

        for task_id in dropped_tasks {
            let function = ExecutedFunction::TaskDrop { task_id };
            self.execute(function, receipt)?;
        }
        Ok(())
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

    fn poll_task(
        &mut self,
        task_id: TaskId,
        wake_up_cause: WakeUpCause,
        receipt: &mut Receipt,
    ) -> Result<(), Trap> {
        crate::trace!("Polling task {} because of {:?}", task_id, wake_up_cause);

        let function = ExecutedFunction::Task {
            task_id,
            wake_up_cause,
            poll_result: Poll::Pending,
        };
        let poll_result = self.execute(function, receipt);
        crate::log_result!(poll_result, "Finished polling task {}", task_id).map(drop)
    }

    fn wake_tasks(&mut self, receipt: &mut Receipt) -> Result<(), Trap> {
        let wakers = self.store.data_mut().take_wakers();
        for (waker_id, wake_up_cause) in wakers {
            let function = ExecutedFunction::Waker {
                waker_id,
                wake_up_cause,
            };
            self.execute(function, receipt)?;
        }
        Ok(())
    }

    // FIXME: revert actions (sending messages, spawning tasks) on task panic
    pub(crate) fn tick(&mut self) -> Result<Receipt, ExecutionError> {
        let mut receipt = Receipt::new();
        match self.do_tick(&mut receipt) {
            Ok(()) => Ok(receipt),
            Err(trap) => Err(ExecutionError::new(trap, receipt)),
        }
    }

    fn do_tick(&mut self, receipt: &mut Receipt) -> Result<(), Trap> {
        self.wake_tasks(receipt)?;

        while let Some((task_id, wake_up_cause)) = self.store.data_mut().take_next_task() {
            self.poll_task(task_id, wake_up_cause, receipt)?;
            self.wake_tasks(receipt)?;
        }
        Ok(())
    }

    pub(crate) fn data_input(&self, input_name: &str) -> Option<Vec<u8>> {
        self.store.data().data_input(input_name)
    }

    pub(crate) fn push_inbound_message(
        &mut self,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<(), ConsumeError> {
        let message_len = message.len();
        let result = self
            .store
            .data_mut()
            .push_inbound_message(channel_name, message);
        crate::log_result!(
            result,
            "Consumed message ({} bytes) for channel `{}`",
            message_len,
            channel_name
        )
    }

    pub(crate) fn take_outbound_messages(&mut self, channel_name: &str) -> (usize, Vec<Vec<u8>>) {
        let (start_idx, messages) = self.store.data_mut().take_outbound_messages(channel_name);
        crate::trace!(
            "Taken messages with lengths {:?} from channel `{}`",
            messages.iter().map(Vec::len).collect::<Vec<_>>(),
            channel_name
        );
        (start_idx, messages)
    }

    /// Returns the current time for the workflow.
    pub fn current_time(&self) -> DateTime<Utc> {
        self.store.data().timers().current_time()
    }

    /// Sets the current time for the workflow and completes the relevant timers.
    ///
    /// # Errors
    ///
    /// Returns an error if workflow execution traps.
    pub fn set_current_time(&mut self, time: DateTime<Utc>) -> Result<Receipt, ExecutionError> {
        self.store.data_mut().set_current_time(time);
        crate::trace!("Set current time to {}", time);
        self.tick()
    }

    /// Returns a timer with the specified `id`.
    pub fn timer(&self, id: TimerId) -> Option<&TimerState> {
        self.store.data().timers().get(id)
    }

    /// Enumerates all timers together with their states.
    pub fn timers(&self) -> impl Iterator<Item = (TimerId, &TimerState)> + '_ {
        self.store.data().timers().iter()
    }

    /// Persists this workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow is in such a state that it cannot be persisted
    /// right now.
    pub fn persist(&self) -> Result<PersistedWorkflow, PersistError> {
        PersistedWorkflow::new(self)
    }

    fn copy_memory(&mut self, offset: usize, memory_contents: &[u8]) -> anyhow::Result<()> {
        const WASM_PAGE_SIZE: u64 = 65_536;

        let memory = self.store.data().exports().memory;
        let delta_bytes =
            (memory_contents.len() + offset).saturating_sub(memory.data_size(&mut self.store));
        let delta_pages = ((delta_bytes as u64) + WASM_PAGE_SIZE - 1) / WASM_PAGE_SIZE;

        if delta_pages > 0 {
            memory.grow(&mut self.store, delta_pages)?;
        }
        memory.write(&mut self.store, offset, memory_contents)?;
        Ok(())
    }
}
