//! `Workflow` and tightly related types.

use anyhow::Context;
use chrono::{DateTime, Utc};
use wasmtime::{AsContextMut, Linker, Store, Trap};

use std::{marker::PhantomData, task::Poll};

mod env;
pub use self::env::{DataPeeker, MessageReceiver, MessageSender, WorkflowEnv, WorkflowHandle};

use crate::{
    data::{ConsumeError, PersistError, TaskState, TimerState, WorkflowData, WorkflowState},
    module::{ModuleExports, WorkflowModule},
    receipt::{
        Event, ExecutedFunction, Execution, ExecutionError, Receipt, ResourceEventKind, ResourceId,
        WakeUpCause,
    },
    TaskId, TimerId,
};
use tardigrade_shared::workflow::{Initialize, TakeHandle};

#[derive(Debug)]
pub struct PersistedWorkflow {
    state: WorkflowState,
    memory: Vec<u8>,
}

/// Workflow instance.
#[derive(Debug)]
pub struct Workflow<W> {
    store: Store<WorkflowData>,
    _interface: PhantomData<*const W>,
}

impl<W: Initialize<Id = ()>> Workflow<W> {
    pub fn new(module: &WorkflowModule<W>, inputs: W::Init) -> anyhow::Result<Receipt<Self>> {
        let raw_inputs = module.interface().create_inputs(inputs);
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
    pub fn handle(&'a mut self) -> WorkflowHandle<'a, W> {
        WorkflowHandle::new(self)
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
        Ok(Self {
            store,
            _interface: PhantomData,
        })
    }

    fn spawn_main_task(&mut self) -> Result<(), Trap> {
        let exports = self.store.data().exports();
        let task_ptr = exports.create_main_task(self.store.as_context_mut())?;
        self.store.data_mut().spawn_main_task(task_ptr);
        Ok(())
    }

    pub fn task(&self, task_id: TaskId) -> Option<&TaskState> {
        self.store.data().task(task_id)
    }

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
        let output = output?;

        for task_id in dropped_tasks {
            let function = ExecutedFunction::TaskDrop { task_id };
            self.execute(function, receipt)?;
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
    pub fn tick(&mut self) -> Result<Receipt, ExecutionError> {
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

    fn data_input(&self, input_name: &str) -> Option<Vec<u8>> {
        self.store.data().data_input(input_name)
    }

    fn push_inbound_message(
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

    fn take_outbound_messages(&mut self, channel_name: &str) -> Vec<Vec<u8>> {
        let messages = self.store.data_mut().take_outbound_messages(channel_name);
        crate::trace!(
            "Taken messages with lengths {:?} from channel `{}`",
            messages.iter().map(Vec::len).collect::<Vec<_>>(),
            channel_name
        );
        messages
    }

    pub fn current_time(&self) -> DateTime<Utc> {
        self.store.data().timers().current_time()
    }

    pub fn set_current_time(&mut self, time: DateTime<Utc>) -> Result<Receipt, ExecutionError> {
        self.store.data_mut().set_current_time(time);
        crate::trace!("Set current time to {}", time);
        self.tick()
    }

    pub fn timer(&self, id: TimerId) -> Option<&TimerState> {
        self.store.data().timers().get(id)
    }

    pub fn timers(&self) -> impl Iterator<Item = (TimerId, &TimerState)> + '_ {
        self.store.data().timers().iter()
    }

    pub fn persist(&self) -> Result<PersistedWorkflow, PersistError> {
        let state = self.store.data().persist()?;
        let memory = self.store.data().exports().memory;
        let memory = memory.data(&self.store).to_vec();
        Ok(PersistedWorkflow { state, memory })
    }

    pub fn restored(
        module: &WorkflowModule<W>,
        persisted: PersistedWorkflow,
    ) -> anyhow::Result<Self> {
        let data = persisted
            .state
            .restore(module.interface())
            .context("failed restoring workflow state")?;
        let mut this = Self::from_state(module, data)?;
        this.copy_memory(&persisted.memory)
            .context("failed restoring workflow memory")?;
        Ok(this)
    }

    fn copy_memory(&mut self, memory_contents: &[u8]) -> anyhow::Result<()> {
        const WASM_PAGE_SIZE: u64 = 65_536;

        let memory = self.store.data().exports().memory;
        let delta_bytes = memory_contents
            .len()
            .saturating_sub(memory.data_size(&mut self.store));
        let delta_pages = ((delta_bytes as u64) + WASM_PAGE_SIZE - 1) / WASM_PAGE_SIZE;

        if delta_pages > 0 {
            memory.grow(&mut self.store, delta_pages)?;
        }
        memory.write(&mut self.store, 0, memory_contents)?;
        Ok(())
    }
}
