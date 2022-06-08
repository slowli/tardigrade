//! `Workflow` and tightly related types.

use chrono::{DateTime, Utc};
use wasmtime::{Linker, Store, Trap};

use std::{marker::PhantomData, task::Poll};

mod env;
pub use self::env::{
    DataPeeker, MessageReceiver, MessageSender, TimerHandle, WorkflowEnv, WorkflowHandle,
};

use crate::{
    module::{ModuleExports, ModuleImports, WorkflowModule},
    state::{ConsumeError, PersistError, State, WakeUpCause, WakerId, WorkflowState},
    TaskId,
};
use tardigrade_shared::workflow::{InputsBuilder, PutHandle, TakeHandle};

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Execution {
    #[non_exhaustive]
    Task {
        task_id: TaskId,
        wake_up_cause: WakeUpCause,
    },
    #[non_exhaustive]
    Waker {
        waker_id: WakerId,
        result: Result<(), Trap>,
    },
    #[non_exhaustive]
    TaskDrop {
        task_id: TaskId,
        result: Result<(), Trap>,
    },
}

#[derive(Debug)]
pub struct Receipt {
    pub(crate) executions: Vec<Execution>,
}

impl Receipt {
    fn new() -> Self {
        Self {
            executions: Vec::new(),
        }
    }

    pub fn executions(&self) -> &[Execution] {
        &self.executions
    }
}

/// Workflow instance.
#[derive(Debug)]
pub struct Workflow<W> {
    store: Store<State>,
    _interface: PhantomData<*const W>,
}

impl<W: PutHandle<InputsBuilder, ()>> Workflow<W> {
    pub fn new(module: &WorkflowModule<W>, inputs: W::Handle) -> anyhow::Result<(Self, Receipt)> {
        let raw_inputs = module.interface().create_inputs(inputs);
        let state = State::from_interface(module.interface(), raw_inputs.into_inner());
        let mut this = Self::from_state(module, state)?;
        this.spawn_main_task()?;
        let receipt = this.tick();
        Ok((this, receipt))
    }
}

impl<'a, W> Workflow<W>
where
    W: TakeHandle<WorkflowEnv<'a, W>, ()> + 'a,
{
    pub fn handle(&'a mut self) -> WorkflowHandle<W> {
        WorkflowHandle::new(self)
    }
}

impl<W> Workflow<W> {
    fn from_state(module: &WorkflowModule<W>, state: State) -> anyhow::Result<Self> {
        let mut linker = Linker::new(module.inner.engine());
        let mut store = Store::new(linker.engine(), state);
        ModuleImports::extend_linker(&mut store, &mut linker)?;
        let instance = linker.instantiate(&mut store, &module.inner)?;
        let exports = ModuleExports::new(&mut store, &instance);
        store.data_mut().set_exports(exports);
        Ok(Self {
            store,
            _interface: PhantomData,
        })
    }

    fn spawn_main_task(&mut self) -> Result<(), Trap> {
        let main_task_fn = self.store.data().exports().create_main_task;
        let task_ptr = main_task_fn.call(&mut self.store, ())?;
        self.store.data_mut().spawn_main_task(task_ptr);
        Ok(())
    }

    fn poll_task(&mut self, task_id: TaskId, receipt: &mut Receipt) {
        self.store.data_mut().set_current_task(task_id);
        let host_context = task_id;

        let poll_fn = self.store.data().exports().poll_task;
        let poll_result = poll_fn.call(&mut self.store, (task_id, host_context));
        let poll_result = poll_result.and_then(|is_finished| match is_finished {
            0 => Ok(Poll::Pending),
            1 => Ok(Poll::Ready(())),
            _ => Err(Trap::new("unexpected value returned by task polling")),
        });

        let dropped_tasks = self
            .store
            .data_mut()
            .remove_current_task(poll_result.as_ref().copied());
        for task_id in dropped_tasks {
            self.drop_task(task_id, receipt);
        }
    }

    fn drop_task(&mut self, task_id: TaskId, receipt: &mut Receipt) {
        let drop_fn = self.store.data().exports().drop_task;
        let result = drop_fn.call(&mut self.store, task_id);
        receipt
            .executions
            .push(Execution::TaskDrop { task_id, result });
    }

    pub fn queued_tasks(&self) -> impl Iterator<Item = (TaskId, &WakeUpCause)> + '_ {
        self.store.data().queued_tasks()
    }

    fn wake_tasks(&mut self, receipt: &mut Receipt) {
        for wakers in self.store.data_mut().take_wakers() {
            wakers.wake_all(&mut self.store, receipt);
        }
    }

    // FIXME: revert actions (sending messages, spawning tasks) on task panic
    fn tick(&mut self) -> Receipt {
        let mut receipt = Receipt::new();
        self.wake_tasks(&mut receipt);

        while let Some((task_id, wake_up_cause)) = self.store.data_mut().take_next_task() {
            receipt.executions.push(Execution::Task {
                task_id,
                wake_up_cause,
            });
            self.poll_task(task_id, &mut receipt);
            self.wake_tasks(&mut receipt);
        }
        receipt
    }

    fn data_input(&self, input_name: &str) -> Option<Vec<u8>> {
        self.store.data().data_input(input_name)
    }

    fn consume_message(
        &mut self,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<Receipt, ConsumeError> {
        self.store
            .data_mut()
            .push_inbound_message(channel_name, message)?;
        Ok(self.tick())
    }

    fn take_outbound_messages(&mut self, channel_name: &str) -> Vec<Vec<u8>> {
        self.store.data_mut().take_outbound_messages(channel_name)
    }

    fn current_time(&self) -> DateTime<Utc> {
        self.store.data().timers().current_time()
    }

    fn set_current_time(&mut self, time: DateTime<Utc>) {
        self.store.data_mut().set_current_time(time);
    }

    pub fn persist_state(&self) -> Result<(WorkflowState, Vec<u8>), PersistError> {
        let state = self.store.data().persist()?;
        let memory = self.store.data().exports().memory;
        let memory = memory.data(&self.store).to_vec();
        Ok((state, memory))
    }

    pub fn restored(
        module: &WorkflowModule<W>,
        state: WorkflowState,
        memory_contents: &[u8],
    ) -> anyhow::Result<Self> {
        // FIXME: check state / interface agreement
        let mut this = Self::from_state(module, State::from(state))?;
        this.copy_memory(memory_contents)?;
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
