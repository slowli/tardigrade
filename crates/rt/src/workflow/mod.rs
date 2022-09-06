//! `Workflow` and tightly related types.

use anyhow::Context;
use wasmtime::{AsContextMut, Linker, Store, Trap};

use std::{collections::HashMap, fmt, marker::PhantomData, sync::Arc, task::Poll};

mod persistence;

pub use self::persistence::PersistedWorkflow;

use crate::{
    data::{ConsumeError, PersistError, WorkflowData},
    module::{DataSection, ModuleExports, WorkflowSpawner},
    receipt::{
        Event, ExecutedFunction, Execution, ExecutionError, ExtendedTrap, Receipt,
        ResourceEventKind, ResourceId, WakeUpCause,
    },
    services::Services,
    utils::Message,
    ChannelId, TaskId, WorkflowId,
};
use tardigrade::spawn::{ChannelHandles, ChannelSpawnConfig};

#[derive(Debug, Default)]
pub(crate) struct ChannelIds {
    pub inbound: HashMap<String, ChannelId>,
    pub outbound: HashMap<String, ChannelId>,
}

impl ChannelIds {
    pub fn new(handles: &ChannelHandles, mut new_channel: impl FnMut() -> ChannelId) -> Self {
        Self {
            inbound: Self::map_channels(&handles.inbound, &mut new_channel),
            outbound: Self::map_channels(&handles.outbound, new_channel),
        }
    }

    fn map_channels(
        config: &HashMap<String, ChannelSpawnConfig>,
        mut new_channel: impl FnMut() -> ChannelId,
    ) -> HashMap<String, ChannelId> {
        let channel_ids = config.iter().map(|(name, config)| {
            let channel_id = match config {
                ChannelSpawnConfig::New => new_channel(),
                ChannelSpawnConfig::Closed => 0,
                _ => unreachable!(),
            };
            (name.clone(), channel_id)
        });
        channel_ids.collect()
    }
}

impl WorkflowSpawner<()> {
    pub(crate) fn spawn(
        &self,
        raw_args: Vec<u8>,
        channel_ids: &ChannelIds,
        services: Services,
    ) -> anyhow::Result<Workflow<()>> {
        let state = WorkflowData::new(self.interface(), channel_ids, services);
        Workflow::from_state(self, state, Some(raw_args.into()))
    }
}

/// Workflow instance.
pub struct Workflow<W> {
    store: Store<WorkflowData>,
    data_section: Option<Arc<DataSection>>,
    raw_args: Option<Message>,
    _ty: PhantomData<W>,
}

impl<W> fmt::Debug for Workflow<W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Workflow")
            .field("store", &self.store)
            .field("data_section", &self.data_section)
            .field("raw_args", &self.raw_args)
            .finish()
    }
}

impl<W> Workflow<W> {
    fn from_state(
        spawner: &WorkflowSpawner<W>,
        state: WorkflowData,
        raw_args: Option<Message>,
    ) -> anyhow::Result<Self> {
        let mut linker = Linker::new(spawner.module.engine());
        let mut store = Store::new(spawner.module.engine(), state);
        spawner
            .extend_linker(&mut store, &mut linker)
            .context("failed extending `Linker` for module")?;

        let instance = linker
            .instantiate(&mut store, &spawner.module)
            .context("failed instantiating module")?;
        let exports = ModuleExports::new(&mut store, &instance, spawner.workflow_name());
        store.data_mut().set_exports(exports);
        let data_section = spawner.cache_data_section(&store);
        Ok(Self {
            store,
            data_section,
            raw_args,
            _ty: PhantomData,
        })
    }

    pub(crate) fn is_initialized(&self) -> bool {
        self.raw_args.is_none()
    }

    pub(crate) fn initialize(&mut self) -> Result<Receipt, ExecutionError> {
        let raw_args = self
            .raw_args
            .take()
            .expect("workflow is already initialized");
        let mut spawn_receipt = self.spawn_main_task(raw_args.as_ref())?;
        let tick_receipt = self.tick()?;
        spawn_receipt.extend(tick_receipt);
        Ok(spawn_receipt)
    }

    fn spawn_main_task(&mut self, raw_data: &[u8]) -> Result<Receipt, ExecutionError> {
        let function = ExecutedFunction::Entry { task_id: 0 };
        let mut receipt = Receipt::new();
        if let Err(err) = self.execute(function, Some(raw_data), &mut receipt) {
            return Err(ExecutionError::new(err, receipt));
        }

        let task_id = match &receipt.executions[0].function {
            ExecutedFunction::Entry { task_id } => *task_id,
            _ => unreachable!(),
        };
        self.store.data_mut().spawn_main_task(task_id);
        Ok(receipt)
    }

    fn do_execute(
        &mut self,
        function: &mut ExecutedFunction,
        data: Option<&[u8]>,
    ) -> Result<(), Trap> {
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
            ExecutedFunction::Entry { task_id } => {
                let exports = self.store.data().exports();
                exports
                    .create_main_task(self.store.as_context_mut(), data.unwrap())
                    .map(|new_task_id| {
                        *task_id = new_task_id;
                    })
            }
        }
    }

    fn execute(
        &mut self,
        mut function: ExecutedFunction,
        data: Option<&[u8]>,
        receipt: &mut Receipt,
    ) -> Result<(), ExtendedTrap> {
        self.store
            .data_mut()
            .set_current_execution(function.clone());

        let output = self.do_execute(&mut function, data);
        let (events, panic_info) = self
            .store
            .data_mut()
            .remove_current_execution(output.is_err());
        let dropped_tasks = Self::dropped_tasks(&events);
        receipt.executions.push(Execution { function, events });

        // On error, we don't drop tasks mentioned in `resource_events`.
        output.map_err(|trap| ExtendedTrap::new(trap, panic_info))?;

        for task_id in dropped_tasks {
            let function = ExecutedFunction::TaskDrop { task_id };
            self.execute(function, None, receipt)?;
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
    ) -> Result<(), ExtendedTrap> {
        crate::trace!("Polling task {} because of {:?}", task_id, wake_up_cause);

        let function = ExecutedFunction::Task {
            task_id,
            wake_up_cause,
            poll_result: Poll::Pending,
        };
        let poll_result = self.execute(function, None, receipt);
        crate::log_result!(poll_result, "Finished polling task {}", task_id)
    }

    fn wake_tasks(&mut self, receipt: &mut Receipt) -> Result<(), ExtendedTrap> {
        let wakers = self.store.data_mut().take_wakers();
        for (waker_id, wake_up_cause) in wakers {
            let function = ExecutedFunction::Waker {
                waker_id,
                wake_up_cause,
            };
            self.execute(function, None, receipt)?;
        }
        Ok(())
    }

    pub(crate) fn tick(&mut self) -> Result<Receipt, ExecutionError> {
        if !self.is_initialized() {
            return self.initialize();
        }

        let mut receipt = Receipt::new();
        match self.do_tick(&mut receipt) {
            Ok(()) => Ok(receipt),
            Err(trap) => Err(ExecutionError::new(trap, receipt)),
        }
    }

    fn do_tick(&mut self, receipt: &mut Receipt) -> Result<(), ExtendedTrap> {
        self.wake_tasks(receipt)?;

        while let Some((task_id, wake_up_cause)) = self.store.data_mut().take_next_task() {
            self.poll_task(task_id, wake_up_cause, receipt)?;
            self.wake_tasks(receipt)?;
        }
        Ok(())
    }

    pub(crate) fn push_inbound_message(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<(), ConsumeError> {
        let message_len = message.len();
        let result = self
            .store
            .data_mut()
            .push_inbound_message(workflow_id, channel_name, message);
        crate::log_result!(
            result,
            "Consumed message ({} bytes) for channel `{}`",
            message_len,
            channel_name
        )
    }

    pub(crate) fn close_inbound_channel(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
    ) -> Result<(), ConsumeError> {
        let result = self
            .store
            .data_mut()
            .close_inbound_channel(workflow_id, channel_name);
        crate::log_result!(result, "Closed inbound channel `{}`", channel_name)
    }

    pub(crate) fn drain_messages(&mut self) -> Vec<(ChannelId, Vec<Message>)> {
        self.store.data_mut().drain_messages()
    }

    /// Persists this workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow is in such a state that it cannot be persisted
    /// right now.
    pub(crate) fn persist(&mut self) -> Result<PersistedWorkflow, PersistError> {
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
