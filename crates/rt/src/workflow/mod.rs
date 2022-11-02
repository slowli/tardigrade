//! `Workflow` and tightly related types.

use anyhow::Context;
use wasmtime::{AsContextMut, Linker, Store, Trap};

use std::{collections::HashMap, mem, sync::Arc};
use tracing_tunnel::PersistedMetadata;

mod persistence;

pub use self::persistence::PersistedWorkflow;

use crate::{
    data::{ConsumeError, WorkflowData},
    module::{DataSection, ModuleExports, Services, WorkflowSpawner},
    receipt::{
        Event, ExecutedFunction, Execution, ExecutionError, Receipt, ResourceEventKind, ResourceId,
        WakeUpCause,
    },
    utils::Message,
};
use tardigrade::{
    spawn::{ChannelSpawnConfig, ChannelsConfig},
    task::TaskResult,
    ChannelId, TaskId, WorkflowId,
};

#[derive(Debug, Default)]
struct ExecutionOutput {
    main_task_id: Option<TaskId>,
    task_result: Option<TaskResult>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ChannelIds {
    pub inbound: HashMap<String, ChannelId>,
    pub outbound: HashMap<String, ChannelId>,
}

impl ChannelIds {
    pub fn new(
        channels: ChannelsConfig<ChannelId>,
        mut new_channel: impl FnMut() -> ChannelId,
    ) -> Self {
        Self {
            inbound: Self::map_channels(channels.inbound, &mut new_channel),
            outbound: Self::map_channels(channels.outbound, new_channel),
        }
    }

    fn map_channels(
        config: HashMap<String, ChannelSpawnConfig<ChannelId>>,
        mut new_channel: impl FnMut() -> ChannelId,
    ) -> HashMap<String, ChannelId> {
        let channel_ids = config.into_iter().map(|(name, config)| {
            let channel_id = match config {
                ChannelSpawnConfig::New => new_channel(),
                ChannelSpawnConfig::Closed => 0,
                ChannelSpawnConfig::Existing(id) => id,
            };
            (name, channel_id)
        });
        channel_ids.collect()
    }
}

impl WorkflowSpawner<()> {
    pub(crate) fn spawn<'a>(
        &self,
        raw_args: Vec<u8>,
        channel_ids: &ChannelIds,
        services: Services<'a>,
    ) -> anyhow::Result<Workflow<'a>> {
        let state = WorkflowData::new(self.interface(), channel_ids, services);
        Workflow::from_state(self, state, Some(raw_args.into()))
    }
}

/// Workflow instance.
#[derive(Debug)]
pub(crate) struct Workflow<'a> {
    store: Store<WorkflowData<'a>>,
    data_section: Option<Arc<DataSection>>,
    raw_args: Option<Message>,
}

impl<'a> Workflow<'a> {
    fn from_state(
        spawner: &WorkflowSpawner<()>,
        state: WorkflowData<'a>,
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
        })
    }

    #[cfg(test)]
    pub(crate) fn data(&self) -> &WorkflowData<'a> {
        self.store.data()
    }

    #[cfg(test)]
    pub(crate) fn data_mut(&mut self) -> &mut WorkflowData<'a> {
        self.store.data_mut()
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
        let function = ExecutedFunction::Entry;
        let mut receipt = Receipt::default();
        let task_id = self
            .execute(function, Some(raw_data), &mut receipt)?
            .main_task_id
            .expect("main task ID not set");
        self.store.data_mut().spawn_main_task(task_id);
        Ok(receipt)
    }

    fn do_execute(
        &mut self,
        function: &ExecutedFunction,
        data: Option<&[u8]>,
    ) -> Result<ExecutionOutput, Trap> {
        let mut output = ExecutionOutput::default();
        match function {
            ExecutedFunction::Task { task_id, .. } => {
                let exports = self.store.data().exports();
                let exec_result = exports.poll_task(self.store.as_context_mut(), *task_id);
                exec_result.map(|poll| {
                    if poll.is_ready() {
                        output.task_result = Some(self.store.data_mut().complete_current_task());
                    }
                })?;
            }

            ExecutedFunction::Waker {
                waker_id,
                wake_up_cause,
            } => {
                WorkflowData::wake(&mut self.store, *waker_id, wake_up_cause.clone())?;
            }
            ExecutedFunction::TaskDrop { task_id } => {
                let exports = self.store.data().exports();
                exports.drop_task(self.store.as_context_mut(), *task_id)?;
            }
            ExecutedFunction::Entry => {
                let exports = self.store.data().exports();
                output.main_task_id =
                    Some(exports.create_main_task(self.store.as_context_mut(), data.unwrap())?);
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
        self.store.data_mut().set_current_execution(&function);

        let mut output = self.do_execute(&function, data);
        let (events, panic_info) = self
            .store
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

        while let Some((task_id, wake_up_cause)) = self.store.data_mut().take_next_task() {
            self.poll_task(task_id, wake_up_cause, receipt)?;
            if self.store.data().result().is_ready() {
                tracing::debug!("workflow is completed; clearing task queue");
                self.store.data_mut().clear_task_queue();
                break;
            }

            self.wake_tasks(receipt)?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, message), err, fields(message.len = message.len()))]
    pub(crate) fn push_inbound_message(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<(), ConsumeError> {
        self.store
            .data_mut()
            .push_inbound_message(workflow_id, channel_name, message)
    }

    pub(crate) fn take_pending_inbound_message(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
    ) -> bool {
        self.store
            .data_mut()
            .take_pending_inbound_message(workflow_id, channel_name)
    }

    #[tracing::instrument(level = "debug", skip(self), ret)]
    pub(crate) fn drain_messages(&mut self) -> HashMap<ChannelId, Vec<Message>> {
        self.store.data_mut().drain_messages()
    }

    /// Persists this workflow.
    ///
    /// # Panics
    ///
    /// Panics if the workflow is in such a state that it cannot be persisted right now.
    pub(crate) fn persist(self, metadata: &mut PersistedMetadata) -> PersistedWorkflow {
        PersistedWorkflow::new(self, metadata).unwrap()
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
