//! `Workflow` and tightly related types.

use anyhow::Context;
use chrono::{DateTime, Utc};
use wasmtime::{AsContextMut, Linker, Store, Trap};

use std::{
    collections::{HashMap, HashSet},
    fmt,
    marker::PhantomData,
    sync::Arc,
    task::Poll,
};

mod persistence;
#[cfg(test)]
mod tests;

pub use self::persistence::PersistedWorkflow;

use crate::{
    data::{
        ChildWorkflowState, ConsumeError, InboundChannelState, OutboundChannelState, PersistError,
        TaskState, TimerState, WorkflowData,
    },
    module::{DataSection, ModuleExports, WorkflowSpawner},
    receipt::{
        Event, ExecutedFunction, Execution, ExecutionError, ExtendedTrap, Receipt,
        ResourceEventKind, ResourceId, WakeUpCause,
    },
    services::Services,
    utils::Message,
    ChannelId, TaskId, TimerId, WorkflowId,
};
use tardigrade::spawn::ChannelSpawnConfig;
use tardigrade::{interface::Interface, spawn::ChannelHandles};

#[derive(Debug)]
pub(crate) struct ListenedEvents {
    inbound_channels: HashSet<String>,
    nearest_timer: Option<DateTime<Utc>>,
}

impl ListenedEvents {
    pub fn is_empty(&self) -> bool {
        self.inbound_channels.is_empty() && self.nearest_timer.is_none()
    }

    pub fn nearest_timer(&self) -> Option<DateTime<Utc>> {
        self.nearest_timer
    }

    pub fn contains_inbound_channel(&self, name: &str) -> bool {
        self.inbound_channels.contains(name)
    }
}

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
    ) -> anyhow::Result<InitializingWorkflow<()>> {
        let state = WorkflowData::new(self.interface().clone().erase(), channel_ids, services);
        let workflow = Workflow::from_state(self, state)?;
        Ok(InitializingWorkflow {
            inner: workflow,
            raw_args,
        })
    }
}

/// [`Workflow`] that has not been initialized yet.
///
/// The encapsulated workflow should be initialized by calling [`Self::init()`].
#[must_use = "Should be initialized by calling `Self::init()`"]
pub struct InitializingWorkflow<W> {
    // FIXME: no longer public?
    inner: Workflow<W>,
    raw_args: Vec<u8>,
}

impl<W> fmt::Debug for InitializingWorkflow<W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InitializingWorkflow")
            .field("inner", &self.inner)
            .field("raw_args_len", &self.raw_args.len())
            .finish()
    }
}

impl<W> InitializingWorkflow<W> {
    /// Initializes the workflow by spawning the main task and polling it. Note that depending
    /// on the workflow config, other tasks may be immediately spawned as well.
    ///
    /// # Errors
    ///
    /// Returns an error if the entry point of the workflow or initially polling it fails.
    pub fn init(mut self) -> Result<Receipt<Workflow<W>>, ExecutionError> {
        let mut spawn_receipt = self.inner.spawn_main_task(&self.raw_args)?;
        let tick_receipt = self.inner.tick()?;
        spawn_receipt.extend(tick_receipt);
        Ok(spawn_receipt.map(|()| self.inner))
    }
}

/// Workflow instance.
pub struct Workflow<W> {
    store: Store<WorkflowData>,
    data_section: Option<Arc<DataSection>>,
    _ty: PhantomData<W>,
}

impl<W> fmt::Debug for Workflow<W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Workflow")
            .field("store", &self.store)
            .field("data_section", &self.data_section)
            .finish()
    }
}

impl<W> Workflow<W> {
    fn from_state(spawner: &WorkflowSpawner<W>, state: WorkflowData) -> anyhow::Result<Self> {
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
            _ty: PhantomData,
        })
    }

    pub(crate) fn interface(&self) -> &Interface<()> {
        self.store.data().interface()
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

    /// Returns the current state of a task with the specified ID.
    pub fn task(&self, task_id: TaskId) -> Option<&TaskState> {
        self.store.data().task(task_id)
    }

    /// Lists all tasks in this workflow.
    pub fn tasks(&self) -> impl Iterator<Item = (TaskId, &TaskState)> + '_ {
        self.store.data().tasks()
    }

    /// Checks whether the workflow is finished, i.e., all tasks in it have completed.
    pub fn is_finished(&self) -> bool {
        self.store
            .data()
            .tasks()
            .all(|(_, state)| state.result().is_ready())
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

    /// Enumerates child workflows.
    pub fn child_workflows(&self) -> impl Iterator<Item = (WorkflowId, &ChildWorkflowState)> + '_ {
        self.store.data().child_workflows()
    }

    /// Returns the local state of the child workflow with the specified ID, or `None`
    /// if a workflow with such ID was not spawned by this workflow.
    pub fn child_workflow(&self, id: WorkflowId) -> Option<&ChildWorkflowState> {
        self.store.data().child_workflow(id)
    }

    /// Returns the current state of an inbound channel with the specified name.
    pub fn inbound_channel(&self, channel_name: &str) -> Option<&InboundChannelState> {
        self.store.data().inbound_channel(channel_name)
    }

    /// Returns the current state of an outbound channel with the specified name.
    pub fn outbound_channel(&self, channel_name: &str) -> Option<&OutboundChannelState> {
        self.store.data().outbound_channel(channel_name)
    }

    pub(crate) fn inbound_channel_by_id(
        &self,
        channel_id: ChannelId,
    ) -> Option<(Option<WorkflowId>, &str)> {
        let mut channels = self.store.data().inbound_channels();
        channels.find_map(|(workflow_id, name, state)| {
            if state.id() == channel_id {
                Some((workflow_id, name))
            } else {
                None
            }
        })
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

    #[cfg(feature = "async")]
    pub(crate) fn listened_events(&self) -> ListenedEvents {
        let data = self.store.data();
        let expirations = data.timers().iter().filter_map(|(_, state)| {
            if state.completed_at().is_none() {
                Some(state.definition().expires_at)
            } else {
                None
            }
        });
        ListenedEvents {
            inbound_channels: data
                .listened_inbound_channels()
                .map(str::to_owned)
                .collect(),
            nearest_timer: expirations.min(),
        }
    }

    pub(crate) fn take_outbound_messages(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
    ) -> (usize, Vec<Message>) {
        let (start_idx, messages) = self
            .store
            .data_mut()
            .take_outbound_messages(workflow_id, channel_name);
        crate::trace!(
            "Taken messages with lengths {:?} from channel `{}`",
            messages
                .iter()
                .map(|message| message.as_ref().len())
                .collect::<Vec<_>>(),
            channel_name
        );
        (start_idx, messages)
    }

    pub(crate) fn drain_messages(&mut self) -> Vec<(ChannelId, Vec<Message>)> {
        self.store.data_mut().drain_messages()
    }

    /// Returns the current time for the workflow.
    pub fn current_time(&self) -> DateTime<Utc> {
        self.store.data().timers().last_known_time()
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

    /// Checks whether this workflow can be persisted right now.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be persisted. The error points out
    /// a reason (potentially, one of the reasons if there are multiple).
    pub fn check_persistence(&self) -> Result<(), PersistError> {
        self.store.data().check_persistence()
    }

    /// Persists this workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow is in such a state that it cannot be persisted
    /// right now.
    pub fn persist(&mut self) -> Result<PersistedWorkflow, PersistError> {
        PersistedWorkflow::new(self)
    }

    /// Executes the provided closure, reverting any workflow progress if an error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the workflow cannot be persisted (e.g., due to non-flushed messages).
    /// Call [`Self::check_persistence()`] beforehand to determine whether this is the case.
    ///
    /// # Errors
    ///
    /// Passes through errors returned by the closure.
    pub fn rollback_on_error<T, E>(
        &mut self,
        action: impl FnOnce(&mut Self) -> Result<T, E>,
    ) -> Result<T, E> {
        let backup = self.persist().unwrap();
        action(self).map_err(|err| {
            backup.restore_to_workflow(self); // `unwrap()` should be safe by design
            err
        })
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
