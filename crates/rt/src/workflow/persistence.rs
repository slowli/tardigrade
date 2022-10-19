//! Workflow persistence.

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use wasmtime::Store;

use std::{fmt, task::Poll};

use crate::{
    data::{
        ChildWorkflowState, InboundChannelState, OutboundChannelState, PersistError,
        PersistedWorkflowData, Refs, TaskState, TimerState, Wakers, WorkflowData,
    },
    module::{DataSection, Services, WorkflowSpawner},
    receipt::WakeUpCause,
    utils::Message,
    workflow::{ChannelIds, Workflow},
};
use tardigrade::{task::JoinError, ChannelId, TaskId, TimerId, WorkflowId};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Memory {
    Unstructured(#[serde(with = "serde_compress")] Vec<u8>),
    Structured {
        data_base: u32,
        #[serde(with = "serde_compress")]
        data_diff: Vec<u8>,
        #[serde(with = "serde_compress")]
        heap: Vec<u8>,
    },
}

impl fmt::Debug for Memory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unstructured(bytes) => formatter
                .debug_struct("Unstructured")
                .field("len", &bytes.len())
                .finish(),
            Self::Structured {
                data_base,
                data_diff,
                heap,
            } => formatter
                .debug_struct("Structured")
                .field("data_base", data_base)
                .field("data_diff_len", &data_diff.len())
                .field("heap_len", &heap.len())
                .finish(),
        }
    }
}

mod serde_compress {
    use flate2::{
        bufread::{DeflateDecoder, DeflateEncoder},
        Compression,
    };
    use serde::{de::Error as _, ser::Error as _, Deserializer, Serializer};

    use std::io::{BufReader, Read};

    use crate::utils::serde_b64;

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        let reader = BufReader::new(bytes);
        let mut compressor = DeflateEncoder::new(reader, Compression::best());
        let mut output = vec![];
        compressor
            .read_to_end(&mut output)
            .map_err(|err| S::Error::custom(format!("cannot compress memory snapshot: {}", err)))?;

        serde_b64::serialize(&output, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let compressed: Vec<u8> = serde_b64::deserialize(deserializer)?;
        let reader = BufReader::new(compressed.as_slice());
        let mut output = vec![];
        DeflateDecoder::new(reader)
            .read_to_end(&mut output)
            .map_err(|err| {
                D::Error::custom(format!("cannot decompress memory snapshot: {}", err))
            })?;
        Ok(output)
    }
}

impl Memory {
    fn new(store: &Store<WorkflowData>, data_section: Option<&DataSection>) -> Self {
        let exports = store.data().exports();
        if let Some(section) = data_section {
            let memory = exports.memory.data(store);
            Self::Structured {
                data_base: section.start(),
                data_diff: section.create_diff(memory),
                heap: memory[section.end()..].to_vec(),
            }
        } else {
            Self::Unstructured(exports.memory.data(store).to_vec())
        }
    }

    fn restore(self, workflow: &mut Workflow) -> anyhow::Result<()> {
        match self {
            Self::Unstructured(bytes) => {
                workflow
                    .copy_memory(0, &bytes)
                    .context("failed restoring workflow memory")?;
            }
            Self::Structured {
                data_base,
                mut data_diff,
                heap,
            } => {
                let section = workflow
                    .data_section
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("expected data section"))?;
                ensure!(
                    section.start() == data_base && section.len() == data_diff.len(),
                    "data section shape differs in the persisted workflow and workflow module"
                );
                let heap_start = section.end();

                section.restore_from_diff(&mut data_diff);
                workflow
                    .copy_memory(data_base as usize, &data_diff)
                    .context("failed restoring workflow data section")?;
                workflow
                    .copy_memory(heap_start, &heap)
                    .context("failed restoring workflow heap")?;
            }
        }
        Ok(())
    }
}

/// Persisted version of a workflow containing the state of its external dependencies
/// (channels and timers), and its linear WASM memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedWorkflow {
    state: PersistedWorkflowData,
    refs: Refs,
    memory: Memory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    args: Option<Message>,
}

impl PersistedWorkflow {
    pub(super) fn new(mut workflow: Workflow) -> Result<Self, PersistError> {
        workflow.store.data().check_persistence()?;
        let refs = Refs::new(&mut workflow.store);
        let memory = Memory::new(&workflow.store, workflow.data_section.as_deref());
        let state = workflow.store.into_data().persist();

        Ok(Self {
            state,
            refs,
            memory,
            args: workflow.raw_args.clone(),
        })
    }

    pub(crate) fn inbound_channels(
        &self,
    ) -> impl Iterator<Item = (Option<WorkflowId>, &str, &InboundChannelState)> + '_ {
        self.state.inbound_channels()
    }

    pub(crate) fn outbound_channels(
        &self,
    ) -> impl Iterator<Item = (Option<WorkflowId>, &str, &OutboundChannelState)> + '_ {
        self.state.outbound_channels()
    }

    pub(crate) fn close_inbound_channel(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
    ) {
        self.state.close_inbound_channel(workflow_id, channel_name);
    }

    pub(crate) fn close_outbound_channel(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
    ) {
        self.state.close_outbound_channel(workflow_id, channel_name);
    }

    pub(crate) fn close_outbound_channels_by_id(&mut self, channel_id: ChannelId) {
        for (_, _, channel_state) in self.state.outbound_channels_mut() {
            if channel_state.id() == channel_id {
                channel_state.close();
            }
        }
    }

    /// Returns the current state of a task with the specified ID.
    pub fn task(&self, task_id: TaskId) -> Option<&TaskState> {
        self.state.task(task_id)
    }

    /// Returns the current state of the main task (the task that initializes the workflow),
    /// or `None` if the workflow is not initialized.
    pub fn main_task(&self) -> Option<&TaskState> {
        self.state.main_task()
    }

    /// Lists all tasks in this workflow.
    pub fn tasks(&self) -> impl Iterator<Item = (TaskId, &TaskState)> + '_ {
        self.state.tasks()
    }

    /// Enumerates child workflows.
    pub fn child_workflows(&self) -> impl Iterator<Item = (WorkflowId, &ChildWorkflowState)> + '_ {
        self.state.child_workflows()
    }

    /// Returns the local state of the child workflow with the specified ID, or `None`
    /// if a workflow with such ID was not spawned by this workflow.
    pub fn child_workflow(&self, id: WorkflowId) -> Option<&ChildWorkflowState> {
        self.state.child_workflow(id)
    }

    pub(crate) fn notify_on_child_completion(
        &mut self,
        id: WorkflowId,
        result: Result<(), JoinError>,
    ) {
        self.state.notify_on_child_completion(id, result);
    }

    /// Checks whether the workflow is initialized.
    pub fn is_initialized(&self) -> bool {
        self.args.is_none()
    }

    /// Returns the result of executing this workflow, which is the output of its main task.
    pub fn result(&self) -> Poll<Result<(), &JoinError>> {
        self.state
            .main_task()
            .map_or(Poll::Pending, TaskState::result)
    }

    /// Aborts the workflow by changing the result of its main task.
    pub(crate) fn abort(&mut self) {
        self.state.abort();
    }

    /// Returns the current time for the workflow.
    pub fn current_time(&self) -> DateTime<Utc> {
        self.state.timers.last_known_time()
    }

    pub(crate) fn set_current_time(&mut self, time: DateTime<Utc>) {
        self.state.set_current_time(time);
    }

    /// Returns a timer with the specified `id`.
    pub fn timer(&self, id: TimerId) -> Option<&TimerState> {
        self.state.timers.get(id)
    }

    /// Enumerates all timers together with their states.
    pub fn timers(&self) -> impl Iterator<Item = (TimerId, &TimerState)> + '_ {
        self.state.timers.iter()
    }

    /// Iterates over pending [`WakeUpCause`]s.
    pub fn pending_events(&self) -> impl Iterator<Item = &WakeUpCause> + '_ {
        self.state.waker_queue.iter().map(Wakers::cause)
    }

    pub(crate) fn channel_ids(&self) -> ChannelIds {
        self.state.channel_ids()
    }

    pub(crate) fn check_on_restore(&self, spawner: &WorkflowSpawner<()>) -> anyhow::Result<()> {
        self.state.check_on_restore(spawner.interface())
    }

    /// Restores a workflow from the persisted state and the `spawner` defining the workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow definition from `module` and the `persisted` state
    /// do not match (e.g., differ in defined channels).
    pub(crate) fn restore<'a>(
        self,
        spawner: &WorkflowSpawner<()>,
        services: Services<'a>,
    ) -> anyhow::Result<Workflow<'a>> {
        let interface = spawner.interface();
        let data = self
            .state
            .restore(interface, services)
            .context("failed restoring workflow state")?;
        let mut workflow = Workflow::from_state(spawner, data, self.args)?;
        self.memory
            .restore(&mut workflow)
            .context("failed restoring workflow memory")?;
        self.refs
            .restore(&mut workflow.store)
            .context("failed restoring workflow externrefs")?;
        Ok(workflow)
    }
}
