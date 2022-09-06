//! Workflow persistence.

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use wasmtime::Store;

use crate::{
    data::{
        ChildWorkflowState, InboundChannelState, PersistError, PersistedWorkflowData, Refs,
        TaskState, TimerState, Wakers, WorkflowData,
    },
    module::{DataSection, WorkflowSpawner},
    receipt::WakeUpCause,
    services::Services,
    utils::Message,
    workflow::{ChannelIds, Workflow},
    TaskId, TimerId, WorkflowId,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    fn restore<W>(self, workflow: &mut Workflow<W>) -> anyhow::Result<()> {
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

/// Persisted version of a [`Workflow`] containing the state of its external dependencies
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
    pub(super) fn new<W>(workflow: &mut Workflow<W>) -> Result<Self, PersistError> {
        workflow.store.data().check_persistence()?;
        let state = workflow.store.data().persist();
        let refs = Refs::new(&mut workflow.store);
        let memory = Memory::new(&workflow.store, workflow.data_section.as_deref());
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

    /// Returns the current state of a task with the specified ID.
    pub fn task(&self, task_id: TaskId) -> Option<&TaskState> {
        self.state.task(task_id)
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

    /// Checks whether the workflow is initialized.
    pub fn is_initialized(&self) -> bool {
        self.args.is_none()
    }

    /// Checks whether the workflow is finished, i.e., all tasks in it have completed.
    pub fn is_finished(&self) -> bool {
        self.is_initialized() && self.tasks().all(|(_, state)| state.result().is_ready())
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

    /// Restores a workflow from the persisted state and the `spawner` defining the workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow definition from `module` and the `persisted` state
    /// do not match (e.g., differ in defined channels).
    pub(crate) fn restore(
        self,
        spawner: &WorkflowSpawner<()>,
        services: Services,
    ) -> anyhow::Result<Workflow<()>> {
        let interface = spawner.interface();
        let data = self
            .state
            .restore(interface, services)
            .context("failed restoring workflow state")?;
        let mut workflow = Workflow::from_state(spawner, data, self.args)?;
        self.memory.restore(&mut workflow)?;
        self.refs.restore(&mut workflow.store)?;
        Ok(workflow)
    }
}
