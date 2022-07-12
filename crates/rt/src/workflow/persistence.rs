//! Workflow persistence.

use anyhow::{ensure, Context};

use serde::{Deserialize, Serialize};
use wasmtime::Store;

use crate::{
    data::{PersistError, WorkflowData, WorkflowState},
    module::{DataSection, WorkflowModule},
    workflow::Workflow,
};

#[derive(Debug, Serialize, Deserialize)]
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
}

/// Persisted version of a [`Workflow`] containing the state of its external dependencies
/// (channels and timers), and its linear WASM memory.
#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedWorkflow {
    state: WorkflowState,
    memory: Memory,
}

impl PersistedWorkflow {
    pub(super) fn new<W>(workflow: &Workflow<W>) -> Result<Self, PersistError> {
        let state = workflow.store.data().persist()?;
        let memory = Memory::new(&workflow.store, workflow.data_section.as_deref());
        Ok(Self { state, memory })
    }

    /// Restores a workflow from the persisted state and the `module` defining the workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow definition from `module` and the `persisted` state
    /// do not match (e.g., differ in defined channels / data inputs).
    pub fn restore<W>(self, module: &WorkflowModule<W>) -> anyhow::Result<Workflow<W>> {
        let data = self
            .state
            .restore(module.interface(), module.clone_clock())
            .context("failed restoring workflow state")?;
        let mut workflow = Workflow::from_state(module, data)?;

        match self.memory {
            Memory::Unstructured(bytes) => {
                workflow
                    .copy_memory(0, &bytes)
                    .context("failed restoring workflow memory")?;
            }
            Memory::Structured {
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
        Ok(workflow)
    }
}
