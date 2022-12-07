//! `wasmtime` implementation for workflow instances.

use anyhow::{anyhow, ensure, Context};
use serde::{Deserialize, Serialize};
use wasmtime::{AsContextMut, ExternRef, Linker, Store, Val};

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
    task::Poll,
};

use super::{
    api::ModuleExports,
    module::{DataSection, WasmtimeDefinition},
};
use crate::{
    data::WorkflowData,
    engine::{AsWorkflowData, PersistWorkflow, RunWorkflow},
    workflow::ChannelIds,
};
use tardigrade::{spawn::HostError, ChannelId, TaskId, WakerId, WorkflowId};

#[derive(Debug)]
pub(super) struct InstanceData {
    pub inner: WorkflowData,
    exports: Option<ModuleExports>,
}

impl InstanceData {
    pub fn exports(&self) -> ModuleExports {
        self.exports
            .expect("attempted accessing exports before they are set")
    }
}

impl From<WorkflowData> for InstanceData {
    fn from(inner: WorkflowData) -> Self {
        Self {
            inner,
            exports: None,
        }
    }
}

/// Workflow instance powered by the `wasmtime` crate.
#[derive(Debug)]
pub struct WasmtimeInstance {
    store: Store<InstanceData>,
    data_section: Option<Arc<DataSection>>,
}

impl WasmtimeInstance {
    pub(super) fn new(definition: &WasmtimeDefinition, data: WorkflowData) -> anyhow::Result<Self> {
        let data = InstanceData {
            inner: data,
            exports: None,
        };

        let mut linker = Linker::new(definition.module.engine());
        let mut store = Store::new(definition.module.engine(), data);
        definition
            .extend_linker(&mut store, &mut linker)
            .context("failed extending `Linker` for module")?;

        let instance = linker
            .instantiate(&mut store, &definition.module)
            .context("failed instantiating module")?;
        let exports = ModuleExports::new(&mut store, &instance, &definition.workflow_name);
        store.data_mut().exports = Some(exports);
        let data_section = definition.cache_data_section(&store);
        Ok(Self {
            store,
            data_section,
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

impl AsWorkflowData for WasmtimeInstance {
    fn data(&self) -> &WorkflowData {
        &self.store.data().inner
    }

    fn data_mut(&mut self) -> &mut WorkflowData {
        &mut self.store.data_mut().inner
    }
}

impl RunWorkflow for WasmtimeInstance {
    fn create_main_task(&mut self, raw_args: &[u8]) -> anyhow::Result<TaskId> {
        let exports = self.store.data().exports();
        exports.create_main_task(self.store.as_context_mut(), raw_args)
    }

    fn poll_task(&mut self, task_id: TaskId) -> anyhow::Result<Poll<()>> {
        let exports = self.store.data().exports();
        exports.poll_task(self.store.as_context_mut(), task_id)
    }

    fn drop_task(&mut self, task_id: TaskId) -> anyhow::Result<()> {
        let exports = self.store.data().exports();
        exports.drop_task(self.store.as_context_mut(), task_id)
    }

    fn wake_waker(&mut self, waker_id: WakerId) -> anyhow::Result<()> {
        let exports = self.store.data().exports();
        exports.wake_waker(self.store.as_context_mut(), waker_id)
    }

    fn initialize_child(&mut self, local_id: WorkflowId, result: Result<WorkflowId, HostError>) {
        let exports = self.store.data().exports();
        if let Err(err) = exports.initialize_child(self.store.as_context_mut(), local_id, result) {
            tracing::warn!(%err, "failed initializing child");
        }
    }

    fn initialize_channel(&mut self, local_id: ChannelId, channel_id: ChannelId) {
        let exports = self.store.data().exports();
        let init_result =
            exports.initialize_channel(self.store.as_context_mut(), local_id, channel_id);
        if let Err(err) = init_result {
            tracing::warn!(%err, "failed initializing channel");
        }
    }
}

impl PersistWorkflow for WasmtimeInstance {
    type Persisted = PersistedInstance;

    fn persist(&mut self) -> Self::Persisted {
        let data_section = self.data_section.as_deref();
        let memory = Memory::new(&self.store, data_section);
        let refs = Refs::new(&mut self.store);
        PersistedInstance { memory, refs }
    }

    fn restore(&mut self, persisted: Self::Persisted) -> anyhow::Result<()> {
        persisted
            .memory
            .restore(self)
            .context("failed restoring workflow memory")?;
        persisted
            .refs
            .restore(&mut self.store)
            .context("failed restoring workflow externrefs")?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedInstance {
    memory: Memory,
    refs: Refs,
}

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
            .map_err(|err| D::Error::custom(format!("cannot decompress memory snapshot: {err}")))?;
        Ok(output)
    }
}

impl Memory {
    fn new(store: &Store<InstanceData>, data_section: Option<&DataSection>) -> Self {
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

    fn restore(self, workflow: &mut WasmtimeInstance) -> anyhow::Result<()> {
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

#[derive(Debug, Clone, Default)]
pub(super) struct SharedChannelHandles {
    pub inner: Arc<Mutex<ChannelIds>>,
}

impl SharedChannelHandles {
    fn new(ids: ChannelIds) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ids)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum HostResource {
    Receiver(ChannelId),
    Sender(ChannelId),
    #[serde(skip)] // FIXME: why is this allowed?
    ChannelHandles(SharedChannelHandles),
    Workflow(WorkflowId),
}

impl HostResource {
    pub(crate) fn from_ref(reference: Option<&ExternRef>) -> anyhow::Result<&Self> {
        let reference = reference.ok_or_else(|| anyhow!("null reference provided to runtime"))?;
        reference
            .data()
            .downcast_ref::<Self>()
            .ok_or_else(|| anyhow!("reference of unexpected type"))
    }

    pub(crate) fn into_ref(self) -> ExternRef {
        ExternRef::new(self)
    }

    pub(crate) fn channel_handles(ids: ChannelIds) -> Self {
        let handles = SharedChannelHandles::new(ids);
        Self::ChannelHandles(handles)
    }

    pub(crate) fn as_receiver(&self) -> anyhow::Result<ChannelId> {
        if let Self::Receiver(channel_id) = self {
            Ok(*channel_id)
        } else {
            let err = anyhow!("unexpected reference type: expected inbound channel, got {self:?}");
            Err(err)
        }
    }

    pub(crate) fn as_sender(&self) -> anyhow::Result<ChannelId> {
        if let Self::Sender(channel_id) = self {
            Ok(*channel_id)
        } else {
            let err = anyhow!("unexpected reference type: expected outbound channel, got {self:?}");
            Err(err)
        }
    }

    pub fn as_channel_handles(&self) -> anyhow::Result<&SharedChannelHandles> {
        if let Self::ChannelHandles(handles) = self {
            Ok(handles)
        } else {
            let err =
                anyhow!("unexpected reference type: expected workflow spawn config, got {self:?}");
            Err(err)
        }
    }

    pub(crate) fn as_workflow(&self) -> anyhow::Result<WorkflowId> {
        if let Self::Workflow(id) = self {
            Ok(*id)
        } else {
            let err = anyhow!("unexpected reference type: expected workflow handle, got {self:?}");
            Err(err)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
struct Refs {
    inner: HashMap<u32, HostResource>,
}

impl Refs {
    fn new(store: &mut Store<InstanceData>) -> Self {
        let ref_table = store.data().exports().ref_table;
        let ref_count = ref_table.size(&mut *store);
        let refs = (0..ref_count).filter_map(|idx| {
            let val = ref_table.get(&mut *store, idx).unwrap();
            // ^ `unwrap()` is safe: we know that the index is in bounds
            val.externref().and_then(|reference| {
                HostResource::from_ref(reference.as_ref())
                    .ok()
                    .cloned()
                    .map(|res| (idx, res))
            })
        });

        Self {
            inner: refs.collect(),
        }
    }

    fn restore(self, store: &mut Store<InstanceData>) -> anyhow::Result<()> {
        let ref_table = store.data().exports().ref_table;
        let expected_size = self.inner.keys().copied().max().map_or(0, |idx| idx + 1);
        let current_size = ref_table.size(&mut *store);
        if current_size < expected_size {
            ref_table.grow(
                &mut *store,
                expected_size - current_size,
                Val::ExternRef(None),
            )?;
        }
        for (idx, resource) in self.inner {
            ref_table.set(&mut *store, idx, resource.into_ref().into())?;
        }
        Ok(())
    }
}
