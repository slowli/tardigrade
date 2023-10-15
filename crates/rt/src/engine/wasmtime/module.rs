//! `wasmtime`-powered modules and definitions.

use anyhow::{anyhow, bail, ensure, Context};
use once_cell::sync::OnceCell;
use wasmtime::{Engine, Func, Linker, Module, Store};

use std::{collections::HashMap, fmt, str, sync::Arc};

use super::{
    api::{ModuleExports, ModuleImports},
    functions::{SpawnFunctions, TracingFunctions, WorkflowFunctions},
    instance::{InstanceData, WasmtimeInstance},
};
use crate::{
    data::WorkflowData,
    engine::{DefineWorkflow, WorkflowModule},
};
use tardigrade::interface::Interface;

/// Workflow module wrapping a [`Module`] from the `wasmtime` crate.
#[derive(Clone)]
pub struct WasmtimeModule {
    pub(crate) inner: Module,
    pub(crate) bytes: Arc<[u8]>,
    interfaces: HashMap<String, Interface>,
}

impl fmt::Debug for WasmtimeModule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowModule")
            .field("bytes_len", &self.bytes.len())
            .field("interfaces", &self.interfaces)
            .finish_non_exhaustive()
    }
}

impl WasmtimeModule {
    fn interfaces_from_wasm(module_bytes: &[u8]) -> anyhow::Result<HashMap<String, Interface>> {
        const INTERFACE_SECTION: &str = "__tardigrade_spec";
        const CUSTOM_SECTION_TYPE: u8 = 0;
        const HEADER_LEN: usize = 8; // 4-byte magic + 4-byte version field

        ensure!(
            module_bytes.len() >= HEADER_LEN,
            "WASM module lacks magic and/or version fields"
        );

        let mut remaining_bytes = &module_bytes[HEADER_LEN..];
        while !remaining_bytes.is_empty() {
            let section_type = remaining_bytes[0];
            remaining_bytes = &remaining_bytes[1..];

            let section_len = leb128::read::unsigned(&mut remaining_bytes)
                .context("cannot read WASM section length")?;
            let section_len =
                usize::try_from(section_len).context("cannot convert WASM section length")?;

            if section_type == CUSTOM_SECTION_TYPE {
                let (section_name, section_bytes) =
                    Self::read_section(&remaining_bytes[..section_len])?;
                if section_name == INTERFACE_SECTION.as_bytes() {
                    return Self::interfaces_from_section(section_bytes);
                }
            }

            remaining_bytes = &remaining_bytes[section_len..];
        }
        bail!("WASM lacks `{INTERFACE_SECTION}` custom section");
    }

    fn read_section(mut bytes: &[u8]) -> anyhow::Result<(&[u8], &[u8])> {
        let name_len = leb128::read::unsigned(&mut bytes)
            .context("cannot read WASM custom section name length")?;
        let name_len =
            usize::try_from(name_len).context("cannot convert WASM custom section name length")?;
        Ok(bytes.split_at(name_len))
    }

    fn interfaces_from_section(mut section: &[u8]) -> anyhow::Result<HashMap<String, Interface>> {
        let mut interfaces = HashMap::with_capacity(1);
        while !section.is_empty() {
            let name_len =
                leb128::read::unsigned(&mut section).context("cannot read workflow name length")?;
            let name_len =
                usize::try_from(name_len).context("cannot convert workflow name length")?;
            let name = section
                .get(0..name_len)
                .ok_or_else(|| anyhow!("workflow name is out of bounds"))?;
            let name = str::from_utf8(name).context("workflow name is not UTF-8")?;
            section = &section[name_len..];

            let interface_len = leb128::read::unsigned(&mut section)
                .context("cannot read interface spec length")?;
            let interface_len =
                usize::try_from(interface_len).context("cannot convert interface spec length")?;
            let interface_spec = section
                .get(0..interface_len)
                .ok_or_else(|| anyhow!("interface spec is out of bounds"))?;
            let interface_spec = Interface::try_from_bytes(interface_spec)
                .with_context(|| format!("failed parsing interface spec for workflow `{name}`"))?;
            Self::check_internal_validity(&interface_spec)?;
            section = &section[interface_len..];

            if interfaces.insert(name.to_owned(), interface_spec).is_some() {
                bail!("Interface for workflow `{name}` is redefined");
            }
        }
        Ok(interfaces)
    }

    fn check_internal_validity(interface: &Interface) -> anyhow::Result<()> {
        const EXPECTED_VERSION: u32 = 0;

        ensure!(
            interface.version() == EXPECTED_VERSION,
            "Unsupported interface version: {}, expected {EXPECTED_VERSION}",
            interface.version()
        );
        Ok(())
    }

    fn validate_module(
        module: &Module,
        workflows: &HashMap<String, Interface>,
    ) -> anyhow::Result<()> {
        ModuleExports::validate_module(module, workflows)?;
        ModuleImports::validate_module(module)?;
        Ok(())
    }

    /// Lists the interfaces of workflows defined in this module.
    pub fn interfaces(&self) -> impl Iterator<Item = (&str, &Interface)> + '_ {
        self.interfaces
            .iter()
            .map(|(name, interface)| (name.as_str(), interface))
    }

    pub(super) fn new(engine: &Engine, module_bytes: Arc<[u8]>) -> anyhow::Result<Self> {
        let module =
            Module::from_binary(engine, &module_bytes).context("cannot parse WASM module")?;
        let interfaces = Self::interfaces_from_wasm(&module_bytes)
            .context("cannot extract workflow interface from WASM module")?;
        Self::validate_module(&module, &interfaces)?;
        Ok(Self {
            inner: module,
            bytes: module_bytes,
            interfaces,
        })
    }

    fn into_definitions(self) -> impl Iterator<Item = (String, WasmtimeDefinition)> {
        let module = self.inner;
        self.interfaces.into_iter().map(move |(name, interface)| {
            let definition = WasmtimeDefinition::new(module.clone(), name.clone(), interface);
            (name, definition)
        })
    }
}

impl IntoIterator for WasmtimeModule {
    type Item = (String, WasmtimeDefinition);
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.into_definitions())
    }
}

impl WorkflowModule for WasmtimeModule {
    type Definition = WasmtimeDefinition;

    fn bytes(&self) -> Arc<[u8]> {
        Arc::clone(&self.bytes)
    }
}

/// Workflow definition powered by the `wasmtime` crate.
pub struct WasmtimeDefinition {
    pub(super) module: Module,
    pub(super) workflow_name: String,
    interface: Interface,
    linker_extensions: Vec<Box<dyn LowLevelExtendLinker>>,
    data_section: OnceCell<Arc<DataSection>>,
}

impl fmt::Debug for WasmtimeDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmtimeDefinition")
            .field("workflow_name", &self.workflow_name)
            .field("data_section", &self.data_section)
            .finish_non_exhaustive()
    }
}

impl WasmtimeDefinition {
    fn new(module: Module, workflow_name: String, interface: Interface) -> Self {
        Self {
            module,
            workflow_name,
            interface,
            linker_extensions: vec![
                Box::new(WorkflowFunctions),
                Box::new(SpawnFunctions),
                Box::new(TracingFunctions),
            ],
            data_section: OnceCell::new(),
        }
    }

    pub(super) fn extend_linker(
        &self,
        store: &mut Store<InstanceData>,
        linker: &mut Linker<InstanceData>,
    ) -> anyhow::Result<()> {
        for extension in &self.linker_extensions {
            extension.extend_linker(store, linker)?;
        }
        Ok(())
    }

    pub(super) fn cache_data_section(
        &self,
        store: &Store<InstanceData>,
    ) -> Option<Arc<DataSection>> {
        let exports = store.data().exports();
        exports.data_location.map(|(start, end)| {
            let section = self.data_section.get_or_init(|| {
                let section = exports.memory.data(store)[start as usize..end as usize].to_vec();
                Arc::new(DataSection { start, section })
            });
            Arc::clone(section)
        })
    }
}

impl DefineWorkflow for WasmtimeDefinition {
    type Instance = WasmtimeInstance;

    fn interface(&self) -> &Interface {
        &self.interface
    }

    fn create_workflow(&self, data: WorkflowData) -> anyhow::Result<Self::Instance> {
        WasmtimeInstance::new(self, data)
    }
}

/// Data section of memory in a WASM module.
pub(super) struct DataSection {
    start: u32,
    section: Vec<u8>,
}

impl fmt::Debug for DataSection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DataSection")
            .field("start", &self.start)
            .field("section_len", &self.section.len())
            .finish()
    }
}

impl DataSection {
    pub fn start(&self) -> u32 {
        self.start
    }

    pub fn len(&self) -> usize {
        self.section.len()
    }

    pub fn end(&self) -> usize {
        self.start as usize + self.section.len()
    }

    pub fn create_diff(&self, memory: &[u8]) -> Vec<u8> {
        let mut diff = self.section.clone();
        let section_in_memory = &memory[self.start as usize..];
        for (byte, &mem_byte) in diff.iter_mut().zip(section_in_memory) {
            *byte ^= mem_byte;
        }
        diff
    }

    pub fn restore_from_diff(&self, diff: &mut [u8]) {
        debug_assert_eq!(diff.len(), self.section.len());
        for (byte, &mem_byte) in diff.iter_mut().zip(&self.section) {
            *byte ^= mem_byte;
        }
    }
}

/// WASM linker extension allowing to define additional functions (besides ones provided
/// by the Tardigrade runtime) to be imported into the workflow WASM module.
pub(super) trait ExtendLinker: Send + Sync + 'static {
    /// Name of the module imported into WASM.
    const MODULE_NAME: &'static str;

    /// Returns function imports provided by this extension.
    fn functions(&self, store: &mut Store<InstanceData>) -> Vec<(&'static str, Func)>;
}

/// Object-safe version of `ExtendLinker`.
pub(super) trait LowLevelExtendLinker: Send + Sync + 'static {
    fn extend_linker(
        &self,
        store: &mut Store<InstanceData>,
        linker: &mut Linker<InstanceData>,
    ) -> anyhow::Result<()>;
}

impl<T: ExtendLinker> LowLevelExtendLinker for T {
    fn extend_linker(
        &self,
        store: &mut Store<InstanceData>,
        linker: &mut Linker<InstanceData>,
    ) -> anyhow::Result<()> {
        for (name, function) in self.functions(store) {
            linker.define(Self::MODULE_NAME, name, function)?;
        }
        Ok(())
    }
}
