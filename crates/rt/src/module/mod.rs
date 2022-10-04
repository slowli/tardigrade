//! `WorkflowModule`, `WorkflowSpawner` and closely related types.

use anyhow::{anyhow, bail, ensure, Context};
use once_cell::sync::OnceCell;
use wasmtime::{Engine, ExternType, Func, Linker, Module, Store, WasmParams, WasmResults};

use std::{collections::HashMap, fmt, marker::PhantomData, str, sync::Arc};

use crate::data::{SpawnFunctions, WorkflowData, WorkflowFunctions};
use tardigrade::{
    interface::Interface,
    workflow::{GetInterface, NamedWorkflow},
};

mod exports;
mod imports;
mod services;
#[cfg(test)]
mod tests;

pub use self::services::Clock;
#[cfg(test)]
pub(crate) use self::tests::{ExportsMock, MockPollFn};
pub(crate) use self::{
    exports::ModuleExports,
    services::{NoOpWorkflowManager, Services, WorkflowAndChannelIds},
};

use self::imports::ModuleImports;

fn ensure_func_ty<Args, Out>(ty: &ExternType, fn_name: &str) -> anyhow::Result<()>
where
    Args: WasmParams,
    Out: WasmResults,
{
    let ty = ty
        .func()
        .ok_or_else(|| anyhow!("`{}` is not a function", fn_name))?;

    Args::typecheck(ty.params())
        .with_context(|| format!("`{}` function has incorrect param types", fn_name))?;
    Out::typecheck(ty.results())
        .with_context(|| format!("`{}` function has incorrect return types", fn_name))
}

/// WASM linker extension allowing to define additional functions (besides ones provided
/// by the Tardigrade runtime) to be imported into the workflow WASM module.
pub trait ExtendLinker: Send + Sync + 'static {
    /// Name of the module imported into WASM.
    const MODULE_NAME: &'static str;

    /// Returns function imports provided by this extension.
    fn functions(&self, store: &mut Store<WorkflowData>) -> Vec<(&'static str, Func)>;
}

/// Object-safe version of `ExtendLinker`.
trait LowLevelExtendLinker: Send + Sync + 'static {
    fn extend_linker(
        &self,
        store: &mut Store<WorkflowData>,
        linker: &mut Linker<WorkflowData>,
    ) -> anyhow::Result<()>;
}

impl<T: ExtendLinker> LowLevelExtendLinker for T {
    fn extend_linker(
        &self,
        store: &mut Store<WorkflowData>,
        linker: &mut Linker<WorkflowData>,
    ) -> anyhow::Result<()> {
        for (name, function) in self.functions(store) {
            linker.define(Self::MODULE_NAME, name, function)?;
        }
        Ok(())
    }
}

/// Workflow engine, essentially a thin wrapper around [`Engine`] from wasmtime.
#[derive(Default)]
pub struct WorkflowEngine {
    inner: Engine,
}

impl fmt::Debug for WorkflowEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowEngine")
            .finish_non_exhaustive()
    }
}

pub(crate) struct DataSection {
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

/// Workflow module that combines a WASM module with the workflow logic and the declared
/// workflow [`Interface`].
pub struct WorkflowModule {
    pub(crate) inner: Module,
    interfaces: HashMap<String, Interface>,
}

impl fmt::Debug for WorkflowModule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowModule")
            .field("interfaces", &self.interfaces)
            .finish_non_exhaustive()
    }
}

impl WorkflowModule {
    #[cfg_attr(test, mimicry::mock(using = "ExportsMock"))]
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
        bail!("WASM lacks `{}` custom section", INTERFACE_SECTION);
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
            let interface_spec = Interface::try_from_bytes(interface_spec).with_context(|| {
                format!("failed parsing interface spec for workflow `{}`", name)
            })?;
            Self::check_internal_validity(&interface_spec)?;
            section = &section[interface_len..];

            if interfaces.insert(name.to_owned(), interface_spec).is_some() {
                bail!("Interface for workflow `{}` is redefined", name);
            }
        }
        Ok(interfaces)
    }

    fn check_internal_validity(interface: &Interface) -> anyhow::Result<()> {
        const EXPECTED_VERSION: u32 = 0;

        ensure!(
            interface.version() == EXPECTED_VERSION,
            "Unsupported interface version: {}, expected {}",
            interface.version(),
            EXPECTED_VERSION
        );
        Ok(())
    }

    #[cfg_attr(test, mimicry::mock(using = "ExportsMock"))]
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

    /// Validates the provided WASM module and wraps it.
    ///
    /// # Errors
    ///
    /// Returns an error in any of the following cases:
    ///
    /// - `module_bytes` is not a valid WASM module.
    /// - The module has bogus imports from the `tardigrade_rt` module, such as an unknown function
    ///   or a known functions with an incorrect signature.
    /// - The module does not have necessary exports.
    /// - The module does not have a custom section with the workflow interface definition(s).
    pub fn new(engine: &WorkflowEngine, module_bytes: &[u8]) -> anyhow::Result<Self> {
        let module =
            Module::from_binary(&engine.inner, module_bytes).context("cannot parse WASM module")?;
        let interfaces = WorkflowModule::interfaces_from_wasm(module_bytes)
            .context("cannot extract workflow interface from WASM module")?;
        WorkflowModule::validate_module(&module, &interfaces)?;
        Ok(Self {
            inner: module,
            interfaces,
        })
    }

    /// Returns a spawner for a strongly-typed workflow defined in this module.
    ///
    /// # Errors
    ///
    /// Returns an error in any of the following cases.
    ///
    /// - The workflow is not present in the module.
    /// - The workflow interface definition does not match the interface implied by type param `W`.
    pub fn for_workflow<W>(&self) -> anyhow::Result<WorkflowSpawner<W>>
    where
        W: GetInterface + NamedWorkflow,
    {
        let interface = self.interfaces.get(W::WORKFLOW_NAME).ok_or_else(|| {
            anyhow!(
                "workflow module does not contain definition of workflow `{}`",
                W::WORKFLOW_NAME
            )
        })?;
        W::interface()
            .check_compatibility(interface)
            .with_context(|| {
                anyhow!("mismatch in interface for workflow `{}`", W::WORKFLOW_NAME)
            })?;

        Ok(WorkflowSpawner::new(
            self.inner.clone(),
            interface.clone(),
            W::WORKFLOW_NAME,
        ))
    }

    /// Creates a spawner for a dynamically-typed workflow with the specified name.
    /// Returns `None` if the workflow with such a name is not present in the module.
    pub fn for_untyped_workflow(&self, workflow_name: &str) -> Option<WorkflowSpawner<()>> {
        let interface = self.interfaces.get(workflow_name)?.clone();
        Some(WorkflowSpawner::new(
            self.inner.clone(),
            interface,
            workflow_name,
        ))
    }
}

/// Spawner of workflows of a specific type.
///
/// Can be created using [`WorkflowModule::for_workflow`] or
/// [`WorkflowModule::for_untyped_workflow`].
pub struct WorkflowSpawner<W> {
    pub(crate) module: Module,
    interface: Interface,
    workflow_name: String,
    linker_extensions: Vec<Box<dyn LowLevelExtendLinker>>,
    data_section: OnceCell<Arc<DataSection>>,
    _ty: PhantomData<W>,
}

impl<W> fmt::Debug for WorkflowSpawner<W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowSpawner")
            .field("interface", &self.interface)
            .field("workflow_name", &self.workflow_name)
            .field("data_section", &self.data_section)
            .finish_non_exhaustive()
    }
}

impl<W> WorkflowSpawner<W> {
    fn new(module: Module, interface: Interface, workflow_name: &str) -> Self {
        Self {
            module,
            interface,
            workflow_name: workflow_name.to_owned(),
            linker_extensions: vec![Box::new(WorkflowFunctions), Box::new(SpawnFunctions)],
            data_section: OnceCell::new(),
            _ty: PhantomData,
        }
    }

    /// Inserts imports into the module linker, allowing the workflow in the module depend
    /// on additional imports besides ones provided by the Tardigrade runtime.
    pub fn insert_imports(&mut self, imports: impl ExtendLinker) -> &mut Self {
        self.linker_extensions.push(Box::new(imports));
        self
    }

    pub(crate) fn extend_linker(
        &self,
        store: &mut Store<WorkflowData>,
        linker: &mut Linker<WorkflowData>,
    ) -> anyhow::Result<()> {
        for extension in &self.linker_extensions {
            extension.extend_linker(store, linker)?;
        }
        Ok(())
    }

    pub(crate) fn cache_data_section(
        &self,
        store: &Store<WorkflowData>,
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

    /// Returns the interface of the workflow spawned by this spawner.
    pub fn interface(&self) -> &Interface {
        &self.interface
    }

    /// Returns the name of the workflow spawned by this spawner.
    pub fn workflow_name(&self) -> &str {
        &self.workflow_name
    }

    pub(crate) fn erase(self) -> WorkflowSpawner<()> {
        WorkflowSpawner {
            module: self.module,
            interface: self.interface,
            workflow_name: self.workflow_name,
            linker_extensions: self.linker_extensions,
            data_section: self.data_section,
            _ty: PhantomData,
        }
    }
}
