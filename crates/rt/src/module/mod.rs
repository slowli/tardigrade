//! Module utils.

use anyhow::{anyhow, bail, ensure, Context};
use chrono::{DateTime, Utc};
use once_cell::sync::OnceCell;
use wasmtime::{
    AsContextMut, Caller, Engine, ExternType, Func, Instance, Linker, Memory, Module, Store,
    StoreContextMut, Trap, TypedFunc, WasmParams, WasmResults, WasmRet, WasmTy,
};

use std::{collections::HashMap, fmt, str, sync::Arc, task::Poll};

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) use self::tests::{ExportsMock, MockPollFn};

use crate::{
    data::{WasmContextPtr, WorkflowData, WorkflowFunctions},
    TaskId, TimerId, WakerId,
};
use tardigrade::interface::Interface;
use tardigrade::workflow::GetInterface;
use tardigrade_shared::abi::TryFromWasm;

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
pub trait ExtendLinker: 'static {
    /// Name of the module imported into WASM.
    const MODULE_NAME: &'static str;

    /// Returns function imports provided by this extension.
    fn functions(&self, store: &mut Store<WorkflowData>) -> Vec<(&'static str, Func)>;
}

/// Object-safe version of `ExtendLinker`.
trait LowLevelExtendLinker {
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

/// Wall clock.
pub trait Clock: Send + Sync + 'static {
    /// Returns the current timestamp. This is used in [`Workflow`]s when creating new timers.
    ///
    /// [`Workflow`]: crate::Workflow
    fn now(&self) -> DateTime<Utc>;
}

impl<F> Clock for F
where
    F: Fn() -> DateTime<Utc> + Send + Sync + 'static,
{
    fn now(&self) -> DateTime<Utc> {
        self()
    }
}

impl fmt::Debug for dyn Clock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Clock").finish_non_exhaustive()
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

#[derive(Debug)]
pub(crate) struct DataSection {
    start: u32,
    section: Vec<u8>,
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
    interfaces: HashMap<String, Interface<()>>,
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
    #[cfg_attr(test, mimicry::mock(using = "tests::ExportsMock"))]
    fn interfaces_from_wasm(module_bytes: &[u8]) -> anyhow::Result<HashMap<String, Interface<()>>> {
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

    fn interfaces_from_section(
        mut section: &[u8],
    ) -> anyhow::Result<HashMap<String, Interface<()>>> {
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

    fn check_internal_validity(interface: &Interface<()>) -> anyhow::Result<()> {
        const EXPECTED_VERSION: u32 = 0;

        ensure!(
            interface.version() == EXPECTED_VERSION,
            "Unsupported interface version: {}, expected {}",
            interface.version(),
            EXPECTED_VERSION
        );
        Ok(())
    }

    #[cfg_attr(test, mimicry::mock(using = "tests::ExportsMock"))]
    fn validate_module(
        module: &Module,
        workflows: &HashMap<String, Interface<()>>,
    ) -> anyhow::Result<()> {
        ModuleExports::validate_module(module, workflows)?;
        ModuleImports::validate_module(module)?;
        Ok(())
    }

    /// Lists the interfaces of workflows defined in this module.
    pub fn interfaces(&self) -> impl Iterator<Item = (&str, &Interface<()>)> + '_ {
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
        W: GetInterface,
    {
        let interface = self.interfaces.get(W::WORKFLOW_NAME).ok_or_else(|| {
            anyhow!(
                "workflow module does not contain definition of workflow `{}`",
                W::WORKFLOW_NAME
            )
        })?;
        let interface = interface.clone().downcast::<W>().with_context(|| {
            anyhow!("mismatch in interface for workflow `{}`", W::WORKFLOW_NAME)
        })?;
        Ok(WorkflowSpawner::new(
            self.inner.clone(),
            interface,
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
    interface: Interface<W>,
    workflow_name: String,
    clock: Arc<dyn Clock>,
    linker_extensions: Vec<Box<dyn LowLevelExtendLinker>>,
    data_section: OnceCell<Arc<DataSection>>,
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
    fn new(module: Module, interface: Interface<W>, workflow_name: &str) -> Self {
        Self {
            module,
            interface,
            workflow_name: workflow_name.to_owned(),
            clock: Arc::new(Utc::now),
            linker_extensions: vec![Box::new(WorkflowFunctions)],
            data_section: OnceCell::new(),
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
    pub fn interface(&self) -> &Interface<W> {
        &self.interface
    }

    /// Returns the name of the workflow spawned by this spawner.
    pub fn workflow_name(&self) -> &str {
        &self.workflow_name
    }

    /// Specifies a [`Clock`] implementation to be used with [`Workflow`]s instantiated
    /// from this module.
    ///
    /// [`Workflow`]: crate::Workflow
    #[must_use]
    pub fn with_clock(mut self, clock: impl Clock) -> Self {
        self.clock = Arc::new(clock);
        self
    }

    pub(crate) fn clone_clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ModuleExports {
    pub memory: Memory,
    data_location: Option<(u32, u32)>,
    create_main_task: TypedFunc<(), TaskId>,
    poll_task: TypedFunc<(TaskId, TaskId), i32>,
    drop_task: TypedFunc<TaskId, ()>,
    alloc_bytes: TypedFunc<u32, u32>,
    create_waker: TypedFunc<WasmContextPtr, WakerId>,
    wake_waker: TypedFunc<WakerId, ()>,
}

#[cfg_attr(test, mimicry::mock(using = "tests::ExportsMock"))]
impl ModuleExports {
    pub fn create_main_task(&self, ctx: StoreContextMut<'_, WorkflowData>) -> Result<TaskId, Trap> {
        let result = self.create_main_task.call(ctx, ());
        crate::log_result!(result, "Created main task")
    }

    pub fn poll_task(
        &self,
        ctx: StoreContextMut<'_, WorkflowData>,
        task_id: TaskId,
    ) -> Result<Poll<()>, Trap> {
        let result = self
            .poll_task
            .call(ctx, (task_id, task_id))
            .and_then(|res| <Poll<()>>::try_from_wasm(res).map_err(Trap::new));
        crate::log_result!(result, "Polled task {}", task_id)
    }

    pub fn drop_task(
        &self,
        ctx: StoreContextMut<'_, WorkflowData>,
        task_id: TaskId,
    ) -> Result<(), Trap> {
        let result = self.drop_task.call(ctx, task_id);
        crate::log_result!(result, "Dropped task {}", task_id)
    }

    pub fn alloc_bytes(
        &self,
        ctx: StoreContextMut<'_, WorkflowData>,
        capacity: u32,
    ) -> Result<u32, Trap> {
        let result = self.alloc_bytes.call(ctx, capacity);
        crate::log_result!(result, "Allocated {} bytes", capacity)
    }

    pub fn create_waker(
        &self,
        ctx: StoreContextMut<'_, WorkflowData>,
        cx_ptr: WasmContextPtr,
    ) -> Result<WakerId, Trap> {
        let result = self.create_waker.call(ctx, cx_ptr);
        crate::log_result!(result, "Created waker from context {}", cx_ptr)
    }

    pub fn wake_waker(
        &self,
        ctx: StoreContextMut<'_, WorkflowData>,
        waker_id: WakerId,
    ) -> Result<(), Trap> {
        let result = self.wake_waker.call(ctx, waker_id);
        crate::log_result!(result, "Waked waker {}", waker_id)
    }
}

impl fmt::Debug for ModuleExports {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModuleExports")
            .field("memory", &self.memory)
            .finish()
    }
}

impl ModuleExports {
    fn validate_module(
        module: &Module,
        workflows: &HashMap<String, Interface<()>>,
    ) -> anyhow::Result<()> {
        let memory_ty = module
            .get_export("memory")
            .ok_or_else(|| anyhow!("module does not export memory"))?;
        ensure!(
            matches!(memory_ty, ExternType::Memory(_)),
            "`memory` export is not a memory"
        );

        for workflow_name in workflows.keys() {
            Self::ensure_export_ty::<(), TaskId>(
                module,
                &format!("tardigrade_rt::spawn::{}", workflow_name),
            )?;
        }
        Self::ensure_export_ty::<(TaskId, TaskId), i32>(module, "tardigrade_rt::poll_task")?;
        Self::ensure_export_ty::<TaskId, ()>(module, "tardigrade_rt::drop_task")?;
        Self::ensure_export_ty::<u32, u32>(module, "tardigrade_rt::alloc_bytes")?;
        Self::ensure_export_ty::<WasmContextPtr, WakerId>(module, "tardigrade_rt::create_waker")?;
        Self::ensure_export_ty::<WakerId, ()>(module, "tardigrade_rt::wake_waker")?;

        Ok(())
    }

    fn ensure_export_ty<Args, Out>(module: &Module, fn_name: &str) -> anyhow::Result<()>
    where
        Args: WasmParams,
        Out: WasmResults,
    {
        let ty = module
            .get_export(fn_name)
            .ok_or_else(|| anyhow!("module does not export `{}` function", fn_name))?;
        ensure_func_ty::<Args, Out>(&ty, fn_name)
    }

    #[cfg_attr(test, mimicry::mock(using = "tests::ExportsMock::mock_new"))]
    pub fn new(store: &mut Store<WorkflowData>, instance: &Instance, workflow_name: &str) -> Self {
        let memory = instance.get_memory(&mut *store, "memory").unwrap();
        let global_base = Self::extract_u32_global(&mut *store, instance, "__global_base");
        let heap_base = Self::extract_u32_global(&mut *store, instance, "__heap_base");
        let data_location = global_base.and_then(|start| heap_base.map(|end| (start, end)));
        let main_fn_name = format!("tardigrade_rt::spawn::{}", workflow_name);

        Self {
            memory,
            data_location,
            create_main_task: Self::extract_function(&mut *store, instance, &main_fn_name),
            poll_task: Self::extract_function(&mut *store, instance, "tardigrade_rt::poll_task"),
            drop_task: Self::extract_function(&mut *store, instance, "tardigrade_rt::drop_task"),
            alloc_bytes: Self::extract_function(
                &mut *store,
                instance,
                "tardigrade_rt::alloc_bytes",
            ),
            create_waker: Self::extract_function(store, instance, "tardigrade_rt::create_waker"),
            wake_waker: Self::extract_function(store, instance, "tardigrade_rt::wake_waker"),
        }
    }

    #[allow(clippy::cast_sign_loss)] // intentional
    fn extract_u32_global(
        store: &mut Store<WorkflowData>,
        instance: &Instance,
        name: &str,
    ) -> Option<u32> {
        let value = instance.get_global(&mut *store, name)?.get(&mut *store);
        value.i32().map(|value| value as u32)
    }

    fn extract_function<Args, Out>(
        store: &mut Store<WorkflowData>,
        instance: &Instance,
        fn_name: &str,
    ) -> TypedFunc<Args, Out>
    where
        Args: WasmParams,
        Out: WasmResults,
    {
        instance
            .get_func(&mut *store, fn_name)
            .unwrap_or_else(|| panic!("function `{}` is not exported", fn_name))
            .typed(&*store)
            .with_context(|| format!("Function `{}` has incorrect signature", fn_name))
            .unwrap()
    }
}

#[derive(Debug)]
pub(crate) struct ModuleImports;

impl ModuleImports {
    const RT_MODULE: &'static str = "tardigrade_rt";

    fn validate_module(module: &Module) -> anyhow::Result<()> {
        let rt_imports = module
            .imports()
            .filter(|import| import.module() == Self::RT_MODULE);

        for import in rt_imports {
            let ty = import.ty();
            let fn_name = import.name();
            Self::validate_import(&ty, fn_name)?;
        }

        Ok(())
    }

    fn validate_import(ty: &ExternType, fn_name: &str) -> anyhow::Result<()> {
        match fn_name {
            "task::poll_completion" => ensure_func_ty::<(TaskId, WasmContextPtr), i64>(ty, fn_name),
            "task::spawn" => ensure_func_ty::<(u32, u32, TaskId), ()>(ty, fn_name),
            "task::wake" | "task::abort" => ensure_func_ty::<TaskId, ()>(ty, fn_name),

            "data_input::get" => ensure_func_ty::<(u32, u32), i64>(ty, fn_name),

            "mpsc_receiver::get" | "mpsc_sender::get" => {
                ensure_func_ty::<(u32, u32), i64>(ty, fn_name)
            }

            "mpsc_receiver::poll_next" => {
                ensure_func_ty::<(u32, u32, WasmContextPtr), i64>(ty, fn_name)
            }

            "mpsc_sender::poll_ready" | "mpsc_sender::poll_flush" => {
                ensure_func_ty::<(u32, u32, WasmContextPtr), i32>(ty, fn_name)
            }
            "mpsc_sender::start_send" => ensure_func_ty::<(u32, u32, u32, u32), ()>(ty, fn_name),

            "timer::now" => ensure_func_ty::<(), i64>(ty, fn_name),
            "timer::new" => ensure_func_ty::<i64, TimerId>(ty, fn_name),
            "timer::drop" => ensure_func_ty::<TimerId, ()>(ty, fn_name),
            "timer::poll" => ensure_func_ty::<(TimerId, WasmContextPtr), i64>(ty, fn_name),

            "panic" => ensure_func_ty::<(u32, u32, u32, u32, u32, u32), ()>(ty, fn_name),

            other => {
                bail!(
                    "Unknown import from `{}` module: `{}`",
                    Self::RT_MODULE,
                    other
                );
            }
        }
    }
}

impl ExtendLinker for WorkflowFunctions {
    const MODULE_NAME: &'static str = "tardigrade_rt";

    fn functions(&self, store: &mut Store<WorkflowData>) -> Vec<(&'static str, Func)> {
        vec![
            // Task functions
            (
                "task::poll_completion",
                wrap2(&mut *store, Self::poll_task_completion),
            ),
            ("task::spawn", wrap3(&mut *store, Self::spawn_task)),
            ("task::wake", wrap1(&mut *store, Self::wake_task)),
            (
                "task::abort",
                wrap1(&mut *store, Self::schedule_task_abortion),
            ),
            // Data input functions
            ("data_input::get", wrap2(&mut *store, Self::get_data_input)),
            // Channel functions
            ("mpsc_receiver::get", wrap2(&mut *store, Self::get_receiver)),
            (
                "mpsc_receiver::poll_next",
                wrap3(&mut *store, Self::poll_next_for_receiver),
            ),
            ("mpsc_sender::get", wrap2(&mut *store, Self::get_sender)),
            (
                "mpsc_sender::poll_ready",
                wrap3(&mut *store, Self::poll_ready_for_sender),
            ),
            (
                "mpsc_sender::start_send",
                wrap4(&mut *store, Self::start_send),
            ),
            (
                "mpsc_sender::poll_flush",
                wrap3(&mut *store, Self::poll_flush_for_sender),
            ),
            // Timer functions
            ("timer::now", wrap0(&mut *store, Self::current_timestamp)),
            ("timer::new", wrap1(&mut *store, Self::create_timer)),
            ("timer::drop", wrap1(&mut *store, Self::drop_timer)),
            ("timer::poll", wrap2(&mut *store, Self::poll_timer)),
            // Panic hook
            ("panic", wrap6(&mut *store, Self::report_panic)),
        ]
    }
}

macro_rules! impl_wrapper {
    ($fn_name:ident => $($arg:ident : $arg_ty:ident),*) => {
        fn $fn_name<R, $($arg_ty,)*>(
            store: &mut Store<WorkflowData>,
            function: fn(StoreContextMut<'_, WorkflowData>, $($arg_ty,)*) -> R,
        ) -> Func
        where
            R: 'static + WasmRet,
            $($arg_ty: 'static + WasmTy,)*
        {
            Func::wrap(store, move |mut caller: Caller<'_, WorkflowData>, $($arg,)*| {
                function(caller.as_context_mut(), $($arg,)*)
            })
        }
    };
}

impl_wrapper!(wrap0 =>);
impl_wrapper!(wrap1 => a: A);
impl_wrapper!(wrap2 => a: A, b: B);
impl_wrapper!(wrap3 => a: A, b: B, c: C);
impl_wrapper!(wrap4 => a: A, b: B, c: C, d: D);
impl_wrapper!(wrap6 => a: A, b: B, c: C, d: D, e: E, f: F);
