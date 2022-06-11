//! Module utils.

use anyhow::{anyhow, bail, ensure, Context};
use wasmtime::{
    AsContextMut, Engine, ExternType, Func, Instance, Linker, Memory, Store, Trap, TypedFunc,
    WasmParams, WasmResults,
};

use std::{fmt, task::Poll};

use crate::{
    state::{State, StateFunctions, WasmContextPtr},
    FutureId, TaskId, TimerId, WakerId,
};
use tardigrade_shared::workflow::{Interface, ValidateInterface};

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

pub struct WorkflowModule<W> {
    pub(crate) inner: wasmtime::Module,
    interface: Interface<W>,
}

impl<W> fmt::Debug for WorkflowModule<W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowModule")
            .field("module", &"_")
            .finish()
    }
}

impl WorkflowModule<()> {
    fn interface_from_wasm(module_bytes: &[u8]) -> anyhow::Result<Interface<()>> {
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
                    let interface: Interface<()> = serde_json::from_slice(section_bytes)?;
                    Self::check_internal_validity(&interface)?;
                    return Ok(interface);
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

    fn validate_module(module: &wasmtime::Module) -> anyhow::Result<()> {
        ModuleExports::validate_module(module)?;
        ModuleImports::validate_module(module)?;
        Ok(())
    }
}

impl<W> WorkflowModule<W> {
    pub fn interface(&self) -> &Interface<W> {
        &self.interface
    }
}

impl<W: ValidateInterface<()>> WorkflowModule<W> {
    /// Validates the provided module and wraps it.
    pub fn new(engine: &Engine, module_bytes: &[u8]) -> anyhow::Result<Self> {
        let module = wasmtime::Module::from_binary(engine, module_bytes)?;
        WorkflowModule::validate_module(&module)?;
        let interface = WorkflowModule::interface_from_wasm(module_bytes)?;
        Ok(Self {
            inner: module,
            interface: interface.downcast()?,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ModuleExports {
    pub memory: Memory,
    create_main_task: TypedFunc<(), TaskId>,
    poll_task: TypedFunc<(TaskId, TaskId), i32>,
    drop_task: TypedFunc<TaskId, ()>,
    alloc_bytes: TypedFunc<u32, u32>,
    create_waker: TypedFunc<WasmContextPtr, WakerId>,
    wake_waker: TypedFunc<WakerId, ()>,
}

impl ModuleExports {
    pub fn create_main_task(&self, cx: impl AsContextMut) -> Result<TaskId, Trap> {
        let result = self.create_main_task.call(cx, ());
        crate::log_result!(result, "Created main task")
    }

    pub fn poll_task(&self, cx: impl AsContextMut, task_id: TaskId) -> Result<Poll<()>, Trap> {
        let result = self
            .poll_task
            .call(cx, (task_id, task_id))
            .and_then(|raw| match raw {
                0 => Ok(Poll::Pending),
                1 => Ok(Poll::Ready(())),
                _ => Err(Trap::new("unexpected value returned by task polling")),
            });
        crate::log_result!(result, "Polled task {}", task_id)
    }

    pub fn drop_task(&self, cx: impl AsContextMut, task_id: TaskId) -> Result<(), Trap> {
        let result = self.drop_task.call(cx, task_id);
        crate::log_result!(result, "Dropped task {}", task_id)
    }

    pub fn alloc_bytes(&self, cx: impl AsContextMut, capacity: u32) -> Result<u32, Trap> {
        let result = self.alloc_bytes.call(cx, capacity);
        crate::log_result!(result, "Allocated {} bytes", capacity)
    }

    pub fn create_waker(
        &self,
        cx: impl AsContextMut,
        cx_ptr: WasmContextPtr,
    ) -> Result<WakerId, Trap> {
        let result = self.create_waker.call(cx, cx_ptr);
        crate::log_result!(result, "Created waker from context {}", cx_ptr)
    }

    pub fn wake_waker(&self, cx: impl AsContextMut, waker_id: WakerId) -> Result<(), Trap> {
        let result = self.wake_waker.call(cx, waker_id);
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
    fn validate_module(module: &wasmtime::Module) -> anyhow::Result<()> {
        let memory_ty = module
            .get_export("memory")
            .ok_or_else(|| anyhow!("module does not export memory"))?;
        ensure!(
            matches!(memory_ty, ExternType::Memory(_)),
            "`memory` export is not a memory"
        );

        Self::ensure_export_ty::<(), TaskId>(module, "__tardigrade_rt__main")?;
        Self::ensure_export_ty::<(TaskId, TaskId), i32>(module, "__tardigrade_rt__poll_task")?;
        Self::ensure_export_ty::<TaskId, ()>(module, "__tardigrade_rt__drop_task")?;
        Self::ensure_export_ty::<u32, u32>(module, "__tardigrade_rt__alloc_bytes")?;
        Self::ensure_export_ty::<WasmContextPtr, WakerId>(
            module,
            "__tardigrade_rt__context_create_waker",
        )?;
        Self::ensure_export_ty::<WakerId, ()>(module, "__tardigrade_rt__waker_wake")?;

        Ok(())
    }

    fn ensure_export_ty<Args, Out>(module: &wasmtime::Module, fn_name: &str) -> anyhow::Result<()>
    where
        Args: WasmParams,
        Out: WasmResults,
    {
        let ty = module
            .get_export(fn_name)
            .ok_or_else(|| anyhow!("module does not export `{}` function", fn_name))?;
        ensure_func_ty::<Args, Out>(&ty, fn_name)
    }

    pub fn new(store: &mut Store<State>, instance: &Instance) -> Self {
        let memory = instance.get_memory(&mut *store, "memory").unwrap();

        Self {
            memory,
            create_main_task: Self::extract_function(
                &mut *store,
                instance,
                "__tardigrade_rt__main",
            ),
            poll_task: Self::extract_function(&mut *store, instance, "__tardigrade_rt__poll_task"),
            drop_task: Self::extract_function(&mut *store, instance, "__tardigrade_rt__drop_task"),
            alloc_bytes: Self::extract_function(
                &mut *store,
                instance,
                "__tardigrade_rt__alloc_bytes",
            ),
            create_waker: Self::extract_function(
                store,
                instance,
                "__tardigrade_rt__context_create_waker",
            ),
            wake_waker: Self::extract_function(store, instance, "__tardigrade_rt__waker_wake"),
        }
    }

    fn extract_function<Args, Out>(
        store: &mut Store<State>,
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

    fn validate_module(module: &wasmtime::Module) -> anyhow::Result<()> {
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
            "task_poll_completion" => ensure_func_ty::<(TaskId, WasmContextPtr), i64>(ty, fn_name),
            "task_spawn" => ensure_func_ty::<(u32, u32, TaskId), ()>(ty, fn_name),
            "task_wake" | "task_schedule_abortion" => ensure_func_ty::<TaskId, ()>(ty, fn_name),

            "data_input_get" => ensure_func_ty::<(u32, u32), i64>(ty, fn_name),

            "mpsc_receiver_get" | "mpsc_sender_get" => {
                ensure_func_ty::<(u32, u32), i32>(ty, fn_name)
            }

            "mpsc_receiver_poll_next" => {
                ensure_func_ty::<(u32, u32, WasmContextPtr), i64>(ty, fn_name)
            }

            "mpsc_sender_poll_ready" | "mpsc_sender_poll_flush" => {
                ensure_func_ty::<(u32, u32, WasmContextPtr), i32>(ty, fn_name)
            }
            "mpsc_sender_start_send" => ensure_func_ty::<(u32, u32, u32, u32), ()>(ty, fn_name),

            "timer_new" => ensure_func_ty::<(i32, i64), TimerId>(ty, fn_name),
            "timer_drop" => ensure_func_ty::<TimerId, ()>(ty, fn_name),
            "timer_poll" => ensure_func_ty::<(TimerId, WasmContextPtr), i32>(ty, fn_name),

            "traced_future_new" => ensure_func_ty::<(u32, u32), FutureId>(ty, fn_name),
            "traced_future_update" => ensure_func_ty::<(FutureId, i32), ()>(ty, fn_name),

            other => {
                bail!(
                    "Unknown import from `{}` module: `{}`",
                    Self::RT_MODULE,
                    other
                );
            }
        }
    }

    pub fn extend_linker(
        store: &mut Store<State>,
        linker: &mut Linker<State>,
    ) -> anyhow::Result<()> {
        Self::import_task_functions(store, linker)?;
        Self::import_channel_functions(store, linker)?;
        Self::import_timer_functions(store, linker)?;
        Self::import_tracing_functions(store, linker)?;

        let data_input_get = Func::wrap(&mut *store, StateFunctions::data_input);
        linker.define(Self::RT_MODULE, "data_input_get", data_input_get)?;
        Ok(())
    }

    fn import_task_functions(
        store: &mut Store<State>,
        linker: &mut Linker<State>,
    ) -> anyhow::Result<()> {
        let poll_task_completion = Func::wrap(&mut *store, StateFunctions::poll_task_completion);
        linker.define(
            Self::RT_MODULE,
            "task_poll_completion",
            poll_task_completion,
        )?;
        let spawn_task = Func::wrap(&mut *store, StateFunctions::spawn_task);
        linker.define(Self::RT_MODULE, "task_spawn", spawn_task)?;
        let wake_task = Func::wrap(&mut *store, StateFunctions::wake_task);
        linker.define(Self::RT_MODULE, "task_wake", wake_task)?;
        let schedule_task_abortion = Func::wrap(store, StateFunctions::schedule_task_abortion);
        linker.define(
            Self::RT_MODULE,
            "task_schedule_abortion",
            schedule_task_abortion,
        )?;

        Ok(())
    }

    fn import_channel_functions(
        store: &mut Store<State>,
        linker: &mut Linker<State>,
    ) -> anyhow::Result<()> {
        let receiver = Func::wrap(&mut *store, StateFunctions::receiver);
        linker.define(Self::RT_MODULE, "mpsc_receiver_get", receiver)?;
        let poll_next_for_receiver =
            Func::wrap(&mut *store, StateFunctions::poll_next_for_receiver);
        linker.define(
            Self::RT_MODULE,
            "mpsc_receiver_poll_next",
            poll_next_for_receiver,
        )?;

        let sender = Func::wrap(&mut *store, StateFunctions::sender);
        linker.define(Self::RT_MODULE, "mpsc_sender_get", sender)?;
        let poll_ready_for_sender = Func::wrap(&mut *store, StateFunctions::poll_ready_for_sender);
        linker.define(
            Self::RT_MODULE,
            "mpsc_sender_poll_ready",
            poll_ready_for_sender,
        )?;
        let start_send = Func::wrap(&mut *store, StateFunctions::start_send);
        linker.define(Self::RT_MODULE, "mpsc_sender_start_send", start_send)?;
        let poll_flush_for_sender = Func::wrap(store, StateFunctions::poll_flush_for_sender);
        linker.define(
            Self::RT_MODULE,
            "mpsc_sender_poll_flush",
            poll_flush_for_sender,
        )?;
        Ok(())
    }

    fn import_timer_functions(
        store: &mut Store<State>,
        linker: &mut Linker<State>,
    ) -> anyhow::Result<()> {
        let create_timer = Func::wrap(&mut *store, StateFunctions::create_timer);
        linker.define(Self::RT_MODULE, "timer_new", create_timer)?;
        let drop_timer = Func::wrap(&mut *store, StateFunctions::drop_timer);
        linker.define(Self::RT_MODULE, "timer_drop", drop_timer)?;
        let poll_timer = Func::wrap(&mut *store, StateFunctions::poll_timer);
        linker.define(Self::RT_MODULE, "timer_poll", poll_timer)?;
        Ok(())
    }

    fn import_tracing_functions(
        store: &mut Store<State>,
        linker: &mut Linker<State>,
    ) -> anyhow::Result<()> {
        let create_future = Func::wrap(&mut *store, StateFunctions::create_traced_future);
        linker.define(Self::RT_MODULE, "traced_future_new", create_future)?;
        let update_future = Func::wrap(&mut *store, StateFunctions::update_traced_future);
        linker.define(Self::RT_MODULE, "traced_future_update", update_future)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    #[test]
    fn import_checks_are_consistent() {
        let interface = Interface::default();
        let state = State::from_interface(&interface, HashMap::new());
        let engine = Engine::default();
        let mut store = Store::new(&engine, state);
        let mut linker = Linker::new(&engine);

        ModuleImports::extend_linker(&mut store, &mut linker).unwrap();
        let linker_contents: Vec<_> = linker.iter(&mut store).collect();
        for (module, name, value) in linker_contents {
            assert_eq!(module, ModuleImports::RT_MODULE);
            ModuleImports::validate_import(&value.ty(&store), name).unwrap();
        }
    }
}
