//! Module utils.

use anyhow::{anyhow, bail, ensure, Context};
use wasmtime::{
    Caller, Engine, ExternType, Func, Instance, Linker, Memory, Store, Trap, TypedFunc,
};

use std::fmt;

use crate::{
    abi::{copy_bytes_from_wasm, copy_string_from_wasm, WasmAllocator},
    state::{State, WakerId, WasmContext, WasmContextPointer},
    time::{Timer, TimerId},
    TaskId,
};
use tardigrade_shared::{
    workflow::{Interface, ValidateInterface},
    ChannelErrorKind, IntoAbi, TimerKind,
};

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

pub(crate) struct ModuleExports {
    pub memory: Memory,
    pub create_main_task: TypedFunc<(), TaskId>,
    pub poll_task: TypedFunc<(TaskId, TaskId), i32>,
    pub drop_task: TypedFunc<TaskId, ()>,
    pub alloc_bytes: TypedFunc<u32, u32>,
    pub create_waker: TypedFunc<WasmContextPointer, WakerId>,
    pub wake_waker: TypedFunc<WakerId, ()>,
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
            .ok_or_else(|| anyhow!("Module does not export memory"))?;
        ensure!(
            matches!(memory_ty, ExternType::Memory(_)),
            "`memory` export is not a memory"
        );

        Self::ensure_export_ty::<(), TaskId>(module, "__tardigrade_rt__main")?;
        Self::ensure_export_ty::<(TaskId, TaskId), i32>(module, "__tardigrade_rt__poll_task")?;
        Self::ensure_export_ty::<TaskId, ()>(module, "__tardigrade_rt__drop_task")?;
        Self::ensure_export_ty::<u32, u32>(module, "__tardigrade_rt__alloc_bytes")?;
        Self::ensure_export_ty::<WasmContextPointer, WakerId>(
            module,
            "__tardigrade_rt__context_create_waker",
        )?;
        Self::ensure_export_ty::<WakerId, ()>(module, "__tardigrade_rt__waker_wake")?;

        Ok(())
    }

    fn ensure_export_ty<Args, Out>(module: &wasmtime::Module, fn_name: &str) -> anyhow::Result<()>
    where
        Args: wasmtime::WasmParams,
        Out: wasmtime::WasmResults,
    {
        let ty = module
            .get_export(fn_name)
            .ok_or_else(|| anyhow!("Module does not export `{}` function", fn_name))?;
        Self::ensure_func_ty::<Args, Out>(&ty, fn_name)
    }

    fn ensure_func_ty<Args, Out>(ty: &ExternType, fn_name: &str) -> anyhow::Result<()>
    where
        Args: wasmtime::WasmParams,
        Out: wasmtime::WasmResults,
    {
        let ty = ty
            .func()
            .ok_or_else(|| anyhow!("`{}` is not a function", fn_name))?;

        Args::typecheck(ty.params())
            .with_context(|| format!("`{}` function has incorrect param types", fn_name))?;
        Out::typecheck(ty.results())
            .with_context(|| format!("`{}` function has incorrect return types", fn_name))
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
        Args: wasmtime::WasmParams,
        Out: wasmtime::WasmResults,
    {
        instance
            .get_func(&mut *store, fn_name)
            .unwrap_or_else(|| panic!("Function `{}` is not exported", fn_name))
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

            match fn_name {
                "task_poll_completion" => {
                    ModuleExports::ensure_func_ty::<(TaskId, WasmContextPointer), i64>(
                        &ty, fn_name,
                    )?;
                }
                "task_spawn" => {
                    ModuleExports::ensure_func_ty::<(u32, u32, TaskId), ()>(&ty, fn_name)?;
                }
                "task_wake" | "task_schedule_abortion" => {
                    ModuleExports::ensure_func_ty::<TaskId, ()>(&ty, fn_name)?;
                }

                "data_input_get" => {
                    ModuleExports::ensure_func_ty::<(u32, u32), i64>(&ty, fn_name)?;
                }

                "mpsc_receiver_get" | "mpsc_sender_get" => {
                    ModuleExports::ensure_func_ty::<(u32, u32), i32>(&ty, fn_name)?;
                }

                "mpsc_receiver_poll_next" => {
                    ModuleExports::ensure_func_ty::<(u32, u32, WasmContextPointer), i64>(
                        &ty, fn_name,
                    )?;
                }

                "mpsc_sender_poll_ready" | "mpsc_sender_poll_flush" => {
                    ModuleExports::ensure_func_ty::<(u32, u32, WasmContextPointer), i32>(
                        &ty, fn_name,
                    )?;
                }
                "mpsc_sender_start_send" => {
                    ModuleExports::ensure_func_ty::<(u32, u32, u32, u32), ()>(&ty, fn_name)?;
                }

                "timer_new" => {
                    ModuleExports::ensure_func_ty::<(u32, u32, i32, i64), TimerId>(&ty, fn_name)?;
                }
                "timer_drop" => {
                    ModuleExports::ensure_func_ty::<TimerId, ()>(&ty, fn_name)?;
                }
                "timer_poll" => {
                    ModuleExports::ensure_func_ty::<(TimerId, WasmContextPointer), i32>(
                        &ty, fn_name,
                    )?;
                }

                other => {
                    bail!(
                        "Unknown import from `{}` module: `{}`",
                        Self::RT_MODULE,
                        other
                    );
                }
            }
        }

        Ok(())
    }

    pub fn extend_linker(
        store: &mut Store<State>,
        linker: &mut Linker<State>,
    ) -> anyhow::Result<()> {
        Self::import_task_functions(store, linker)?;
        Self::import_channel_functions(store, linker)?;
        Self::import_timer_functions(store, linker)?;

        let data_input_get = Func::wrap(&mut *store, Self::data_input_get);
        linker.define(Self::RT_MODULE, "data_input_get", data_input_get)?;
        Ok(())
    }

    fn import_task_functions(
        store: &mut Store<State>,
        linker: &mut Linker<State>,
    ) -> anyhow::Result<()> {
        let future_poll_task_completion =
            Func::wrap(&mut *store, Self::future_poll_task_completion);
        linker.define(
            Self::RT_MODULE,
            "task_poll_completion",
            future_poll_task_completion,
        )?;
        let future_spawn_task = Func::wrap(&mut *store, Self::future_spawn_task);
        linker.define(Self::RT_MODULE, "task_spawn", future_spawn_task)?;
        let future_wake_task = Func::wrap(&mut *store, Self::future_wake_task);
        linker.define(Self::RT_MODULE, "task_wake", future_wake_task)?;
        let future_schedule_task_abortion = Func::wrap(store, Self::future_schedule_task_abortion);
        linker.define(
            Self::RT_MODULE,
            "task_schedule_abortion",
            future_schedule_task_abortion,
        )?;

        Ok(())
    }

    fn future_poll_task_completion(
        mut caller: Caller<'_, State>,
        task: TaskId,
        cx: WasmContextPointer,
    ) -> Result<i64, Trap> {
        let mut cx = WasmContext::new(cx);
        let poll_result = caller.data_mut().poll_task_completion(task, &mut cx);
        cx.save_waker(&mut caller)?;
        poll_result.into_abi(&mut WasmAllocator::new(caller))
    }

    fn future_spawn_task(
        mut caller: Caller<'_, State>,
        task_name_ptr: u32,
        task_name_len: u32,
        task: TaskId,
    ) -> Result<(), Trap> {
        let memory = &caller.data().exports().memory;
        let task_name = copy_string_from_wasm(&caller, memory, task_name_ptr, task_name_len)?;
        caller.data_mut().spawn_task(task, task_name)
    }

    fn future_wake_task(mut caller: Caller<'_, State>, task: TaskId) -> Result<(), Trap> {
        caller.data_mut().schedule_task_wakeup(task)
    }

    fn future_schedule_task_abortion(
        mut caller: Caller<'_, State>,
        task: TaskId,
    ) -> Result<(), Trap> {
        caller.data_mut().schedule_task_abortion(task)
    }

    fn import_channel_functions(
        store: &mut Store<State>,
        linker: &mut Linker<State>,
    ) -> anyhow::Result<()> {
        let mpsc_receiver_get = Func::wrap(&mut *store, Self::mpsc_receiver_get);
        linker.define(Self::RT_MODULE, "mpsc_receiver_get", mpsc_receiver_get)?;
        let mpsc_receiver_poll_next = Func::wrap(&mut *store, Self::mpsc_receiver_poll_next);
        linker.define(
            Self::RT_MODULE,
            "mpsc_receiver_poll_next",
            mpsc_receiver_poll_next,
        )?;

        let mpsc_sender_get = Func::wrap(&mut *store, Self::mpsc_sender_get);
        linker.define(Self::RT_MODULE, "mpsc_sender_get", mpsc_sender_get)?;
        let mpsc_sender_poll_ready = Func::wrap(&mut *store, Self::mpsc_sender_poll_ready);
        linker.define(
            Self::RT_MODULE,
            "mpsc_sender_poll_ready",
            mpsc_sender_poll_ready,
        )?;
        let mpsc_sender_start_send = Func::wrap(&mut *store, Self::mpsc_sender_start_send);
        linker.define(
            Self::RT_MODULE,
            "mpsc_sender_start_send",
            mpsc_sender_start_send,
        )?;
        let mpsc_sender_poll_flush = Func::wrap(store, Self::mpsc_sender_poll_flush);
        linker.define(
            Self::RT_MODULE,
            "mpsc_sender_poll_flush",
            mpsc_sender_poll_flush,
        )?;
        Ok(())
    }

    fn mpsc_receiver_get(
        mut caller: Caller<'_, State>,
        channel_name_ptr: u32,
        channel_name_len: u32,
    ) -> Result<i32, Trap> {
        let memory = &caller.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&caller, memory, channel_name_ptr, channel_name_len)?;
        caller
            .data_mut()
            .acquire_inbound_channel(&channel_name)
            .into_abi(&mut WasmAllocator::new(caller))
    }

    fn mpsc_receiver_poll_next(
        mut caller: Caller<'_, State>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        cx: WasmContextPointer,
    ) -> Result<i64, Trap> {
        let memory = &caller.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&caller, memory, channel_name_ptr, channel_name_len)?;
        dbg!("recv", &channel_name);

        let mut cx = WasmContext::new(cx);
        let poll_result = caller
            .data_mut()
            .poll_inbound_channel(&channel_name, &mut cx)?;
        cx.save_waker(&mut caller)?;
        poll_result.into_abi(&mut WasmAllocator::new(caller))
    }

    fn mpsc_sender_get(
        caller: Caller<'_, State>,
        channel_name_ptr: u32,
        channel_name_len: u32,
    ) -> Result<i32, Trap> {
        let memory = &caller.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&caller, memory, channel_name_ptr, channel_name_len)?;
        let result = if caller.data().has_outbound_channel(&channel_name) {
            Ok(())
        } else {
            Err(ChannelErrorKind::Unknown)
        };
        result.into_abi(&mut WasmAllocator::new(caller))
    }

    fn mpsc_sender_poll_ready(
        mut caller: Caller<'_, State>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        cx: WasmContextPointer,
    ) -> Result<i32, Trap> {
        dbg!("mpsc_sender_poll_ready");
        let memory = &caller.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&caller, memory, channel_name_ptr, channel_name_len)?;

        let mut cx = WasmContext::new(cx);
        let poll_result = caller
            .data_mut()
            .poll_outbound_channel(&channel_name, false, &mut cx)?;
        cx.save_waker(&mut caller)?;
        poll_result.into_abi(&mut WasmAllocator::new(caller))
    }

    fn mpsc_sender_start_send(
        mut caller: Caller<'_, State>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        message_ptr: u32,
        message_len: u32,
    ) -> Result<(), Trap> {
        dbg!("mpsc_sender_start_send");
        let memory = &caller.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&caller, memory, channel_name_ptr, channel_name_len)?;
        let message = copy_bytes_from_wasm(&caller, memory, message_ptr, message_len)?;
        caller
            .data_mut()
            .push_outbound_message(&channel_name, message)
    }

    fn mpsc_sender_poll_flush(
        mut caller: Caller<'_, State>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        cx: WasmContextPointer,
    ) -> Result<i32, Trap> {
        dbg!("mpsc_sender_poll_flush");
        let memory = &caller.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&caller, memory, channel_name_ptr, channel_name_len)?;

        let mut cx = WasmContext::new(cx);
        let poll_result = caller
            .data_mut()
            .poll_outbound_channel(&channel_name, true, &mut cx)?;
        cx.save_waker(&mut caller)?;
        poll_result.into_abi(&mut WasmAllocator::new(caller))
    }

    fn data_input_get(
        caller: Caller<'_, State>,
        input_name_ptr: u32,
        input_name_len: u32,
    ) -> Result<i64, Trap> {
        let memory = &caller.data().exports().memory;
        let input_name = copy_string_from_wasm(&caller, memory, input_name_ptr, input_name_len)?;
        caller
            .data()
            .data_input(&input_name)
            .into_abi(&mut WasmAllocator::new(caller))
    }

    fn import_timer_functions(
        store: &mut Store<State>,
        linker: &mut Linker<State>,
    ) -> anyhow::Result<()> {
        let timer_new = Func::wrap(&mut *store, Self::timer_new);
        linker.define(Self::RT_MODULE, "timer_new", timer_new)?;
        let timer_drop = Func::wrap(&mut *store, Self::timer_drop);
        linker.define(Self::RT_MODULE, "timer_drop", timer_drop)?;
        let timer_poll = Func::wrap(&mut *store, Self::timer_poll);
        linker.define(Self::RT_MODULE, "timer_poll", timer_poll)?;
        Ok(())
    }

    fn timer_new(
        mut caller: Caller<'_, State>,
        timer_name_ptr: u32,
        timer_name_len: u32,
        timer_kind: i32,
        timer_value: i64,
    ) -> Result<TimerId, Trap> {
        dbg!("timer_new");
        let timer_kind =
            TimerKind::try_from(timer_kind).map_err(|err| Trap::new(err.to_string()))?;
        let memory = &caller.data().exports().memory;
        let timer_name = copy_string_from_wasm(&caller, memory, timer_name_ptr, timer_name_len)?;
        dbg!(&timer_name);
        Ok(caller
            .data_mut()
            .timers_mut()
            .insert(timer_name, dbg!(Timer::from_raw(timer_kind, timer_value))))
    }

    fn timer_drop(mut caller: Caller<'_, State>, timer_id: TimerId) -> Result<(), Trap> {
        dbg!(timer_id);
        caller.data_mut().timers_mut().drop(timer_id)
    }

    fn timer_poll(
        mut caller: Caller<'_, State>,
        timer_id: TimerId,
        cx: WasmContextPointer,
    ) -> Result<i32, Trap> {
        dbg!("timer_poll", timer_id);

        let mut cx = WasmContext::new(cx);
        let poll_result = caller.data_mut().poll_timer(timer_id, &mut cx)?;
        cx.save_waker(&mut caller)?;
        poll_result.into_abi(&mut WasmAllocator::new(caller))
    }
}
