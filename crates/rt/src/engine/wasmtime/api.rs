//! Expected exports from workflow WASM modules.

use anyhow::{anyhow, bail, ensure, Context};
use wasmtime::{
    AsContextMut, Caller, ExternRef, ExternType, Func, Instance, Memory, Module, Store,
    StoreContextMut, Table, TypedFunc, ValType, WasmParams, WasmResults, WasmRet, WasmTy,
};

use std::{collections::HashMap, fmt, str, task::Poll};

use super::{
    functions::{SpawnFunctions, TracingFunctions, WasmContextPtr, WorkflowFunctions},
    instance::{HostResource, InstanceData},
    module::ExtendLinker,
    WasmAllocator,
};
use tardigrade::{
    abi::{IntoWasm, TryFromWasm},
    interface::Interface,
    spawn::HostError,
    ChannelId, TaskId, TimerId, WakerId, WorkflowId,
};

type Ref = Option<ExternRef>;

fn ensure_func_ty<Args, Out>(ty: &ExternType, fn_name: &str) -> anyhow::Result<()>
where
    Args: WasmParams,
    Out: WasmResults,
{
    let ty = ty
        .func()
        .ok_or_else(|| anyhow!("`{fn_name}` is not a function"))?;

    Args::typecheck(ty.params())
        .with_context(|| format!("`{fn_name}` function has incorrect param types"))?;
    Out::typecheck(ty.results())
        .with_context(|| format!("`{fn_name}` function has incorrect return types"))
}

#[derive(Clone, Copy)]
pub(super) struct ModuleExports {
    pub memory: Memory,
    pub data_location: Option<(u32, u32)>,
    pub ref_table: Table,
    create_main_task: TypedFunc<(u32, u32, Ref), TaskId>,
    poll_task: TypedFunc<TaskId, i32>,
    drop_task: TypedFunc<TaskId, ()>,
    alloc_bytes: TypedFunc<u32, u32>,
    create_waker: TypedFunc<WasmContextPtr, WakerId>,
    wake_waker: TypedFunc<WakerId, ()>,
    drop_waker: TypedFunc<WakerId, ()>,
    initialize_child: TypedFunc<(WorkflowId, Ref, i64), ()>,
    initialize_channel: TypedFunc<(ChannelId, Ref, Ref), ()>,
}

impl ModuleExports {
    #[tracing::instrument(level = "debug", skip_all, ret, err, fields(args.len = raw_args.len()))]
    pub fn create_main_task(
        &self,
        mut ctx: StoreContextMut<'_, InstanceData>,
        raw_args: &[u8],
    ) -> anyhow::Result<TaskId> {
        let data_len = u32::try_from(raw_args.len()).expect("data is too large");
        let data_ptr = self.alloc_bytes(ctx.as_context_mut(), data_len)?;
        self.memory
            .write(ctx.as_context_mut(), data_ptr as usize, raw_args)
            .context("cannot write workflow args to WASM memory")?;

        let channel_ids = ctx.data().inner.persisted.channels().to_ids();
        let handles = HostResource::channel_handles(channel_ids).into_ref();
        self.create_main_task
            .call(ctx, (data_ptr, data_len, Some(handles)))
    }

    #[tracing::instrument(level = "debug", skip(self, ctx), ret, err)]
    pub fn poll_task(
        &self,
        ctx: StoreContextMut<'_, InstanceData>,
        task_id: TaskId,
    ) -> anyhow::Result<Poll<()>> {
        let res = self.poll_task.call(ctx, task_id)?;
        <Poll<()>>::try_from_wasm(res).map_err(From::from)
    }

    #[tracing::instrument(level = "debug", skip(self, ctx), err)]
    pub fn drop_task(
        &self,
        ctx: StoreContextMut<'_, InstanceData>,
        task_id: TaskId,
    ) -> anyhow::Result<()> {
        self.drop_task.call(ctx, task_id)
    }

    #[tracing::instrument(level = "trace", skip(self, ctx), ret, err)]
    pub fn alloc_bytes(
        &self,
        ctx: StoreContextMut<'_, InstanceData>,
        capacity: u32,
    ) -> anyhow::Result<u32> {
        self.alloc_bytes.call(ctx, capacity)
    }

    #[tracing::instrument(level = "debug", skip(self, ctx), ret, err)]
    pub fn create_waker(
        &self,
        ctx: StoreContextMut<'_, InstanceData>,
        cx_ptr: WasmContextPtr,
    ) -> anyhow::Result<WakerId> {
        self.create_waker.call(ctx, cx_ptr)
    }

    #[tracing::instrument(level = "debug", skip(self, ctx), err)]
    pub fn wake_waker(
        &self,
        ctx: StoreContextMut<'_, InstanceData>,
        waker_id: WakerId,
    ) -> anyhow::Result<()> {
        self.wake_waker.call(ctx, waker_id)
    }

    #[tracing::instrument(level = "debug", skip(self, ctx), err)]
    pub fn drop_waker(
        &self,
        ctx: StoreContextMut<'_, InstanceData>,
        waker_id: WakerId,
    ) -> anyhow::Result<()> {
        self.drop_waker.call(ctx, waker_id)
    }

    #[tracing::instrument(level = "debug", skip(self, ctx), err)]
    pub fn initialize_child(
        &self,
        mut ctx: StoreContextMut<'_, InstanceData>,
        stub_id: WorkflowId,
        result: Result<WorkflowId, HostError>,
    ) -> anyhow::Result<()> {
        let mut child_id = None;
        let result = result.map(|id| {
            child_id = Some(id);
        });
        let result = result.into_wasm(&mut WasmAllocator::new(ctx.as_context_mut()))?;
        let child = child_id.map(|id| HostResource::Workflow(id).into_ref());

        self.initialize_child.call(ctx, (stub_id, child, result))
    }

    #[tracing::instrument(level = "debug", skip(self, ctx), err)]
    pub fn initialize_channel(
        &self,
        ctx: StoreContextMut<'_, InstanceData>,
        stub_id: ChannelId,
        channel_id: ChannelId,
    ) -> anyhow::Result<()> {
        let sender = Some(HostResource::Sender(channel_id).into_ref());
        let receiver = Some(HostResource::Receiver(channel_id).into_ref());
        self.initialize_channel
            .call(ctx, (stub_id, sender, receiver))
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
    pub(crate) fn validate_module(
        module: &Module,
        workflows: &HashMap<String, Interface>,
    ) -> anyhow::Result<()> {
        let memory_ty = module
            .get_export("memory")
            .ok_or_else(|| anyhow!("module does not export memory"))?;
        ensure!(
            matches!(memory_ty, ExternType::Memory(_)),
            "`memory` export is not a memory"
        );

        let refs_ty = module
            .get_export("externrefs")
            .ok_or_else(|| anyhow!("module does not export externrefs table"))?;
        ensure!(
            matches!(
                refs_ty,
                ExternType::Table(table_ty) if table_ty.element() == ValType::ExternRef
            ),
            "`externrefs` export is not a table of `externref`s"
        );

        for workflow_name in workflows.keys() {
            Self::ensure_export_ty::<(u32, u32, Ref), TaskId>(
                module,
                &format!("tardigrade_rt::spawn::{workflow_name}"),
            )?;
        }
        Self::ensure_export_ty::<TaskId, i32>(module, "tardigrade_rt::poll_task")?;
        Self::ensure_export_ty::<TaskId, ()>(module, "tardigrade_rt::drop_task")?;
        Self::ensure_export_ty::<u32, u32>(module, "tardigrade_rt::alloc_bytes")?;
        Self::ensure_export_ty::<WasmContextPtr, WakerId>(module, "tardigrade_rt::create_waker")?;
        Self::ensure_export_ty::<WakerId, ()>(module, "tardigrade_rt::wake_waker")?;
        Self::ensure_export_ty::<WakerId, ()>(module, "tardigrade_rt::drop_waker")?;
        Self::ensure_export_ty::<(WorkflowId, Ref, i64), ()>(module, "tardigrade_rt::init_child")?;
        Self::ensure_export_ty::<(ChannelId, Ref, Ref), ()>(module, "tardigrade_rt::init_channel")?;

        Ok(())
    }

    fn ensure_export_ty<Args, Out>(module: &Module, fn_name: &str) -> anyhow::Result<()>
    where
        Args: WasmParams,
        Out: WasmResults,
    {
        let ty = module
            .get_export(fn_name)
            .ok_or_else(|| anyhow!("module does not export `{fn_name}` function"))?;
        ensure_func_ty::<Args, Out>(&ty, fn_name)
    }

    pub fn new(store: &mut Store<InstanceData>, instance: &Instance, workflow_name: &str) -> Self {
        let memory = instance.get_memory(&mut *store, "memory").unwrap();
        let ref_table = instance.get_table(&mut *store, "externrefs").unwrap();
        let global_base = Self::extract_u32_global(&mut *store, instance, "__global_base");
        let heap_base = Self::extract_u32_global(&mut *store, instance, "__heap_base");
        let data_location = global_base.and_then(|start| heap_base.map(|end| (start, end)));
        let main_fn_name = format!("tardigrade_rt::spawn::{workflow_name}");

        Self {
            memory,
            data_location,
            ref_table,
            create_main_task: Self::extract_function(store, instance, &main_fn_name),
            poll_task: Self::extract_function(store, instance, "tardigrade_rt::poll_task"),
            drop_task: Self::extract_function(store, instance, "tardigrade_rt::drop_task"),
            alloc_bytes: Self::extract_function(store, instance, "tardigrade_rt::alloc_bytes"),
            create_waker: Self::extract_function(store, instance, "tardigrade_rt::create_waker"),
            wake_waker: Self::extract_function(store, instance, "tardigrade_rt::wake_waker"),
            drop_waker: Self::extract_function(store, instance, "tardigrade_rt::drop_waker"),
            initialize_child: Self::extract_function(store, instance, "tardigrade_rt::init_child"),
            initialize_channel: Self::extract_function(
                store,
                instance,
                "tardigrade_rt::init_channel",
            ),
        }
    }

    #[allow(clippy::cast_sign_loss)] // intentional
    fn extract_u32_global(
        store: &mut Store<InstanceData>,
        instance: &Instance,
        name: &str,
    ) -> Option<u32> {
        let value = instance.get_global(&mut *store, name)?.get(&mut *store);
        value.i32().map(|value| value as u32)
    }

    fn extract_function<Args, Out>(
        store: &mut Store<InstanceData>,
        instance: &Instance,
        fn_name: &str,
    ) -> TypedFunc<Args, Out>
    where
        Args: WasmParams,
        Out: WasmResults,
    {
        instance
            .get_func(&mut *store, fn_name)
            .unwrap_or_else(|| panic!("function `{fn_name}` is not exported"))
            .typed(&*store)
            .with_context(|| format!("Function `{fn_name}` has incorrect signature"))
            .unwrap()
    }
}

#[derive(Debug)]
pub(super) struct ModuleImports;

impl ModuleImports {
    const RT_MODULE: &'static str = "tardigrade_rt";

    pub fn validate_module(module: &Module) -> anyhow::Result<()> {
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

            "channel::new" => ensure_func_ty::<ChannelId, ()>(ty, fn_name),

            "mpsc_receiver::poll_next" => ensure_func_ty::<(Ref, WasmContextPtr), i64>(ty, fn_name),

            "mpsc_sender::poll_ready" | "mpsc_sender::poll_flush" => {
                ensure_func_ty::<(Ref, WasmContextPtr), i32>(ty, fn_name)
            }
            "mpsc_sender::start_send" => ensure_func_ty::<(Ref, u32, u32), i32>(ty, fn_name),

            "timer::now" => ensure_func_ty::<(), i64>(ty, fn_name),
            "timer::new" => ensure_func_ty::<i64, TimerId>(ty, fn_name),
            "timer::drop" => ensure_func_ty::<TimerId, ()>(ty, fn_name),
            "timer::poll" => ensure_func_ty::<(TimerId, WasmContextPtr), i64>(ty, fn_name),

            "handles::create" => ensure_func_ty::<(), Ref>(ty, fn_name),
            "handles::insert_sender" | "handles::insert_receiver" => {
                ensure_func_ty::<(Ref, i32, i32, Ref), ()>(ty, fn_name)
            }
            "handles::remove" => ensure_func_ty::<(Ref, i32, i32, i32), Ref>(ty, fn_name),
            "resource::id" => ensure_func_ty::<Ref, u64>(ty, fn_name),
            "resource::drop" => ensure_func_ty::<Ref, ()>(ty, fn_name),
            "task::report_error" | "report_panic" => {
                ensure_func_ty::<(u32, u32, u32, u32, u32, u32), ()>(ty, fn_name)
            }

            other if other.starts_with("workflow::") => SpawnFunctions::validate_import(ty, other),

            other => {
                bail!(
                    "Unknown import from `{}` module: `{other}`",
                    Self::RT_MODULE
                );
            }
        }
    }
}

impl ExtendLinker for WorkflowFunctions {
    const MODULE_NAME: &'static str = "tardigrade_rt";

    fn functions(&self, store: &mut Store<InstanceData>) -> Vec<(&'static str, Func)> {
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
            (
                "task::report_error",
                wrap6(&mut *store, Self::report_task_error),
            ),
            // Channel functions
            ("channel::new", wrap1(&mut *store, Self::new_channel)),
            (
                "mpsc_receiver::poll_next",
                wrap2(&mut *store, Self::poll_next_for_receiver),
            ),
            (
                "mpsc_sender::poll_ready",
                wrap2(&mut *store, Self::poll_ready_for_sender),
            ),
            (
                "mpsc_sender::start_send",
                wrap3(&mut *store, Self::start_send),
            ),
            (
                "mpsc_sender::poll_flush",
                wrap2(&mut *store, Self::poll_flush_for_sender),
            ),
            // Timer functions
            ("timer::now", wrap0(&mut *store, Self::current_timestamp)),
            ("timer::new", wrap1(&mut *store, Self::create_timer)),
            ("timer::drop", wrap1(&mut *store, Self::drop_timer)),
            ("timer::poll", wrap2(&mut *store, Self::poll_timer)),
            // Resource management
            ("resource::id", Func::wrap(&mut *store, Self::resource_id)),
            ("resource::drop", wrap1(&mut *store, Self::drop_resource)),
            (
                "handles::create",
                Func::wrap(&mut *store, Self::create_handles),
            ),
            (
                "handles::insert_sender",
                wrap4(&mut *store, Self::insert_sender_into_handles),
            ),
            (
                "handles::insert_receiver",
                wrap4(&mut *store, Self::insert_receiver_into_handles),
            ),
            (
                "handles::remove",
                wrap4(&mut *store, Self::remove_from_handles),
            ),
            // Panic hook
            ("report_panic", wrap6(&mut *store, Self::report_panic)),
        ]
    }
}

impl SpawnFunctions {
    fn validate_import(ty: &ExternType, fn_name: &str) -> anyhow::Result<()> {
        match fn_name {
            "workflow::interface" => ensure_func_ty::<(u32, u32), i64>(ty, fn_name),
            "workflow::spawn" => {
                ensure_func_ty::<(WorkflowId, u32, u32, u32, u32, Ref), ()>(ty, fn_name)
            }
            "workflow::poll_completion" => {
                ensure_func_ty::<(Ref, WasmContextPtr), i64>(ty, fn_name)
            }
            "workflow::completion_error" => ensure_func_ty::<Ref, i64>(ty, fn_name),

            other => {
                bail!(
                    "Unknown import from `{}` module: `{other}`",
                    ModuleImports::RT_MODULE
                );
            }
        }
    }
}

impl ExtendLinker for SpawnFunctions {
    const MODULE_NAME: &'static str = "tardigrade_rt";

    fn functions(&self, store: &mut Store<InstanceData>) -> Vec<(&'static str, Func)> {
        vec![
            (
                "workflow::interface",
                wrap2(&mut *store, Self::workflow_interface),
            ),
            ("workflow::spawn", wrap6(&mut *store, Self::spawn)),
            (
                "workflow::poll_completion",
                wrap2(&mut *store, Self::poll_workflow_completion),
            ),
            (
                "workflow::completion_error",
                wrap1(&mut *store, Self::completion_error),
            ),
        ]
    }
}

impl ExtendLinker for TracingFunctions {
    const MODULE_NAME: &'static str = "tracing";

    fn functions(&self, store: &mut Store<InstanceData>) -> Vec<(&'static str, Func)> {
        vec![("send_trace", wrap2(&mut *store, Self::send_trace))]
    }
}

macro_rules! impl_wrapper {
    ($fn_name:ident => $($arg:ident : $arg_ty:ident),*) => {
        fn $fn_name<R, $($arg_ty,)*>(
            store: &mut Store<InstanceData>,
            function: fn(StoreContextMut<'_, InstanceData>, $($arg_ty,)*) -> R,
        ) -> Func
        where
            R: 'static + WasmRet,
            $($arg_ty: 'static + WasmTy,)*
        {
            Func::wrap(store, move |mut caller: Caller<'_, InstanceData>, $($arg,)*| {
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

#[cfg(test)]
mod tests {
    use wasmtime::{Engine, Linker};

    use std::sync::Arc;

    use super::*;
    use crate::{
        data::WorkflowData, engine::wasmtime::module::LowLevelExtendLinker, manager::Services,
        workflow::ChannelIds,
    };
    use tardigrade::interface::Interface;

    #[test]
    fn import_checks_are_consistent() {
        let interface = Interface::default();
        let services = Services {
            clock: Arc::new(()),
            stubs: None,
            tracer: None,
        };
        let data = WorkflowData::new(&interface, ChannelIds::default(), services);
        let data = InstanceData::from(data);
        let engine = Engine::default();
        let mut store = Store::new(&engine, data);
        let mut linker = Linker::new(&engine);

        WorkflowFunctions
            .extend_linker(&mut store, &mut linker)
            .unwrap();
        SpawnFunctions
            .extend_linker(&mut store, &mut linker)
            .unwrap();

        let linker_contents: Vec<_> = linker.iter(&mut store).collect();
        for (module, name, value) in linker_contents {
            assert_eq!(module, ModuleImports::RT_MODULE);
            ModuleImports::validate_import(&value.ty(&store), name).unwrap();
        }
    }
}
