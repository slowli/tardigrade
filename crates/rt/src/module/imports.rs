//! Imports for WASM modules provided by the Tardigrade runtime.

use anyhow::bail;
use wasmtime::{
    AsContextMut, Caller, ExternRef, ExternType, Func, Module, Store, StoreContextMut, WasmRet,
    WasmTy,
};

use std::str;

use crate::{
    data::{SpawnFunctions, WasmContextPtr, WorkflowData, WorkflowFunctions},
    module::{ensure_func_ty, ExtendLinker},
    TaskId, TimerId,
};

type Ref = Option<ExternRef>;

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

            "mpsc_receiver::get" | "mpsc_sender::get" => {
                ensure_func_ty::<(Ref, u32, u32, u32), Ref>(ty, fn_name)
            }

            "mpsc_receiver::poll_next" => ensure_func_ty::<(Ref, WasmContextPtr), i64>(ty, fn_name),

            "mpsc_sender::poll_ready" | "mpsc_sender::poll_flush" => {
                ensure_func_ty::<(Ref, WasmContextPtr), i32>(ty, fn_name)
            }
            "mpsc_sender::start_send" => ensure_func_ty::<(Ref, u32, u32), i32>(ty, fn_name),

            "timer::now" => ensure_func_ty::<(), i64>(ty, fn_name),
            "timer::new" => ensure_func_ty::<i64, TimerId>(ty, fn_name),
            "timer::drop" => ensure_func_ty::<TimerId, ()>(ty, fn_name),
            "timer::poll" => ensure_func_ty::<(TimerId, WasmContextPtr), i64>(ty, fn_name),

            "drop_ref" => ensure_func_ty::<Ref, ()>(ty, fn_name),
            "panic" => ensure_func_ty::<(u32, u32, u32, u32, u32, u32), ()>(ty, fn_name),

            other if other.starts_with("workflow::") => SpawnFunctions::validate_import(ty, other),

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
            // Channel functions
            ("mpsc_receiver::get", wrap4(&mut *store, Self::get_receiver)),
            (
                "mpsc_receiver::poll_next",
                wrap2(&mut *store, Self::poll_next_for_receiver),
            ),
            ("mpsc_sender::get", wrap4(&mut *store, Self::get_sender)),
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
            ("drop_ref", wrap1(&mut *store, Self::drop_ref)),
            // Panic hook
            ("panic", wrap6(&mut *store, Self::report_panic)),
        ]
    }
}

impl SpawnFunctions {
    fn validate_import(ty: &ExternType, fn_name: &str) -> anyhow::Result<()> {
        match fn_name {
            "workflow::interface" => ensure_func_ty::<(u32, u32), i64>(ty, fn_name),
            "workflow::create_handles" => ensure_func_ty::<(), Ref>(ty, fn_name),
            "workflow::insert_handle" => {
                ensure_func_ty::<(Ref, i32, u32, u32, i32), ()>(ty, fn_name)
            }
            "workflow::copy_sender_handle" => {
                ensure_func_ty::<(Ref, i32, i32, Ref), ()>(ty, fn_name)
            }
            "workflow::spawn" => ensure_func_ty::<(u32, u32, u32, u32, Ref, u32), Ref>(ty, fn_name),
            "workflow::poll_completion" => {
                ensure_func_ty::<(Ref, WasmContextPtr), i64>(ty, fn_name)
            }

            other => {
                bail!(
                    "Unknown import from `{}` module: `{}`",
                    ModuleImports::RT_MODULE,
                    other
                );
            }
        }
    }
}

impl ExtendLinker for SpawnFunctions {
    const MODULE_NAME: &'static str = "tardigrade_rt";

    fn functions(&self, store: &mut Store<WorkflowData>) -> Vec<(&'static str, Func)> {
        vec![
            (
                "workflow::interface",
                wrap2(&mut *store, Self::workflow_interface),
            ),
            (
                "workflow::create_handles",
                Func::wrap(&mut *store, Self::create_channel_handles),
            ),
            (
                "workflow::insert_handle",
                wrap5(&mut *store, Self::set_channel_handle),
            ),
            (
                "workflow::copy_sender_handle",
                wrap4(&mut *store, Self::copy_sender_handle),
            ),
            ("workflow::spawn", wrap6(&mut *store, Self::spawn)),
            (
                "workflow::poll_completion",
                wrap2(&mut *store, Self::poll_workflow_completion),
            ),
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
impl_wrapper!(wrap5 => a: A, b: B, c: C, d: D, e: E);
impl_wrapper!(wrap6 => a: A, b: B, c: C, d: D, e: E, f: F);

#[cfg(test)]
mod tests {
    use wasmtime::{Engine, Linker};

    use super::*;
    use crate::{
        module::{LowLevelExtendLinker, NoOpWorkflowManager, Services},
        test::MockScheduler,
        workflow::ChannelIds,
    };
    use tardigrade::interface::Interface;

    #[test]
    fn import_checks_are_consistent() {
        let interface = Interface::default();
        let services = Services {
            clock: &MockScheduler::default(),
            workflows: &NoOpWorkflowManager,
        };
        let state = WorkflowData::new(&interface, &ChannelIds::default(), services);
        let engine = Engine::default();
        let mut store = Store::new(&engine, state);
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
