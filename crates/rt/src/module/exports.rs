//! Expected exports from workflow WASM modules.

use anyhow::{anyhow, ensure, Context};
use wasmtime::{
    AsContextMut, ExternType, Instance, Memory, Module, Store, StoreContextMut, Table, Trap,
    TypedFunc, ValType, WasmParams, WasmResults,
};

use std::{collections::HashMap, fmt, str, task::Poll};

use crate::{
    data::{WasmContextPtr, WorkflowData},
    module::ensure_func_ty,
};
use tardigrade::{abi::TryFromWasm, interface::Interface, TaskId, WakerId};

#[derive(Clone, Copy)]
pub(crate) struct ModuleExports {
    pub memory: Memory,
    pub data_location: Option<(u32, u32)>,
    pub ref_table: Table,
    create_main_task: TypedFunc<(u32, u32), TaskId>,
    poll_task: TypedFunc<TaskId, i32>,
    drop_task: TypedFunc<TaskId, ()>,
    alloc_bytes: TypedFunc<u32, u32>,
    create_waker: TypedFunc<WasmContextPtr, WakerId>,
    wake_waker: TypedFunc<WakerId, ()>,
    drop_waker: TypedFunc<WakerId, ()>,
}

#[cfg_attr(test, mimicry::mock(using = "super::tests::ExportsMock"))]
impl ModuleExports {
    pub fn create_main_task(
        &self,
        mut ctx: StoreContextMut<'_, WorkflowData>,
        raw_data: &[u8],
    ) -> Result<TaskId, Trap> {
        let data_len = u32::try_from(raw_data.len()).expect("data is too large");
        let data_ptr = self.alloc_bytes(ctx.as_context_mut(), data_len)?;
        self.memory
            .write(ctx.as_context_mut(), data_ptr as usize, raw_data)
            .map_err(|err| {
                let message = format!("cannot write to WASM memory: {}", err);
                Trap::new(message)
            })?;
        let result = self.create_main_task.call(ctx, (data_ptr, data_len));
        log_result!(result, "Created main task")
    }

    pub fn poll_task(
        &self,
        ctx: StoreContextMut<'_, WorkflowData>,
        task_id: TaskId,
    ) -> Result<Poll<()>, Trap> {
        let result = self
            .poll_task
            .call(ctx, task_id)
            .and_then(|res| <Poll<()>>::try_from_wasm(res).map_err(Trap::new));
        log_result!(result, "Polled task {task_id}")
    }

    pub fn drop_task(
        &self,
        ctx: StoreContextMut<'_, WorkflowData>,
        task_id: TaskId,
    ) -> Result<(), Trap> {
        let result = self.drop_task.call(ctx, task_id);
        log_result!(result, "Dropped task {task_id}")
    }

    pub fn alloc_bytes(
        &self,
        ctx: StoreContextMut<'_, WorkflowData>,
        capacity: u32,
    ) -> Result<u32, Trap> {
        let result = self.alloc_bytes.call(ctx, capacity);
        log_result!(result, "Allocated {capacity} bytes")
    }

    pub fn create_waker(
        &self,
        ctx: StoreContextMut<'_, WorkflowData>,
        cx_ptr: WasmContextPtr,
    ) -> Result<WakerId, Trap> {
        let result = self.create_waker.call(ctx, cx_ptr);
        log_result!(result, "Created waker from context {cx_ptr}")
    }

    pub fn wake_waker(
        &self,
        ctx: StoreContextMut<'_, WorkflowData>,
        waker_id: WakerId,
    ) -> Result<(), Trap> {
        let result = self.wake_waker.call(ctx, waker_id);
        log_result!(result, "Waked waker {waker_id}")
    }

    pub fn drop_waker(
        &self,
        ctx: StoreContextMut<'_, WorkflowData>,
        waker_id: WakerId,
    ) -> Result<(), Trap> {
        let result = self.drop_waker.call(ctx, waker_id);
        log_result!(result, "Dropped waker {waker_id}")
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
    pub(super) fn validate_module(
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
            Self::ensure_export_ty::<(u32, u32), TaskId>(
                module,
                &format!("tardigrade_rt::spawn::{}", workflow_name),
            )?;
        }
        Self::ensure_export_ty::<TaskId, i32>(module, "tardigrade_rt::poll_task")?;
        Self::ensure_export_ty::<TaskId, ()>(module, "tardigrade_rt::drop_task")?;
        Self::ensure_export_ty::<u32, u32>(module, "tardigrade_rt::alloc_bytes")?;
        Self::ensure_export_ty::<WasmContextPtr, WakerId>(module, "tardigrade_rt::create_waker")?;
        Self::ensure_export_ty::<WakerId, ()>(module, "tardigrade_rt::wake_waker")?;
        Self::ensure_export_ty::<WakerId, ()>(module, "tardigrade_rt::drop_waker")?;

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

    #[cfg_attr(test, mimicry::mock(using = "super::tests::ExportsMock::mock_new"))]
    pub fn new(store: &mut Store<WorkflowData>, instance: &Instance, workflow_name: &str) -> Self {
        let memory = instance.get_memory(&mut *store, "memory").unwrap();
        let ref_table = instance.get_table(&mut *store, "externrefs").unwrap();
        let global_base = Self::extract_u32_global(&mut *store, instance, "__global_base");
        let heap_base = Self::extract_u32_global(&mut *store, instance, "__heap_base");
        let data_location = global_base.and_then(|start| heap_base.map(|end| (start, end)));
        let main_fn_name = format!("tardigrade_rt::spawn::{}", workflow_name);

        Self {
            memory,
            data_location,
            ref_table,
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
            drop_waker: Self::extract_function(store, instance, "tardigrade_rt::drop_waker"),
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

#[cfg(test)]
mod tests {
    use mimicry::Mut;
    use wasmtime::{Func, MemoryType, TableType, Val};

    use super::*;
    use crate::{data::WorkflowFunctions, module::ExportsMock};

    #[allow(clippy::unnecessary_wraps)] // required by mock interface
    impl ExportsMock {
        pub(super) fn mock_new(
            this: &Mut<Self>,
            store: &mut Store<WorkflowData>,
            _: &Instance,
            _: &str,
        ) -> ModuleExports {
            this.borrow().exports_created = true;

            let memory = Memory::new(&mut *store, MemoryType::new(1, None)).unwrap();
            let ref_table = Table::new(
                &mut *store,
                TableType::new(ValType::ExternRef, 0, None),
                Val::ExternRef(None),
            );
            // These functions are never called (we mock their calls), so we use
            // the simplest implementations possible.
            ModuleExports {
                memory,
                data_location: None,
                ref_table: ref_table.unwrap(),
                create_main_task: Func::wrap(&mut *store, |_: u32, _: u32| 0_u64)
                    .typed(&*store)
                    .unwrap(),
                poll_task: Func::wrap(&mut *store, |_: TaskId| 0_i32)
                    .typed(&*store)
                    .unwrap(),
                drop_task: Func::wrap(&mut *store, drop::<TaskId>)
                    .typed(&*store)
                    .unwrap(),
                alloc_bytes: Func::wrap(&mut *store, |_: u32| 0_u32)
                    .typed(&*store)
                    .unwrap(),
                create_waker: Func::wrap(&mut *store, |_: WasmContextPtr| 0_u64)
                    .typed(&*store)
                    .unwrap(),
                wake_waker: Func::wrap(&mut *store, drop::<WakerId>)
                    .typed(&*store)
                    .unwrap(),
                drop_waker: Func::wrap(&mut *store, drop::<WakerId>)
                    .typed(&*store)
                    .unwrap(),
            }
        }

        pub(super) fn create_main_task(
            _: &Mut<Self>,
            _: &ModuleExports,
            _: StoreContextMut<'_, WorkflowData>,
            _: &[u8],
        ) -> Result<TaskId, Trap> {
            Ok(0)
        }

        pub(super) fn poll_task(
            this: &Mut<Self>,
            _: &ModuleExports,
            ctx: StoreContextMut<'_, WorkflowData>,
            task_id: TaskId,
        ) -> Result<Poll<()>, Trap> {
            let poll_fn = this.borrow().poll_fns.next_for(task_id);
            poll_fn(ctx)
        }

        pub(super) fn drop_task(
            this: &Mut<Self>,
            _: &ModuleExports,
            _: StoreContextMut<'_, WorkflowData>,
            task_id: TaskId,
        ) -> Result<(), Trap> {
            assert!(
                this.borrow().dropped_tasks.insert(task_id),
                "task {} dropped twice",
                task_id
            );
            Ok(())
        }

        pub(super) fn alloc_bytes(
            this: &Mut<Self>,
            _: &ModuleExports,
            _: StoreContextMut<'_, WorkflowData>,
            capacity: u32,
        ) -> Result<u32, Trap> {
            let ptr = this.borrow().heap_pos;
            this.borrow().heap_pos += capacity;
            Ok(ptr)
        }

        pub(super) fn create_waker(
            this: &Mut<Self>,
            _: &ModuleExports,
            _: StoreContextMut<'_, WorkflowData>,
            _: WasmContextPtr,
        ) -> Result<WakerId, Trap> {
            let mut this = this.borrow();
            let next_waker = this.next_waker;
            this.next_waker += 1;
            Ok(next_waker)
        }

        pub(super) fn wake_waker(
            this: &Mut<Self>,
            _: &ModuleExports,
            ctx: StoreContextMut<'_, WorkflowData>,
            waker_id: WakerId,
        ) -> Result<(), Trap> {
            this.borrow().consume_waker(waker_id);
            WorkflowFunctions::wake_task(ctx, 0).unwrap();
            Ok(())
        }

        fn consume_waker(&mut self, waker_id: WakerId) {
            assert!(waker_id < self.next_waker);
            assert!(
                self.consumed_wakers.insert(waker_id),
                "waker {} consumed twice",
                waker_id
            );
        }

        pub(super) fn drop_waker(
            this: &Mut<Self>,
            _: &ModuleExports,
            _: StoreContextMut<'_, WorkflowData>,
            waker_id: WakerId,
        ) -> Result<(), Trap> {
            this.borrow().consume_waker(waker_id);
            Ok(())
        }
    }
}
