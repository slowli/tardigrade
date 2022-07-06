use mimicry::{Answers, CallReal, Mock, MockGuard, Mut};
use wasmtime::MemoryType;

use std::collections::{HashMap, HashSet};

use super::*;

#[test]
fn import_checks_are_consistent() {
    let interface = Interface::default();
    let state = WorkflowData::from_interface(&interface, HashMap::new());
    let engine = Engine::default();
    let mut store = Store::new(&engine, state);
    let mut linker = Linker::new(&engine);

    WorkflowFunctions
        .extend_linker(&mut store, &mut linker)
        .unwrap();
    let linker_contents: Vec<_> = linker.iter(&mut store).collect();
    for (module, name, value) in linker_contents {
        assert_eq!(module, ModuleImports::RT_MODULE);
        ModuleImports::validate_import(&value.ty(&store), name).unwrap();
    }
}

const INTERFACE: &[u8] = br#"{
    "v": 0,
    "in": { "orders": {} },
    "out": { "events": {}, "traces": { "capacity": null } },
    "data": { "inputs": {} }
}"#;

pub(crate) type MockPollFn = fn(StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap>;

#[derive(Default, Mock)]
#[mock(mut)]
pub(crate) struct ExportsMock {
    pub exports_created: bool,
    pub next_waker: WakerId,
    pub consumed_wakers: HashSet<WakerId>,
    heap_pos: u32,
    poll_fns: Answers<MockPollFn>,
}

#[allow(clippy::unnecessary_wraps)] // required by mock interface
impl ExportsMock {
    pub const MOCK_MODULE_BYTES: &'static [u8] = b"\0asm\x01\0\0\0";

    pub fn prepare(poll_fns: Answers<MockPollFn>) -> MockGuard<Self> {
        let this = Self {
            poll_fns,
            ..Self::default()
        };
        this.set_as_mock()
    }

    pub(super) fn interface_from_wasm(
        this: &Mut<Self>,
        bytes: &[u8],
    ) -> anyhow::Result<Interface<()>> {
        if bytes == Self::MOCK_MODULE_BYTES {
            Ok(Interface::from_bytes(INTERFACE))
        } else {
            this.call_real(|| WorkflowModule::interface_from_wasm(bytes))
        }
    }

    pub(super) fn validate_module(_: &Mut<Self>, _: &Module) -> anyhow::Result<()> {
        Ok(())
    }

    pub(super) fn mock_new(
        this: &Mut<Self>,
        store: &mut Store<WorkflowData>,
        _: &Instance,
    ) -> ModuleExports {
        assert!(!this.borrow().exports_created);
        this.borrow().exports_created = true;

        let memory = Memory::new(&mut *store, MemoryType::new(1, None)).unwrap();
        // These functions are never called (we mock their calls), so we use
        // the simplest implementations possible.
        ModuleExports {
            memory,
            create_main_task: Func::wrap(&mut *store, || 0_u64).typed(&*store).unwrap(),
            poll_task: Func::wrap(&mut *store, |_: TaskId, _: TaskId| 0_i32)
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
        }
    }

    pub(super) fn create_main_task(
        _: &Mut<Self>,
        _: &ModuleExports,
        _: StoreContextMut<'_, WorkflowData>,
    ) -> Result<TaskId, Trap> {
        Ok(0)
    }

    pub(super) fn poll_task(
        this: &Mut<Self>,
        _: &ModuleExports,
        ctx: StoreContextMut<'_, WorkflowData>,
        task_id: TaskId,
    ) -> Result<Poll<()>, Trap> {
        assert_eq!(task_id, 0);
        let poll_fn = this.borrow().poll_fns.next_for(());
        poll_fn(ctx)
    }

    pub(super) fn drop_task(
        _: &Mut<Self>,
        _: &ModuleExports,
        _: StoreContextMut<'_, WorkflowData>,
        _: TaskId,
    ) -> Result<(), Trap> {
        unreachable!("should not be called")
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
        let mut this = this.borrow();
        assert!(waker_id < this.next_waker);
        assert!(
            this.consumed_wakers.insert(waker_id),
            "waker {} consumed twice",
            waker_id
        );
        WorkflowFunctions::wake_task(ctx, 0).unwrap();
        Ok(())
    }
}
