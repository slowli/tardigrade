//! Mocks for `WorkflowModule`s.

use mimicry::{Answers, CallReal, Mock, MockGuard, Mut};
use wasmtime::{StoreContextMut, Trap};

use std::{
    collections::{HashMap, HashSet},
    task::Poll,
};

use super::*;
use crate::{module::WorkflowModule, TaskId, WakerId};

const INTERFACE: &[u8] = br#"{
    "v": 0,
    "in": { "orders": {} },
    "out": { "events": {}, "traces": { "capacity": null } }
}"#;

pub(crate) type MockPollFn = fn(StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap>;

#[derive(Default, Mock)]
#[mock(mut)]
pub(crate) struct ExportsMock {
    pub exports_created: bool,
    pub next_waker: WakerId,
    pub consumed_wakers: HashSet<WakerId>,
    pub dropped_tasks: HashSet<TaskId>,
    pub heap_pos: u32,
    pub poll_fns: Answers<MockPollFn, TaskId>,
}

#[allow(clippy::unnecessary_wraps)] // required by mock interface
impl ExportsMock {
    pub const MOCK_MODULE_BYTES: &'static [u8] = b"\0asm\x01\0\0\0";

    pub fn prepare(poll_fns: Answers<MockPollFn, TaskId>) -> MockGuard<Self> {
        let this = Self {
            heap_pos: 256, // prevent 0 pointers when allocating bytes
            poll_fns,
            ..Self::default()
        };
        this.set_as_mock()
    }

    pub(super) fn interfaces_from_wasm(
        this: &Mut<Self>,
        bytes: &[u8],
    ) -> anyhow::Result<HashMap<String, Interface>> {
        if bytes == Self::MOCK_MODULE_BYTES {
            let mut map = HashMap::with_capacity(1);
            map.insert("TestWorkflow".to_owned(), Interface::from_bytes(INTERFACE));
            Ok(map)
        } else {
            this.call_real(|| WorkflowModule::interfaces_from_wasm(bytes))
        }
    }

    pub(super) fn validate_module(
        _: &Mut<Self>,
        _: &Module,
        _: &HashMap<String, Interface>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
