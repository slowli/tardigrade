//! Workflow state.

use wasmtime::{Caller, Trap};

use std::collections::{HashMap, HashSet};

mod channel;
mod helpers;
mod persistence;
mod task;
mod time;

pub use self::{
    channel::{ConsumeError, ConsumeErrorKind},
    persistence::PersistError,
    task::TaskState,
    time::TimerState,
};
pub(crate) use self::{helpers::WasmContextPtr, persistence::WorkflowState};

use self::{
    channel::{InboundChannelState, OutboundChannelState},
    helpers::{CurrentExecution, Message, Wakers},
    task::TaskQueue,
    time::Timers,
};
use crate::{
    module::ModuleExports,
    receipt::WakeUpCause,
    utils::{copy_string_from_wasm, WasmAllocator},
    TaskId,
};
use tardigrade_shared::{abi::IntoWasm, workflow::Interface};

#[derive(Debug)]
pub struct WorkflowData {
    /// Functions exported by the `Instance`. Instantiated immediately after instance.
    exports: Option<ModuleExports>,
    inbound_channels: HashMap<String, InboundChannelState>,
    outbound_channels: HashMap<String, OutboundChannelState>,
    data_inputs: HashMap<String, Message>,
    timers: Timers,
    /// All tasks together with relevant info.
    tasks: HashMap<TaskId, TaskState>,
    /// Data related to the currently executing WASM call.
    current_execution: Option<CurrentExecution>,
    /// Tasks that should be polled after `current_task`.
    task_queue: TaskQueue,
    /// Wakers that need to be woken up.
    waker_queue: Vec<Wakers>,
    /// Wakeup cause set when waking up tasks.
    current_wakeup_cause: Option<WakeUpCause>,
}

impl WorkflowData {
    pub(crate) fn from_interface<W>(
        interface: &Interface<W>,
        data_inputs: HashMap<String, Vec<u8>>,
    ) -> Self {
        // Sanity-check correspondence of inputs to the interface.
        debug_assert_eq!(
            data_inputs
                .keys()
                .map(String::as_str)
                .collect::<HashSet<_>>(),
            interface
                .data_inputs()
                .map(|(name, _)| name)
                .collect::<HashSet<_>>()
        );

        let inbound_channels = interface
            .inbound_channels()
            .map(|(name, _)| (name.to_owned(), InboundChannelState::default()))
            .collect();
        let outbound_channels = interface
            .outbound_channels()
            .map(|(name, spec)| (name.to_owned(), OutboundChannelState::new(spec)))
            .collect();
        let data_inputs = data_inputs
            .into_iter()
            .map(|(name, bytes)| (name, bytes.into()))
            .collect();

        Self {
            exports: None,
            inbound_channels,
            outbound_channels,
            data_inputs,
            timers: Timers::new(),
            tasks: HashMap::new(),
            current_execution: None,
            task_queue: TaskQueue::default(),
            waker_queue: Vec::new(),
            current_wakeup_cause: None,
        }
    }

    pub(crate) fn exports(&self) -> ModuleExports {
        self.exports
            .expect("exports accessed before `State` is fully initialized")
    }

    pub(crate) fn set_exports(&mut self, exports: ModuleExports) {
        self.exports = Some(exports);
    }

    pub(crate) fn data_input(&self, input_name: &str) -> Option<Vec<u8>> {
        self.data_inputs.get(input_name).map(|data| data.to_vec())
    }
}

/// Functions operating on `WorkflowData` exported to WASM.
pub(crate) struct WorkflowFunctions;

impl WorkflowFunctions {
    pub fn get_data_input(
        caller: Caller<'_, WorkflowData>,
        input_name_ptr: u32,
        input_name_len: u32,
    ) -> Result<i64, Trap> {
        let memory = caller.data().exports().memory;
        let input_name = copy_string_from_wasm(&caller, &memory, input_name_ptr, input_name_len)?;
        let maybe_data = caller.data().data_input(&input_name);

        crate::trace!(
            "Acquired data input `{}`: {}",
            input_name,
            maybe_data
                .as_ref()
                .map(|bytes| format!("{} bytes", bytes.len()))
                .unwrap_or_else(|| "(no data)".to_owned())
        );
        maybe_data.into_wasm(&mut WasmAllocator::new(caller))
    }
}
