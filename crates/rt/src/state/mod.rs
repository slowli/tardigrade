//! Workflow state.

use std::collections::{HashMap, HashSet};

mod channel;
mod helpers;
mod persistence;
mod task;
mod time;

pub(crate) use self::helpers::{WasmContext, WasmContextPtr};
pub use self::{
    channel::{ConsumeError, ConsumeErrorKind},
    persistence::{PersistError, WorkflowState},
    time::TimerState,
};

use self::{
    channel::{InboundChannelState, OutboundChannelState},
    helpers::{CurrentExecution, Message, Wakers},
    task::{TaskQueue, TaskState},
    time::Timers,
};
use crate::{module::ModuleExports, TaskId, WakeUpCause};
use tardigrade_shared::workflow::Interface;

#[derive(Debug)]
pub(crate) struct State {
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

impl State {
    pub fn from_interface<W>(
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
            .map(|(name, _)| (name.to_owned(), OutboundChannelState::default()))
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

    pub fn exports(&self) -> ModuleExports {
        self.exports
            .expect("exports accessed before `State` is fully initialized")
    }

    pub fn set_exports(&mut self, exports: ModuleExports) {
        self.exports = Some(exports);
    }

    pub fn data_input(&self, input_name: &str) -> Option<Vec<u8>> {
        self.data_inputs.get(input_name).map(|data| data.to_vec())
    }
}
