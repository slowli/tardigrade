//! Workflow state.

use wasmtime::{AsContextMut, ExternRef, StoreContextMut, Trap};

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
pub(crate) use self::{
    helpers::WasmContextPtr,
    persistence::{Refs, WorkflowState},
};

use self::{
    channel::{InboundChannelState, OutboundChannelState},
    helpers::{CurrentExecution, Message, Wakers},
    task::TaskQueue,
    time::Timers,
};
use crate::data::helpers::HostResource;
use crate::{
    module::{ModuleExports, Services},
    receipt::{PanicInfo, PanicLocation, WakeUpCause},
    utils::{copy_string_from_wasm, WasmAllocator},
    TaskId,
};
use tardigrade::interface::Interface;
use tardigrade_shared::abi::IntoWasm;

#[derive(Debug)]
pub struct WorkflowData {
    /// Functions exported by the `Instance`. Instantiated immediately after instance.
    exports: Option<ModuleExports>,
    // Interfaces (channels, data inputs).
    interface: Interface<()>,
    inbound_channels: HashMap<String, InboundChannelState>,
    outbound_channels: HashMap<String, OutboundChannelState>,
    data_inputs: HashMap<String, Message>,
    timers: Timers,
    services: Services,
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
    pub(crate) fn from_interface(
        interface: Interface<()>,
        data_inputs: HashMap<String, Vec<u8>>,
        services: Services,
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
            interface,
            inbound_channels,
            outbound_channels,
            data_inputs,
            timers: Timers::new(services.clock.now()),
            services,
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

    pub(crate) fn interface(&self) -> &Interface<()> {
        &self.interface
    }

    pub(crate) fn data_input(&self, input_name: &str) -> Option<Vec<u8>> {
        self.data_inputs.get(input_name).map(Message::to_vec)
    }

    #[cfg(test)]
    pub(crate) fn outbound_channel_wakers(&self) -> impl Iterator<Item = crate::WakerId> + '_ {
        self.outbound_channels
            .values()
            .flat_map(|state| &state.wakes_on_flush)
            .copied()
    }

    #[cfg(test)]
    pub(crate) fn inbound_channel_ref(name: impl Into<String>) -> wasmtime::ExternRef {
        HostResource::InboundChannel(name.into()).into_ref()
    }

    #[cfg(test)]
    pub(crate) fn outbound_channel_ref(name: impl Into<String>) -> wasmtime::ExternRef {
        HostResource::OutboundChannel(name.into()).into_ref()
    }
}

/// Functions operating on `WorkflowData` exported to WASM.
pub(crate) struct WorkflowFunctions;

impl WorkflowFunctions {
    pub fn get_data_input(
        ctx: StoreContextMut<'_, WorkflowData>,
        input_name_ptr: u32,
        input_name_len: u32,
    ) -> Result<i64, Trap> {
        let memory = ctx.data().exports().memory;
        let input_name = copy_string_from_wasm(&ctx, &memory, input_name_ptr, input_name_len)?;
        let maybe_data = ctx.data().data_input(&input_name);

        crate::trace!(
            "Acquired data input `{}`: {}",
            input_name,
            maybe_data.as_ref().map_or_else(
                || "(no data)".to_owned(),
                |bytes| format!("{} bytes", bytes.len())
            )
        );
        maybe_data.into_wasm(&mut WasmAllocator::new(ctx))
    }

    #[allow(clippy::needless_pass_by_value)] // required by wasmtime
    pub fn drop_ref(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        dropped: Option<ExternRef>,
    ) -> Result<(), Trap> {
        let dropped = HostResource::from_ref(dropped.as_ref())?;
        if let HostResource::InboundChannel(name) = dropped {
            let wakers = ctx.data_mut().handle_inbound_channel_closure(name);
            let exports = ctx.data().exports();
            for waker in wakers {
                let result = exports.drop_waker(ctx.as_context_mut(), waker);
                let result = crate::log_result!(
                    result,
                    "Dropped waker {} for inbound channel `{}` closed by the workflow",
                    waker,
                    name
                );
                result.ok();
            }
        }
        Ok(())
    }

    pub fn report_panic(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        message_ptr: u32,
        message_len: u32,
        filename_ptr: u32,
        filename_len: u32,
        line: u32,
        column: u32,
    ) -> Result<(), Trap> {
        let memory = ctx.data().exports().memory;
        let message = if message_ptr == 0 {
            None
        } else {
            Some(copy_string_from_wasm(
                &ctx,
                &memory,
                message_ptr,
                message_len,
            )?)
        };
        let filename = if filename_ptr == 0 {
            None
        } else {
            Some(copy_string_from_wasm(
                &ctx,
                &memory,
                filename_ptr,
                filename_len,
            )?)
        };

        ctx.data_mut().current_execution().set_panic(PanicInfo {
            message,
            location: filename.map(|filename| PanicLocation {
                filename,
                line,
                column,
            }),
        });
        Ok(())
    }
}
