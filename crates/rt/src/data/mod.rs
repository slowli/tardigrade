//! Workflow state.

use wasmtime::{AsContextMut, ExternRef, StoreContextMut, Trap};

use std::collections::HashMap;

mod channel;
mod helpers;
mod persistence;
mod task;
mod time;

pub use self::{
    channel::{ConsumeError, ConsumeErrorKind, InboundChannelState, OutboundChannelState},
    persistence::PersistError,
    task::TaskState,
    time::TimerState,
};
pub(crate) use self::{
    helpers::WasmContextPtr,
    persistence::{Refs, WorkflowState},
};

use self::{
    helpers::{CurrentExecution, Wakers},
    task::TaskQueue,
    time::Timers,
};
use crate::{
    data::helpers::HostResource,
    module::{ModuleExports, Services},
    receipt::{PanicInfo, PanicLocation, WakeUpCause},
    utils::copy_string_from_wasm,
    TaskId,
};
use tardigrade::interface::Interface;

#[derive(Debug)]
pub struct WorkflowData {
    /// Functions exported by the `Instance`. Instantiated immediately after instance.
    exports: Option<ModuleExports>,
    // Workflow interface, such as channels.
    interface: Interface<()>,
    inbound_channels: HashMap<String, InboundChannelState>,
    outbound_channels: HashMap<String, OutboundChannelState>,
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
    pub(crate) fn from_interface(interface: Interface<()>, services: Services) -> Self {
        let inbound_channels = interface
            .inbound_channels()
            .map(|(name, _)| (name.to_owned(), InboundChannelState::default()))
            .collect();
        let outbound_channels = interface
            .outbound_channels()
            .map(|(name, _)| (name.to_owned(), OutboundChannelState::default()))
            .collect();

        Self {
            exports: None,
            interface,
            inbound_channels,
            outbound_channels,
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

    #[cfg(test)]
    pub(crate) fn outbound_channel_wakers(&self) -> impl Iterator<Item = crate::WakerId> + '_ {
        self.outbound_channels
            .values()
            .flat_map(|state| &state.wakes_on_flush)
            .copied()
    }

    #[cfg(test)]
    pub(crate) fn inbound_channel_ref(name: impl Into<String>) -> ExternRef {
        HostResource::InboundChannel(name.into()).into_ref()
    }

    #[cfg(test)]
    pub(crate) fn outbound_channel_ref(name: impl Into<String>) -> ExternRef {
        HostResource::OutboundChannel(name.into()).into_ref()
    }
}

/// Functions operating on `WorkflowData` exported to WASM.
pub(crate) struct WorkflowFunctions;

impl WorkflowFunctions {
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
