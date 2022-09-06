//! Workflow state.

use wasmtime::{AsContextMut, ExternRef, StoreContextMut, Trap};

use std::collections::{HashMap, HashSet};

mod channel;
mod helpers;
mod persistence;
mod spawn;
mod task;
mod time;

pub use self::{
    channel::{ConsumeError, ConsumeErrorKind},
    persistence::{InboundChannelState, OutboundChannelState, PersistError},
    spawn::ChildWorkflowState,
    task::TaskState,
    time::TimerState,
};
pub(crate) use self::{
    helpers::{Wakers, WasmContextPtr},
    persistence::{Refs, WorkflowState},
    spawn::SpawnFunctions,
};

use self::{channel::ChannelStates, helpers::CurrentExecution, task::TaskQueue, time::Timers};
use crate::{
    data::helpers::HostResource,
    module::ModuleExports,
    receipt::{PanicInfo, PanicLocation, WakeUpCause},
    services::Services,
    utils::copy_string_from_wasm,
    workflow::ChannelIds,
    TaskId, WorkflowId,
};
use tardigrade::interface::Interface;

#[derive(Debug)]
pub struct WorkflowData {
    /// Functions exported by the `Instance`. Instantiated immediately after instance.
    exports: Option<ModuleExports>,
    channels: ChannelStates,
    timers: Timers,
    /// Services available to the workflow.
    services: Services,
    /// All tasks together with relevant info.
    tasks: HashMap<TaskId, TaskState>,
    /// Workflows spawned by this workflow.
    child_workflows: HashMap<WorkflowId, ChildWorkflowState>,
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
    pub(crate) fn new(
        interface: &Interface<()>,
        channel_ids: &ChannelIds,
        services: Services,
    ) -> Self {
        debug_assert_eq!(
            interface
                .inbound_channels()
                .map(|(name, _)| name)
                .collect::<HashSet<_>>(),
            channel_ids
                .inbound
                .keys()
                .map(String::as_str)
                .collect::<HashSet<_>>()
        );
        debug_assert_eq!(
            interface
                .outbound_channels()
                .map(|(name, _)| name)
                .collect::<HashSet<_>>(),
            channel_ids
                .outbound
                .keys()
                .map(String::as_str)
                .collect::<HashSet<_>>()
        );

        Self {
            exports: None,
            channels: ChannelStates::new(channel_ids, |name| {
                interface.outbound_channel(name).unwrap().capacity
            }),
            timers: Timers::new(services.clock.now()),
            services,
            tasks: HashMap::new(),
            child_workflows: HashMap::new(),
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

    #[cfg(test)]
    pub(crate) fn inbound_channel_ref(
        workflow_id: Option<WorkflowId>,
        name: impl Into<String>,
    ) -> ExternRef {
        HostResource::inbound_channel(workflow_id, name.into()).into_ref()
    }

    #[cfg(test)]
    pub(crate) fn outbound_channel_ref(
        workflow_id: Option<WorkflowId>,
        name: impl Into<String>,
    ) -> ExternRef {
        HostResource::outbound_channel(workflow_id, name.into()).into_ref()
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
        if let HostResource::InboundChannel(channel_ref) = dropped {
            let wakers = ctx.data_mut().handle_inbound_channel_closure(channel_ref);
            let exports = ctx.data().exports();
            for waker in wakers {
                let result = exports.drop_waker(ctx.as_context_mut(), waker);
                let result = crate::log_result!(
                    result,
                    "Dropped waker {} for inbound channel {:?} closed by the workflow",
                    waker,
                    channel_ref
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
