//! Workflow state.

use serde::{Deserialize, Serialize};
use wasmtime::{AsContextMut, ExternRef, StoreContextMut, Trap};

use std::collections::{HashMap, HashSet};

mod channel;
mod helpers;
mod persistence;
mod spawn;
mod task;
#[cfg(test)]
mod tests;
mod time;

pub(crate) use self::{
    channel::ConsumeError,
    helpers::{Wakers, WasmContextPtr},
    persistence::{PersistError, Refs},
    spawn::SpawnFunctions,
};
pub use self::{
    channel::{InboundChannelState, OutboundChannelState},
    spawn::ChildWorkflowState,
    task::TaskState,
    time::TimerState,
};

use self::{channel::ChannelStates, helpers::CurrentExecution, task::TaskQueue, time::Timers};
use crate::{
    data::helpers::HostResource,
    module::{ModuleExports, Services},
    receipt::{PanicInfo, PanicLocation, WakeUpCause},
    utils::copy_string_from_wasm,
    workflow::ChannelIds,
    TaskId, WorkflowId,
};
use tardigrade::interface::Interface;

/// `Workflow` state that can be persisted between workflow invocations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistedWorkflowData {
    pub timers: Timers,
    pub tasks: HashMap<TaskId, TaskState>,
    child_workflows: HashMap<WorkflowId, ChildWorkflowState>,
    channels: ChannelStates,
    pub waker_queue: Vec<Wakers>,
}

#[derive(Debug)]
pub struct WorkflowData<'a> {
    /// Functions exported by the `Instance`. Instantiated immediately after instance.
    exports: Option<ModuleExports>,
    /// Services available to the workflow.
    services: Services<'a>,
    persisted: PersistedWorkflowData,
    /// Data related to the currently executing WASM call.
    current_execution: Option<CurrentExecution>,
    /// Tasks that should be polled after `current_task`.
    task_queue: TaskQueue,
    /// Wakeup cause set when waking up tasks.
    current_wakeup_cause: Option<WakeUpCause>,
}

impl<'a> WorkflowData<'a> {
    pub(crate) fn new(
        interface: &Interface<()>,
        channel_ids: &ChannelIds,
        services: Services<'a>,
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
            persisted: PersistedWorkflowData::new(interface, channel_ids, services.clock.now()),
            exports: None,
            services,
            current_execution: None,
            task_queue: TaskQueue::default(),
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

    #[cfg(test)]
    pub(crate) fn child_ref(workflow_id: WorkflowId) -> ExternRef {
        HostResource::Workflow(workflow_id).into_ref()
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
        trace!("{:?} dropped by workflow code", dropped);

        let wakers = match dropped {
            HostResource::InboundChannel(channel_ref) => {
                ctx.data_mut().handle_inbound_channel_drop(channel_ref)
            }
            HostResource::OutboundChannel(channel_ref) => {
                ctx.data_mut().handle_outbound_channel_drop(channel_ref)
            }
            HostResource::Workflow(workflow_id) => {
                ctx.data_mut().handle_child_handle_drop(*workflow_id)
            }

            HostResource::ChannelHandles(_) => return Ok(()),
            // ^ no associated wakers, thus no cleanup necessary
        };

        let exports = ctx.data().exports();
        for waker in wakers {
            let result = exports.drop_waker(ctx.as_context_mut(), waker);
            let result = log_result!(
                result,
                "Dropped waker {} for {:?} dropped by the workflow",
                waker,
                dropped
            );
            result.ok(); // We assume traps during dropping wakers is not significant
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
