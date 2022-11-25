//! Workflow state.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tracing::field;
use tracing_tunnel::TracingEvent;
use wasmtime::{AsContextMut, ExternRef, StoreContextMut};

use std::collections::{HashMap, HashSet};

mod channel;
mod helpers;
mod persistence;
mod spawn;
mod task;
mod time;

#[cfg(test)]
pub(crate) mod tests;

pub(crate) use self::{
    channel::ConsumeError,
    helpers::{Wakers, WasmContextPtr},
    persistence::{PersistError, Refs},
    spawn::SpawnFunctions,
};
pub use self::{
    channel::{Channels, ReceiverState, SenderState},
    spawn::ChildWorkflow,
    task::TaskState,
    time::TimerState,
};

use self::{
    channel::ChannelStates,
    helpers::{CurrentExecution, HostResource},
    spawn::{ChildWorkflowState, ChildWorkflowStubs},
    task::TaskQueue,
    time::Timers,
};
use crate::{
    module::{ModuleExports, Services},
    receipt::{PanicInfo, WakeUpCause},
    utils::copy_string_from_wasm,
    workflow::ChannelIds,
};
use tardigrade::{interface::Interface, task::ErrorLocation, TaskId, WorkflowId};

/// Kinds of errors reported by workflows.
#[derive(Debug, Clone, Copy)]
enum ReportedErrorKind {
    Panic,
    TaskError,
}

/// `Workflow` state that can be persisted between workflow invocations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistedWorkflowData {
    pub timers: Timers,
    pub tasks: HashMap<TaskId, TaskState>,
    child_workflow_stubs: ChildWorkflowStubs,
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
    /// Persisted workflow data.
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
        interface: &Interface,
        channel_ids: ChannelIds,
        services: Services<'a>,
    ) -> Self {
        debug_assert_eq!(
            interface
                .receivers()
                .map(|(name, _)| name)
                .collect::<HashSet<_>>(),
            channel_ids
                .receivers
                .keys()
                .map(String::as_str)
                .collect::<HashSet<_>>()
        );
        debug_assert_eq!(
            interface
                .senders()
                .map(|(name, _)| name)
                .collect::<HashSet<_>>(),
            channel_ids
                .senders
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
    pub(crate) fn receiver_ref(&self, child_id: Option<WorkflowId>, name: &str) -> ExternRef {
        let channels = if let Some(child_id) = child_id {
            self.persisted.child_workflow(child_id).unwrap().channels()
        } else {
            self.persisted.channels()
        };
        let channel_id = channels.receiver_id(name).unwrap();
        HostResource::Receiver(channel_id).into_ref()
    }

    #[cfg(test)]
    pub(crate) fn sender_ref(&self, child_id: Option<WorkflowId>, name: &str) -> ExternRef {
        let channels = if let Some(child_id) = child_id {
            self.persisted.child_workflow(child_id).unwrap().channels()
        } else {
            self.persisted.channels()
        };
        let channel_id = channels.sender_id(name).unwrap();
        HostResource::Sender(channel_id).into_ref()
    }

    #[cfg(test)]
    pub(crate) fn child_ref(workflow_id: WorkflowId) -> ExternRef {
        HostResource::Workflow(workflow_id).into_ref()
    }
}

/// Functions operating on `WorkflowData` exported to WASM.
pub(crate) struct WorkflowFunctions;

impl WorkflowFunctions {
    #[tracing::instrument(level = "debug", skip_all, err, fields(resource))]
    #[allow(clippy::needless_pass_by_value)] // required by wasmtime
    pub fn drop_ref(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        dropped: Option<ExternRef>,
    ) -> anyhow::Result<()> {
        let dropped = HostResource::from_ref(dropped.as_ref())?;
        tracing::Span::current().record("resource", field::debug(dropped));

        let wakers = match dropped {
            HostResource::Receiver(channel_id) => ctx.data_mut().handle_receiver_drop(*channel_id),
            HostResource::Sender(channel_id) => ctx.data_mut().handle_sender_drop(*channel_id),
            HostResource::WorkflowStub(stub_id) => ctx.data_mut().handle_child_stub_drop(*stub_id),
            HostResource::Workflow(workflow_id) => {
                ctx.data_mut().handle_child_handle_drop(*workflow_id)
            }

            HostResource::ChannelHandles(_) => return Ok(()),
            // ^ no associated wakers, thus no cleanup necessary
        };

        let exports = ctx.data().exports();
        for waker in wakers {
            let result = exports.drop_waker(ctx.as_context_mut(), waker);
            result.ok(); // We assume traps during dropping wakers is not significant
        }
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip(ctx, message_ptr, message_len, filename_ptr, filename_len),
        err,
        fields(message, filename)
    )]
    pub fn report_panic(
        ctx: StoreContextMut<'_, WorkflowData>,
        message_ptr: u32,
        message_len: u32,
        filename_ptr: u32,
        filename_len: u32,
        line: u32,
        column: u32,
    ) -> anyhow::Result<()> {
        Self::report_error_or_panic(
            ctx,
            ReportedErrorKind::Panic,
            message_ptr,
            message_len,
            filename_ptr,
            filename_len,
            line,
            column,
        )
    }

    #[allow(clippy::too_many_arguments)] // acceptable for internal fn
    fn report_error_or_panic(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        error_kind: ReportedErrorKind,
        message_ptr: u32,
        message_len: u32,
        filename_ptr: u32,
        filename_len: u32,
        line: u32,
        column: u32,
    ) -> anyhow::Result<()> {
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
        tracing::Span::current()
            .record("message", &message)
            .record("filename", &filename);

        let info = PanicInfo {
            message,
            location: filename.map(|filename| ErrorLocation {
                filename: filename.into(),
                line,
                column,
            }),
        };
        match error_kind {
            ReportedErrorKind::TaskError => {
                ctx.data_mut().current_execution().push_task_error(info);
            }
            ReportedErrorKind::Panic => {
                ctx.data_mut().current_execution().set_panic(info);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct TracingFunctions;

impl TracingFunctions {
    #[allow(clippy::needless_pass_by_value)] // required by wasmtime
    pub fn send_trace(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        trace_ptr: u32,
        trace_len: u32,
    ) -> anyhow::Result<()> {
        let memory = ctx.data().exports().memory;
        let trace = copy_string_from_wasm(&ctx, &memory, trace_ptr, trace_len)?;
        let trace: TracingEvent =
            serde_json::from_str(&trace).context("`TracingEvent` deserialization failed")?;
        tracing::trace!(?trace, "received client trace");

        if let Some(tracing) = ctx.data_mut().services.tracer.as_mut() {
            if let Err(err) = tracing.try_receive(trace) {
                tracing::warn!(%err, "received bogus tracing event");
            }
        }
        Ok(())
    }
}
