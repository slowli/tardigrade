//! Workflow state.

use serde::{Deserialize, Serialize};
use tracing_tunnel::TracingEvent;

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
    helpers::{WakerOrTask, Wakers},
    persistence::PersistError,
};
pub use self::{
    channel::{Channels, ReceiverActions, ReceiverState, SenderActions, SenderState},
    helpers::WorkflowPoll,
    spawn::{ChildActions, ChildWorkflow},
    task::{TaskActions, TaskState},
    time::{TimerActions, TimerState},
};

use self::{
    channel::ChannelStates, helpers::CurrentExecution, spawn::ChildWorkflowState, task::TaskQueue,
    time::Timers,
};
use crate::{
    manager::Services,
    receipt::{PanicInfo, WakeUpCause},
    workflow::ChannelIds,
};
use tardigrade::{
    handle::HandlePathBuf, interface::Interface, task::ErrorLocation, TaskId, WorkflowId,
};

/// Kinds of errors reported by workflows.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum ReportedErrorKind {
    /// Panic (non-recoverable error).
    Panic,
    /// Structured task error.
    TaskError,
}

/// `Workflow` state that can be persisted between workflow invocations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistedWorkflowData {
    pub timers: Timers,
    pub tasks: HashMap<TaskId, TaskState>,
    child_workflows: HashMap<WorkflowId, ChildWorkflowState>,
    channels: ChannelStates,
    pub waker_queue: Vec<Wakers>,
}

/// Data associated with a workflow instance.
#[derive(Debug)]
pub struct WorkflowData {
    /// Persisted workflow data.
    pub(crate) persisted: PersistedWorkflowData,
    /// Services available to the workflow.
    services: Option<Services>,
    /// Data related to the currently executing WASM call.
    current_execution: Option<CurrentExecution>,
    /// Tasks that should be polled after `current_task`.
    task_queue: TaskQueue,
    /// Wakeup cause set when waking up tasks.
    current_wakeup_cause: Option<WakeUpCause>,
}

impl WorkflowData {
    pub(crate) fn new(interface: &Interface, channel_ids: ChannelIds, services: Services) -> Self {
        debug_assert_eq!(
            interface
                .handles()
                .map(|(path, _)| path)
                .collect::<HashSet<_>>(),
            channel_ids
                .keys()
                .map(HandlePathBuf::as_ref)
                .collect::<HashSet<_>>()
        );

        Self {
            persisted: PersistedWorkflowData::new(interface, channel_ids, services.clock.now()),
            services: Some(services),
            current_execution: None,
            task_queue: TaskQueue::default(),
            current_wakeup_cause: None,
        }
    }

    pub(crate) fn services(&self) -> &Services {
        self.services
            .as_ref()
            .expect("services accessed after taken out")
    }

    fn services_mut(&mut self) -> &mut Services {
        self.services
            .as_mut()
            .expect("services accessed after taken out")
    }

    pub(crate) fn set_services(&mut self, services: Services) {
        debug_assert!(self.services.is_none(), "services set repeatedly");
        self.services = Some(services);
    }

    pub(crate) fn take_services(&mut self) -> Services {
        self.services.take().expect("services already taken out")
    }

    /// Reports an error or panic that has occurred in the workflow.
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn report_error_or_panic(
        &mut self,
        error_kind: ReportedErrorKind,
        message: Option<String>,
        filename: Option<String>,
        line: u32,
        column: u32,
    ) {
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
                self.current_execution().push_task_error(info);
            }
            ReportedErrorKind::Panic => {
                self.current_execution().set_panic(info);
            }
        }
    }

    /// Reports a tracing event that has occurred in the workflow.
    pub fn send_trace(&mut self, trace: TracingEvent) {
        tracing::trace!(?trace, "received client trace");

        if let Some(tracing) = self.services_mut().tracer.as_mut() {
            if let Err(err) = tracing.try_receive(trace) {
                tracing::warn!(%err, "received bogus tracing event");
            }
        }
    }
}
