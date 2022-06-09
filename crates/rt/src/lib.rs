//! Tradigrade runtime library.

// FIXME: `WakerId` is non-unique! (`TaskId` may be as well.)

mod module;
mod state;
mod time;
mod utils;
mod workflow;

/// Pointer to a task.
pub type TaskId = i64;

pub use crate::{
    module::WorkflowModule,
    workflow::{
        DataPeeker, ExecutedFunction, Execution, ExecutionError, MessageReceiver, MessageSender,
        Receipt, ResourceEvent, ResourceEventKind, ResourceId, Workflow, WorkflowHandle,
    },
};
