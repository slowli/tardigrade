//! Tradigrade runtime library.

// FIXME: `WakerId` is non-unique! (`TaskId` may be as well.)

mod abi;
mod module;
mod state;
mod time;
mod workflow;

/// Pointer to a task.
pub type TaskId = i64;

pub use crate::{
    module::WorkflowModule,
    workflow::{
        DataPeeker, Execution, MessageReceiver, MessageSender, Receipt, TimerHandle, Workflow,
        WorkflowHandle,
    },
};
