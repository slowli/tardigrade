//! Tradigrade runtime library.

// FIXME: `WakerId` is non-unique! (`TaskId` may be as well.)

mod module;
mod receipt;
mod state;
mod utils;
mod workflow;

pub use crate::{
    module::WorkflowModule,
    receipt::{
        ExecutedFunction, Execution, ExecutionError, Receipt, ResourceEvent, ResourceEventKind,
        ResourceId, WakeUpCause,
    },
    state::{ConsumeError, ConsumeErrorKind},
    workflow::{DataPeeker, MessageReceiver, MessageSender, Workflow, WorkflowHandle},
};

/// ID of a task.
pub type TaskId = i64;
/// ID of a timer.
pub type TimerId = u64;
/// WASM waker ID. Equal to a pointer to the waker instance.
pub type WakerId = u32;
