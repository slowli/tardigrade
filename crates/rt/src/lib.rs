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

pub use tardigrade_shared::{TaskId, TimerId, WakerId};
