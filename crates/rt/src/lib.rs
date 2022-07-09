//! Tardigrade runtime library.
//!
//! The runtime provides host environment in which [`Workflow`]s defined in a WASM module
//! can be executed and [persisted](PersistedWorkflow) / restored. Interaction with a workflow
//! (e.g., submitting messages to inbound channels or taking messages from outbound channels)
//! can be performed using [`WorkflowHandle`].
//!
//! # Instantiating workflows
//!
//! 1. [`WorkflowEngine`] encapsulates the [`wasmtime`] engine to validate, compile and run
//!   WASM modules. It should be instantiated once at the beginning of the program lifecycle.
//! 2. [`WorkflowModule`] represents a single workflow definition. It can be instantiated
//!   from the module binary using a `WorkflowEngine`.
//! 3. [`Workflow`] is an instance of a workflow. It can be created from a [`WorkflowModule`].
//!
//! [`wasmtime`]: https://docs.rs/wasmtime/latest/wasmtime/

#![warn(missing_debug_implementations, missing_docs, bare_trait_objects)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::must_use_candidate, clippy::module_name_repetitions)]

mod data;
pub mod handle;
mod module;
pub mod receipt;
pub mod test;
mod utils;
mod workflow;

pub use crate::{
    data::{ConsumeError, ConsumeErrorKind, PersistError, TaskState, TimerState},
    module::{ExtendLinker, WorkflowEngine, WorkflowModule},
    workflow::{PersistedWorkflow, Workflow},
};
pub use tardigrade_shared::{FutureId, TaskId, TimerId, WakerId};
