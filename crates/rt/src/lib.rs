//! Tardigrade runtime library.
//!
//! The runtime provides host environment in which [`Workflow`]s defined in a WASM module
//! can be executed and [persisted](PersistedWorkflow) / restored. Interaction with a workflow
//! (e.g., submitting messages to inbound channels or taking messages from outbound channels)
//! can be performed using low-level, synchronous [`WorkflowHandle`], or more high-level,
//! future-based [`AsyncEnv`].
//!
//! # Instantiating workflows
//!
//! 1. [`WorkflowEngine`] encapsulates the [`wasmtime`] engine to validate, compile and run
//!   WASM modules. It should be instantiated once at the beginning of the program lifecycle.
//! 2. [`WorkflowModule`] represents a deployment artifact represented by a WASM module
//!   with a particular interface (e.g., a custom section declaring one or more workflow
//!   interfaces). It can be instantiated from the module binary using a `WorkflowEngine`.
//! 3. [`WorkflowSpawner`] allows to spawn workflows using a particular workflow definition
//!   from the module. A spawner can be obtained from [`WorkflowModule`].
//! 4. [`Workflow`] is an instance of a workflow. It can be created from a [`WorkflowSpawner`].
//!
//! [`wasmtime`]: https://docs.rs/wasmtime/latest/wasmtime/
//! [`WorkflowHandle`]: crate::handle::WorkflowHandle
//! [`AsyncEnv`]: crate::handle::future::AsyncEnv

// Documentation settings.
#![doc(html_root_url = "https://docs.rs/tardigrade-rt/0.1.0")]
// Linter settings.
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
    module::{Clock, ExtendLinker, WorkflowEngine, WorkflowModule, WorkflowSpawner},
    workflow::{PersistedWorkflow, Workflow},
};
pub use tardigrade_shared::{FutureId, TaskId, TimerId, WakerId};
