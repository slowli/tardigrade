//! Client bindings for Tardigrade workflows. This crate should be used by WASM modules
//! defining a workflow.
//!
//! # What's a workflow, anyway?
//!
//! A workflow is a essentially a [`Future`] with `()` output that interacts with the external
//! world via well-defined interfaces:
//!
//! - Arguments provided to the workflow on creation
//! - Message [channels](channel) similar [to ones from the `future` crate][future-chan]
//! - Timers
//! - Tasks
//!
//! Timers [can be created dynamically](sleep) during workflow operation; likewise, tasks
//! can be [`spawn()`](task::spawn())ed to achieve concurrency (but not parallelism!).
//! In contrast, the set of channel halves and their direction (receiver or sender)
//! are static / predefined in the workflow [`Interface`].
//!
//! A workflow is sandboxed by the virtue of being implemented as a WASM module.
//! This brings multiple benefits:
//!
//! - A workflow can be suspended, its state persisted and then restored completely transparently
//!   for workflow logic. Further, this approach does not restrict workflow functionality;
//!   it can do essentially anything `Future`s can do: fork / joins, fork / selects, retries etc.
//! - The runtime can (theoretically) customize aspects of workflow execution, e.g., priorities
//!   of different tasks. It can create and escalate incidents on task failure (perhaps, continuing
//!   executing other tasks if possible), control rollback of the workflow state etc.
//! - A workflow could (theoretically) implemented in any WASM-targeting programming language
//!   and can run / be debugged in any WASM-supporting env (e.g., in a browser).
//! - Sandboxing means that interfaces for interacting with the external world are well-defined
//!   (e.g., in terms of security) and can be emulated (e.g., for testing) with reasonable effort.
//!
//! [`Future`]: std::future::Future
//! [future-chan]: https://docs.rs/futures/latest/futures/channel/index.html
//! [`Interface`]: interface::Interface
//!
//! # Crate features
//!
//! ## `serde_json`
//!
//! *(On by default)*
//!
//! Exposes [`Json`] [codec](Codec) for messages received by a workflow.
//!
//! ## `tracing`
//!
//! *(On by default)*
//!
//! Enables [tracing] for the library glue code and the workflows that are defined
//! using the library.
//!
//! [tracing]: https://docs.rs/tracing/

// Documentation settings.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(html_root_url = "https://docs.rs/tardigrade/0.1.0")]
// Linter settings.
#![warn(missing_debug_implementations, missing_docs, bare_trait_objects)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::must_use_candidate,
    clippy::module_name_repetitions,
    clippy::trait_duplication_in_bounds
)]

#[doc(hidden)] // used by runtime; low-level and unstable
pub mod abi;
pub mod channel;
mod codec;
mod error;
pub mod spawn;
pub mod task;
#[cfg(not(target_arch = "wasm32"))]
pub mod test;
mod time;
#[cfg(feature = "tracing")]
mod tracing;
#[cfg(target_arch = "wasm32")]
mod wasm_utils;
pub mod workflow;

#[cfg(feature = "serde_json")]
pub use crate::codec::Json;
pub use crate::{
    codec::{Codec, Raw},
    time::{now, sleep, Timer, TimerDefinition},
};

pub use tardigrade_shared::{handle, interface};

#[doc(hidden)] // used by the derive macros; not public
pub mod _reexports {
    #[cfg(target_arch = "wasm32")] // used by `WorkflowEntry` derive macro
    pub use externref;

    pub use once_cell::sync::Lazy; // used by `GetInterface` derive macro
}

/// ID of a [`Waker`](std::task::Waker) defined by a workflow.
pub type WakerId = u64;
/// ID of a workflow task.
pub type TaskId = u64;
/// ID of a workflow timer.
pub type TimerId = u64;
/// ID of a (traced) future defined by a workflow.
pub type FutureId = u64;
/// ID of a workflow.
pub type WorkflowId = u64;
/// ID of a channel.
pub type ChannelId = u64;
