//! Client bindings for Tardigrade workflows. This crate should be used by WASM modules
//! defining a workflow.
//!
//! # What's a workflow, anyway?
//!
//! A workflow is a essentially a [`Future`] with `()` output that interacts with the external
//! world via well-defined interfaces:
//!
//! - [Data inputs](Data) provided to the workflow on creation
//! - Message [channels](crate::channel) similar [to ones from the `future` crate][future-chan]
//! - Timers
//! - Tasks
//!
//! Timers [can be created dynamically](sleep) during workflow operation; likewise, tasks
//! can be [`spawn`]ed to achieve concurrency (but not parallelism!). In contrast, the
//! set of channels and their direction (inbound or outbound), and the set of data inputs
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
//! [`Interface`]: crate::interface::Interface

#![warn(missing_debug_implementations, missing_docs, bare_trait_objects)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::must_use_candidate, clippy::module_name_repetitions)]

pub mod channel;
mod codec;
mod data;
mod ext;
mod task;
#[cfg(not(target_arch = "wasm32"))]
pub mod test;
mod time;
pub mod trace;
pub mod workflow;

#[cfg(feature = "serde_json")]
pub use crate::codec::Json;
pub use crate::{
    codec::{Decoder, Encoder, Raw},
    data::{Data, RawData},
    ext::FutureExt,
    task::{spawn, yield_now, JoinHandle},
    time::sleep,
};

#[cfg(feature = "derive")]
pub use tardigrade_derive::{handle, init};
pub use tardigrade_shared::interface;

/// Creates an entry point for the specified workflow type.
///
/// The specified type must implement [`SpawnWorkflow`](crate::workflow::SpawnWorkflow).
#[macro_export]
macro_rules! workflow_entry {
    ($workflow:ty) => {
        #[cfg(target_arch = "wasm32")]
        #[no_mangle]
        #[export_name = "tardigrade_rt::main"]
        #[doc(hidden)]
        pub extern "C" fn __tardigrade_rt__main() -> $crate::TaskHandle {
            $crate::TaskHandle::from_workflow::<$workflow>().unwrap()
        }
    };
}
