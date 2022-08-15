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
//!
//! # Crate features
//!
//! ## `serde_json`
//!
//! *(On by default)*
//!
//! Exposes [`Json`] [codec](crate::Encode) for messages received by a workflow.
//!
//! ## `derive`
//!
//! *(Off by default)*
//!
//! Re-exports procedural macros from the `tardigrade-derive` crate.

// Documentation settings.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(html_root_url = "https://docs.rs/tardigrade/0.1.0")]
// Linter settings.
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
    codec::{Decode, Encode, Raw},
    data::{Data, RawData},
    ext::FutureExt,
    task::{spawn, yield_now, JoinHandle},
    time::{now, sleep, Timer},
};

/// Proc macro attribute for workflow handles.
///
/// The attribute should be placed on a struct with handles to the workflow interface elements,
/// such as channels and data inputs. These handles must be specified using the [`Handle`] type
/// alias, with the second type arg (the environment) being the only type arg of the struct.
///
/// The attribute will transform the input as follows:
///
/// - Add a `where` clause for the handle struct specifying that field handles exist
///   in an environment
/// - Derive [`TakeHandle`] for the workflow type using the handle struct as a handle
/// - Derive [`ValidateInterface`] for the workflow type
/// - Optionally, derive `Clone` and/or `Debug` for the handle struct if the corresponding derives
///   are requested via `#[derive(..)]`. (Ordinary derivation of these traits is problematic
///   because of the `where` clause on the struct.)
///
/// # Attributes
///
/// Attributes are specified according to standard Rust conventions:
/// `#[tardigrade::handle(attr1 = "value1", ...)]`.
///
/// ## `for`
///
/// Specifies the workflow type that the handle should be attached to.
///
/// # Examples
///
/// ```
/// # use tardigrade::{channel::{Sender, Receiver}, workflow::Handle, Data, Json};
/// /// Workflow type.
/// pub struct MyWorkflow;
///
/// /// Handle for the workflow.
/// #[tardigrade::handle(for = "MyWorkflow")]
/// #[derive(Debug)]
/// pub struct MyHandle<Env> {
///     pub inbound: Handle<Receiver<i64, Json>, Env>,
///     pub outbound: Handle<Sender<i64, Json>, Env>,
/// }
/// ```
///
/// See the [`workflow`](crate::workflow) module docs for an end-to-end example of usage.
///
/// [`Handle`]: crate::workflow::Handle
/// [`TakeHandle`]: crate::workflow::TakeHandle
/// [`ValidateInterface`]: crate::interface::ValidateInterface
#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use tardigrade_derive::handle;

pub use tardigrade_shared::interface;

/// Creates an entry point for the specified workflow type.
///
/// An entry point must be specified for a workflow type in a workflow module in order
/// for the module to properly function (i.e., being able to spawn workflow instances).
/// The specified type must implement [`SpawnWorkflow`](crate::workflow::SpawnWorkflow).
///
/// # Examples
///
/// See the [`workflow`](crate::workflow) module docs for an end-to-end example of usage.
#[macro_export]
macro_rules! workflow_entry {
    ($workflow:ident) => {
        const _: () = {
            #[no_mangle]
            #[export_name = concat!("tardigrade_rt::spawn::", stringify!($workflow))]
            #[doc(hidden)]
            pub unsafe extern "C" fn __tardigrade_rt__main(
                data_ptr: *mut u8,
                data_len: usize,
            ) -> $crate::workflow::TaskHandle {
                $crate::workflow::Wasm::set_panic_hook();
                // ^ Needs to be set at the very start of the workflow
                let data = std::vec::Vec::from_raw_parts(data_ptr, data_len, data_len);
                $crate::workflow::TaskHandle::from_workflow::<$workflow>(data).unwrap()
            }
        };
    };
}
