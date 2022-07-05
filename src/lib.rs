//! Client bindings for Tardigrade workflows.
//!
//! This crate should be used by WASM modules defining a workflow.

#![warn(missing_debug_implementations, missing_docs, bare_trait_objects)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::must_use_candidate, clippy::module_name_repetitions)]

pub mod channel;
mod codec;
mod context;
mod data;
mod ext;
mod task;
#[cfg(not(target_arch = "wasm32"))]
pub mod test;
mod time;
pub mod trace;

#[cfg(feature = "serde_json")]
pub use crate::codec::Json;
pub use crate::{
    codec::{Decoder, Encoder},
    context::{SpawnWorkflow, TaskHandle, UntypedHandle, Wasm},
    data::{Data, RawData},
    ext::FutureExt,
    task::{spawn, yield_now, JoinHandle},
    time::sleep,
};

pub mod workflow {
    //! Workflow-related types.

    // Re-export some types from the shared crate for convenience.
    pub use tardigrade_shared::workflow::*;

    #[cfg(feature = "derive")]
    pub use tardigrade_derive::GetInterface;
}

#[cfg(feature = "derive")]
pub use tardigrade_derive::{handle, init};

/// Creates an entry point for the specified workflow type.
///
/// The specified type must implement [`SpawnWorkflow`].
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
