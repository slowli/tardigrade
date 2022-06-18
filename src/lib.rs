//! FIXME

pub mod channel;
mod codec;
mod context;
mod data;
mod ext;
#[cfg(not(target_arch = "wasm32"))]
mod mock;
mod task;
mod time;
pub mod trace;

#[cfg(feature = "serde_json")]
pub use crate::codec::Json;
#[cfg(not(target_arch = "wasm32"))]
pub use crate::mock::{TestHandle, TestHost, TestWorkflow, TimersHandle};
pub use crate::{
    codec::{Decoder, Encoder},
    context::{SpawnWorkflow, TaskHandle, Wasm},
    data::Data,
    ext::FutureExt,
    task::{spawn, yield_now, JoinHandle},
    time::sleep,
};

pub mod workflow {
    // Re-export some types from the shared crate for convenience.
    pub use tardigrade_shared::workflow::*;

    #[cfg(feature = "derive")]
    pub use tardigrade_derive::{GetInterface, Initialize, TakeHandle, ValidateInterface};
}

#[macro_export]
macro_rules! workflow_entry {
    ($workflow:ty) => {
        #[cfg(target_arch = "wasm32")]
        #[no_mangle]
        #[export_name = "tardigrade_rt::main"]
        #[doc(hidden)]
        pub extern "C" fn __tardigrade_rt__main() -> $crate::TaskHandle {
            $crate::TaskHandle::from_workflow::<$workflow>()
        }
    };
}
