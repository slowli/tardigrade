//! FIXME

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
    pub use tardigrade_derive::GetInterface;
}

#[cfg(feature = "derive")]
pub use tardigrade_derive::{handle, init};

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
