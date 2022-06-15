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

#[cfg(feature = "serde_json")]
pub use crate::codec::Json;
#[cfg(not(target_arch = "wasm32"))]
pub use crate::mock::{TestHandle, TestHost, TestWorkflow, TimersHandle};
pub use crate::{
    codec::{Decoder, Encoder},
    context::{SpawnWorkflow, TaskHandle, Wasm},
    data::Data,
    ext::{FutureExt, Traced, Tracer},
    task::{spawn, yield_now, JoinHandle},
    time::sleep,
};

// Re-export some types from the shared crate for convenience.
pub use tardigrade_shared::workflow;
