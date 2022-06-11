//! FIXME

pub mod channel;
mod codec;
mod context;
mod data;
#[cfg(not(all(target_arch = "wasm32", not(target_os = "emscripten"))))]
mod mock;
mod task;
mod time;

#[cfg(feature = "serde_json")]
pub use crate::codec::Json;
#[cfg(not(all(target_arch = "wasm32", not(target_os = "emscripten"))))]
pub use crate::mock::{TestHandle, TestHost, TestWorkflow, TimersHandle};
pub use crate::{
    codec::{Decoder, Encoder},
    context::{SpawnWorkflow, TaskHandle, Wasm},
    data::Data,
    task::{spawn, yield_now, JoinHandle},
    time::sleep,
};

// Re-export some types from the shared crate for convenience.
pub use tardigrade_shared::workflow;
