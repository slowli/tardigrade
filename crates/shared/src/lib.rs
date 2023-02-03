//! Shared type definitions and traits for Tardigrade runtime and client bindings.

// Documentation settings.
#![doc(html_root_url = "https://docs.rs/tardigrade-shared/0.1.0")]
// Linter settings.
#![warn(missing_debug_implementations, missing_docs, bare_trait_objects)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::must_use_candidate, clippy::module_name_repetitions)]

mod codec;
pub mod handle;
mod helpers;
pub mod interface;
mod path;
mod types;

pub use self::{
    codec::{Codec, Json, Raw},
    types::{ChannelId, Request, Response, TaskId, TimerId, WakerId, WorkflowId},
};
