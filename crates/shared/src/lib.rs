//! Shared type definitions and traits for Tardigrade runtime and client bindings.

// Documentation settings.
#![doc(html_root_url = "https://docs.rs/tardigrade-shared/0.1.0")]
// Linter settings.
#![warn(missing_debug_implementations, missing_docs, bare_trait_objects)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::must_use_candidate, clippy::module_name_repetitions)]

pub mod abi;
pub mod interface;
pub mod trace;
mod types;

pub use crate::types::{
    FutureId, JoinError, PollMessage, PollTask, TaskId, TimerDefinition, TimerId, WakerId,
};
