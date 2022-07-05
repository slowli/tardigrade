//! Shared type definitions and traits for Tardigrade runtime and client bindings.

#![warn(missing_debug_implementations, missing_docs, bare_trait_objects)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::must_use_candidate, clippy::module_name_repetitions)]

pub mod abi;
pub mod trace;
mod types;
pub mod workflow;

pub use crate::types::{
    FutureId, JoinError, PollMessage, PollTask, TaskId, TimerDefinition, TimerId, TimerKind,
    WakerId,
};
