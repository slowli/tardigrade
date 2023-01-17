//! gRPC service wrapper for the Tardigrade runtime.
//!
//! The wrapper can be used as a building block for gRPC server in the cases when
//!
//! Most of generated Protobuf types are not public. This is intentional;
//! the API contract for the gRPC services is not stable yet.

// Documentation settings.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(html_root_url = "https://docs.rs/tardigrade-grpc/0.1.0")]
// Linter settings.
#![warn(missing_debug_implementations, missing_docs, bare_trait_objects)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::must_use_candidate,
    clippy::module_name_repetitions,
    clippy::trait_duplication_in_bounds,
    clippy::doc_markdown // false positive on "gRPC"
)]

mod mapping;
mod service;

#[cfg(test)]
mod tests;

#[allow(clippy::pedantic)] // too may lints triggered
mod proto {
    tonic::include_proto!("tardigrade.v0");
}

pub use crate::{
    proto::{
        channels_service_server::ChannelsServiceServer,
        runtime_service_server::RuntimeServiceServer, test_service_server::TestServiceServer,
    },
    service::{ManagerService, WithClockType},
};

/// Serialized file descriptor set for messages and services declared in this crate.
pub const SERVICE_DESCRIPTOR: &[u8] = tonic::include_file_descriptor_set!("tardigrade_descriptor");
