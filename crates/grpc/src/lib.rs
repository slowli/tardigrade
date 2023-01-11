//! gRPC service wrapper for the Tardigrade runtime.

mod mapping;
mod service;

#[cfg(test)]
mod tests;

#[doc(hidden)] // used in integration tests; not public
pub mod proto {
    tonic::include_proto!("tardigrade");
}

pub use crate::{proto::tardigrade_server::TardigradeServer, service::ManagerWrapper};

pub const SERVICE_DESCRIPTOR: &[u8] = tonic::include_file_descriptor_set!("tardigrade_descriptor");
