//! gRPC service wrapper for the Tardigrade runtime powered by [`tonic`].
//!
//! The wrapper can be used as a building block for gRPC server in the cases when [the CLI app]
//! is not good enough (e.g., if custom authentication / authorization logic is required).
//!
//! The generated Protobuf types are not public. This is intentional; the API contract
//! for the gRPC services is not stable yet.
//!
//! [`tonic`]: https://docs.rs/tonic/
//! [the CLI app]: https://crates.io/crates/tardigrade-cli
//!
//! # Crate features
//!
//! At least one of `server` and `client` features should be enabled, since the crate
//! is useless otherwise.
//!
//! ## `server`
//!
//! *(On by default)*
//!
//! Exposes server functionality for Tardigrade [`Runtime`] and [`Storage`] abstractions.
//!
//! ## `client`
//!
//! *(Off by default)*
//!
//! Exposes client functionality to connect to the runtime.
//!
//! [`Runtime`]: tardigrade_rt::runtime::Runtime
//! [`Storage`]: tardigrade_rt::storage::Storage
//!
//! # Examples
//!
//! ```
//! use tokio::task;
//! use tonic::transport::Server;
//! # use std::{net::SocketAddr, sync::Arc};
//!
//! use tardigrade_rt::{
//!     engine::Wasmtime, runtime::Runtime, TokioScheduler,
//!     storage::{LocalStorage, Streaming},
//! };
//! use tardigrade_grpc::*;
//!
//! # async fn test_wrapper() -> anyhow::Result<()> {
//! // Build a workflow runtime
//! let storage = Arc::new(LocalStorage::default());
//! let (storage, routing_task) = Streaming::new(storage);
//! task::spawn(routing_task);
//! let runtime = Runtime::builder(Wasmtime::default(), storage)
//!     .with_clock(TokioScheduler)
//!     .build();
//!
//! // Create services based on the runtime
//! let service = RuntimeWrapper::new(runtime);
//! let channels_service = ChannelsServiceServer::new(service.storage_wrapper());
//! let runtime_service = RuntimeServiceServer::new(service);
//!
//! // Create gRPC server with the services
//! let addr: SocketAddr = "[::]:9000".parse()?;
//! Server::builder()
//!     .add_service(runtime_service)
//!     .add_service(channels_service)
//!     // Add other services and/or configure the server...
//!     .serve(addr)
//!     .await?;
//! # Ok(())
//! # }
//! ```

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

#[cfg(not(any(feature = "server", feature = "client")))]
compile_error!(
    "At least one of `server` and `client` features should be enabled; \
     the crate is useless otherwise"
);

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "server")]
mod service;

#[allow(clippy::pedantic)] // too may lints triggered
mod proto {
    tonic::include_proto!("tardigrade.v0");
}

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub use crate::client::Client;
#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
pub use crate::{
    proto::{
        channels_service_server::ChannelsServiceServer,
        runtime_service_server::RuntimeServiceServer, test_service_server::TestServiceServer,
    },
    service::{RuntimeWrapper, StorageWrapper, WithClockType},
};

/// Serialized file descriptor set for messages and services declared in this crate.
#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
pub const SERVICE_DESCRIPTOR: &[u8] = tonic::include_file_descriptor_set!("tardigrade_descriptor");
