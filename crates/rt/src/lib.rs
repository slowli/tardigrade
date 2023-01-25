//! Tardigrade runtime library.
//!
//! The runtime provides a [`Runtime`] in which workflows defined in a WASM module
//! can be executed and [persisted](PersistedWorkflow) / restored. Interaction with a workflow
//! (e.g., submitting messages to channel receivers or taking messages from senders)
//! can be performed using [`WorkflowHandle`]s.
//!
//! # Components
//!
//! The crate defines two major abstractions necessary to run workflows:
//!
//! - [**Engine**](engine) that provides a way to define, instantiate, run and snapshot workflows
//! - [**Storage**](storage) that provides a mechanism to durably persist the state of
//!   workflows and channels connecting them and the external world
//!
//! For each of abstractions, there is a default implementation available: the [`wasmtime`]-powered
//! [engine](engine::Wasmtime) and the in-memory, in-process [storage](storage::LocalStorage)
//! (it still provides a way to [(de)serialize] a storage snapshot, so it's not *completely* useless).
//!
//! See the linked module docs for more details on abstractions.
//!
//! [`Runtime`]: runtime::Runtime
//! [`WorkflowHandle`]: handle::WorkflowHandle
//! [`wasmtime`]: https://docs.rs/wasmtime/
//! [(de)serialize]: https://docs.rs/serde/
//!
//! # Crate features
//!
//! ## `test`
//!
//! *(Off by default)*
//!
//! Provides the [`test`](crate::test) module with helpers for integration testing of workflows.
//!
//! ## `async-io`
//!
//! *(Off by default)*
//!
//! Implements [`Schedule`] trait, necessary to instantiate async workflow handles,
//! using the [`async-io`] crate.
//!
//! [`async-io`]: https://docs.rs/async-io/
//!
//! # Examples
//!
//! ## Instantiating workflow
//!
//! ```
//! use tardigrade_rt::{
//!     engine::{Wasmtime, WorkflowEngine}, runtime::Runtime, storage::LocalStorage,
//! };
//! use tardigrade::spawn::CreateWorkflow;
//!
//! # async fn test_wrapper() -> anyhow::Result<()> {
//! let module_bytes: Vec<u8> = // e.g., take from a file
//! #   vec![];
//! let engine = Wasmtime::default();
//! let module = engine.create_module(module_bytes.into()).await?;
//! // It is possible to inspect module definitions:
//! for (workflow_name, interface) in module.interfaces() {
//!     println!("{workflow_name}: {interface:?}");
//! }
//!
//! // Let's instantiate a runtime and add the module to it.
//! let storage = LocalStorage::default();
//! let runtime = Runtime::builder(engine, storage).build();
//! runtime.insert_module("test", module).await;
//!
//! // Workflows are created within a runtime that is responsible
//! // for their persistence and managing channels, time, and child workflows.
//! let spawner = runtime.spawner();
//! let builder = spawner.new_workflow::<()>("test::Workflow").await?;
//! let (handles, self_handles) = builder.handles(|_| {}).await;
//! let new_workflow =
//!     builder.build(b"data".to_vec(), handles).await?;
//!
//! // Let's initialize the workflow.
//! let receipt = runtime.tick().await?.into_inner()?;
//! // `receipt` contains information about WASM code execution. E.g.,
//! // this will print the executed functions and a list of important
//! // events for each of executions:
//! println!("{:?}", receipt.executions());
//! // It's possible to communicate with the workflow using `self_handles`
//! // and/or `new_workflow`.
//! # Ok(())
//! # }
//! ```
//!
//! See [`Runtime`] docs for examples of what to do with workflows after instantiation.
//!
//! ## Persisting and restoring workflow
//!
//! ```
//! # use tardigrade::WorkflowId;
//! # use tardigrade_rt::{
//! #     engine::Wasmtime, runtime::Runtime, storage::LocalStorage, PersistedWorkflow,
//! # };
//! #
//! # async fn test_wrapper(
//! #     runtime: Runtime<Wasmtime, (), LocalStorage>,
//! #     workflow_id: WorkflowId,
//! # ) -> anyhow::Result<()> {
//! let runtime: Runtime<Wasmtime, (), LocalStorage> = // ...
//! #   runtime;
//! let workflow = runtime.storage().workflow(workflow_id).await.unwrap();
//! let persisted: &PersistedWorkflow = workflow.persisted();
//! // The persisted workflow can be serialized:
//! let json = serde_json::to_string(persisted)?;
//!
//! // In case of using `LocalStorage`, the entire state can be serialized
//! // as well:
//! let mut storage = runtime.into_storage();
//! let json = serde_json::to_string(&storage.snapshot())?;
//! # Ok(())
//! # }
//! ```

// Documentation settings.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(html_root_url = "https://docs.rs/tardigrade-rt/0.1.0")]
// Linter settings.
#![warn(missing_debug_implementations, missing_docs, bare_trait_objects)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::must_use_candidate,
    clippy::module_name_repetitions,
    clippy::similar_names
)]

mod backends;
mod data;
pub mod engine;
pub mod handle;
pub mod receipt;
pub mod runtime;
pub mod storage;
#[cfg(any(test, feature = "test"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test")))]
pub mod test;
mod utils;
mod workflow;

#[cfg(feature = "async-io")]
pub use crate::backends::AsyncIoScheduler;
#[cfg(feature = "tokio")]
pub use crate::backends::TokioScheduler;
pub use crate::{
    data::{Channels, ChildWorkflow, ReceiverState, SenderState, TaskState, TimerState},
    runtime::{Clock, Schedule, TimerFuture},
    workflow::PersistedWorkflow,
};
