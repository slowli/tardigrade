//! Tardigrade runtime library.
//!
//! The runtime provides a [`WorkflowManager`] in which workflows defined in a WASM module
//! can be executed and [persisted](PersistedWorkflow) / restored. Interaction with a workflow
//! (e.g., submitting messages to inbound channels or taking messages from outbound channels)
//! can be performed using low-level, synchronous [`WorkflowHandle`], or by driving the manager
//! with the [`Driver`].
//!
//! # Instantiating workflows
//!
//! 1. [`WorkflowEngine`] encapsulates the [`wasmtime`] engine to validate, compile and run
//!   WASM modules. It should be instantiated once at the beginning of the program lifecycle.
//! 2. [`WorkflowModule`] represents a deployment artifact represented by a WASM module
//!   with a particular interface (e.g., a custom section declaring one or more workflow
//!   interfaces). It can be instantiated from the module binary using a `WorkflowEngine`.
//! 3. [`WorkflowSpawner`] allows to spawn workflows using a particular workflow definition
//!   from the module. A spawner can be obtained from [`WorkflowModule`].
//! 4. [`WorkflowManager`] contains workflow instances together with communication channels.
//!
//! [`wasmtime`]: https://docs.rs/wasmtime/latest/wasmtime/
//! [`WorkflowManager`]: crate::manager::WorkflowManager
//! [`WorkflowHandle`]: crate::manager::WorkflowHandle
//! [`Driver`]: crate::manager::driver::Driver
//!
//! # Crate features
//!
//! ## `async-io`
//!
//! *(Off by default)*
//!
//! Implements [`Schedule`] trait, necessary to instantiate async workflow handles,
//! using the [`async-io`] crate.
//!
//! [`Schedule`]: crate::Schedule
//! [`async-io`]: https://docs.rs/async-io/
//!
//! # Examples
//!
//! ## Instantiating workflow
//!
//! ```
//! use tardigrade_rt::{
//!     manager::{WorkflowHandle, WorkflowManager},
//!     storage::LocalStorage,
//!     WorkflowEngine, WorkflowModule,
//! };
//! use tardigrade::spawn::ManageWorkflowsExt;
//!
//! # async fn test_wrapper() -> anyhow::Result<()> {
//! let module_bytes: Vec<u8> = // e.g., take from a file
//! #   vec![];
//! let engine = WorkflowEngine::default();
//! let module = WorkflowModule::new(&engine, &module_bytes)?;
//! // It is possible to inspect module definitions:
//! for (workflow_name, interface) in module.interfaces() {
//!     println!("{}: {:?}", workflow_name, interface);
//! }
//!
//! // Let's instantiate a manager and add the module to it.
//! let storage = LocalStorage::default();
//! let mut manager = WorkflowManager::builder(storage)
//!     .build()
//!     .await?;
//! manager.insert_module("test", module).await;
//!
//! // Workflows are created within a manager that is responsible
//! // for their persistence and managing channels, time, and child workflows.
//! let new_workflow = manager
//!     .new_workflow::<()>("test::Workflow", b"data".to_vec())?
//!     .build()
//!     .await?;
//!
//! // Let's initialize the workflow.
//! let receipt = manager.tick().await?.into_inner()?;
//! // `receipt` contains information about WASM code execution. E.g.,
//! // this will print the executed functions and a list of important
//! // events for each of executions:
//! println!("{:?}", receipt.executions());
//! # Ok(())
//! # }
//! ```
//!
//! See [`WorkflowManager`] and [`Driver`] docs for examples of what to do with workflows
//! after instantiation.
//!
//! ## Persisting and restoring workflow
//!
//! ```
//! # use tardigrade::WorkflowId;
//! # use tardigrade_rt::{manager::WorkflowManager, storage::LocalStorage, PersistedWorkflow};
//! # async fn test_wrapper(
//! #     manager: WorkflowManager<(), LocalStorage>,
//! #     workflow_id: WorkflowId,
//! # ) -> anyhow::Result<()> {
//! let manager: WorkflowManager<(), LocalStorage> = // ...
//! #   manager;
//! let workflow = manager.workflow(workflow_id).await.unwrap();
//! let persisted: &PersistedWorkflow = workflow.persisted();
//! // The persisted workflow can be serialized:
//! let json = serde_json::to_string(persisted)?;
//!
//! // In case of using `LocalStorage`, the entire state can be serialized
//! // as well:
//! let mut storage = manager.into_storage();
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
#![allow(clippy::must_use_candidate, clippy::module_name_repetitions)]

mod backends;
mod data;
pub mod manager;
mod module;
pub mod receipt;
pub mod storage;
pub mod test;
mod utils;
mod workflow;

#[cfg(feature = "async-io")]
pub use crate::backends::AsyncIoScheduler;
pub use crate::{
    data::{ChildWorkflowState, InboundChannelState, OutboundChannelState, TaskState, TimerState},
    module::{
        Clock, ExtendLinker, Schedule, TimerFuture, WorkflowEngine, WorkflowModule, WorkflowSpawner,
    },
    workflow::PersistedWorkflow,
};
