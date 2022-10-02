//! Tardigrade runtime library.
//!
//! The runtime provides a [`WorkflowManager`] in which workflows defined in a WASM module
//! can be executed and [persisted](PersistedWorkflow) / restored. Interaction with a workflow
//! (e.g., submitting messages to inbound channels or taking messages from outbound channels)
//! can be performed using low-level, synchronous [`WorkflowHandle`], or by driving the manager
//! using future-based [`AsyncEnv`].
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
//! [`WorkflowHandle`]: crate::handle::WorkflowHandle
//! [`AsyncEnv`]: crate::handle::future::AsyncEnv
//!
//! # Crate features
//!
//! ## `async`
//!
//! *(Off by default)*
//!
//! Exposes async handles for workflows in the [`handle::future`] module.
//!
//! ## `async-io`
//!
//! *(Off by default)*
//!
//! Implements [`Schedule`] trait, necessary to instantiate async workflow handles,
//! using the [`async-io`] crate. Requires the `async` feature.
//!
//! ## `log`
//!
//! *(Off by default)*
//!
//! Enables logging some tracing information during workflow execution using
//! [the eponymous crate][`log`]. The information is logged to the `tardigrade_rt` logger,
//! mostly using `TRACE` level.
//!
//! [`Schedule`]: crate::handle::future::Schedule
//! [`async-io`]: https://docs.rs/async-io/
//! [`log`]: https://docs.rs/log/
//!
//! # Examples
//!
//! ## Instantiating workflow
//!
//! ```no_run
//! use tardigrade_rt::{
//!     handle::WorkflowHandle,
//!     manager::WorkflowManager,
//!     WorkflowEngine, WorkflowModule,
//! };
//! use tardigrade::spawn::ManageWorkflowsExt;
//!
//! let module_bytes: Vec<u8> = // e.g., take from a file
//! #   vec![];
//! let engine = WorkflowEngine::default();
//! let module = WorkflowModule::new(&engine, &module_bytes)?;
//! // It is possible to inspect module definitions:
//! for (workflow_name, interface) in module.interfaces() {
//!     println!("{}: {:?}", workflow_name, interface);
//! }
//!
//! // There are 2 ways to create workflows: strongly typed workflow
//! // (which requires depending on the workflow crate), and dynamically typed
//! // workflows. The code below uses the second approach.
//! let spawner = module.for_untyped_workflow("TestWorkflow").unwrap();
//! // Workflows are created within a manager that is responsible
//! //  for their persistence and managing channels, time, and child workflows.
//! let mut manager = WorkflowManager::builder()
//!     .with_spawner("test", spawner)
//!     .build();
//! let new_workflow: WorkflowHandle<()> =
//!     manager.new_workflow("test", b"data".to_vec())?.build()?;
//!
//! // Let's initialize the workflow.
//! let receipt = manager.tick()?.into_inner()?;
//! // `receipt` contains information about WASM code execution. E.g.,
//! // this will print the executed functions and a list of important
//! // events for each of executions:
//! println!("{:?}", receipt.executions());
//! # Ok::<_, anyhow::Error>(())
//! ```
//!
//! See [`WorkflowManager`] and [`AsyncEnv`] docs for examples of what to do with workflows
//! after instantiation.
//!
//! ## Persisting and restoring workflow
//!
//! ```
//! # use tardigrade_rt::{
//! #     manager::{PersistedWorkflows, WorkflowManager},
//! #     PersistedWorkflow, WorkflowId,
//! # };
//! # fn test_wrapper(manager: WorkflowManager, workflow_id: WorkflowId) -> anyhow::Result<()> {
//! let manager: WorkflowManager = // ...
//! #   manager;
//! let workflow = manager.workflow(workflow_id).unwrap();
//! let persisted: PersistedWorkflow = workflow.persisted();
//! // The persisted workflow can be serialized:
//! let json = serde_json::to_string(&persisted)?;
//!
//! // The manager state can be serialized as well. It will contain
//! // all non-terminated workflows managed by the manager.
//!
//! let json = manager.persist(serde_json::to_string)?;
//! let persisted: PersistedWorkflows = serde_json::from_str(&json)?;
//! // The manager can then be instantiated again:
//! let manager = WorkflowManager::builder()
//!     .with_state(persisted)
//!     // set other options...
//!     .build();
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

#[macro_use]
mod utils;
mod data;
pub mod handle;
pub mod manager;
mod module;
pub mod receipt;
pub mod test;
mod workflow;

pub use crate::{
    data::{InboundChannelState, OutboundChannelState, TaskState, TimerState},
    module::{Clock, ExtendLinker, WorkflowEngine, WorkflowModule, WorkflowSpawner},
    workflow::PersistedWorkflow,
};
pub use tardigrade_shared::{ChannelId, FutureId, TaskId, TimerId, WakerId, WorkflowId};
