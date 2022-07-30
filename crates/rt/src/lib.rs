//! Tardigrade runtime library.
//!
//! The runtime provides host environment in which [`Workflow`]s defined in a WASM module
//! can be executed and [persisted](PersistedWorkflow) / restored. Interaction with a workflow
//! (e.g., submitting messages to inbound channels or taking messages from outbound channels)
//! can be performed using low-level, synchronous [`WorkflowHandle`], or more high-level,
//! future-based [`AsyncEnv`].
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
//! 4. [`Workflow`] is an instance of a workflow. It can be created from a [`WorkflowSpawner`].
//!
//! [`wasmtime`]: https://docs.rs/wasmtime/latest/wasmtime/
//! [`WorkflowHandle`]: crate::handle::WorkflowHandle
//! [`AsyncEnv`]: crate::handle::future::AsyncEnv
//!
//! # Crate features
//!
//! ## `async`
//!
//! *(Off by default)*
//!
//! Exposes async handles for [`Workflow`]s in the [`handle::future`] module.
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
//! use tardigrade::workflow::InputsBuilder;
//! use tardigrade_rt::{Workflow, WorkflowEngine, WorkflowModule};
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
//! let mut inputs = InputsBuilder::new(&spawner.interface());
//! println!("{:?}", inputs.missing_input_names().collect::<Vec<_>>());
//! // After filling inputs somehow...
//! let receipt = spawner.spawn(inputs.build())?.init()?;
//! // `receipt` contains information about WASM code execution. E.g.,
//! // this will print the executed functions and a list of important
//! // events for each of executions:
//! println!("{:?}", receipt.executions());
//!
//! // The workflow is contained within the receipt:
//! let workflow: Workflow<()> = receipt.into_inner();
//! # Ok::<_, anyhow::Error>(())
//! ```
//!
//! See [`WorkflowEnv`] and [`AsyncEnv`] docs for examples of what to do with workflows
//! after instantiation.
//!
//! [`WorkflowEnv`]: crate::handle::WorkflowEnv
//! [`AsyncEnv`]: crate::handle::future::AsyncEnv
//!
//! ## Persisting and restoring workflow
//!
//! ```
//! # use tardigrade_rt::{PersistedWorkflow, Workflow, WorkflowSpawner};
//! # fn test_wrapper(spawner: WorkflowSpawner<()>, workflow: Workflow<()>) -> anyhow::Result<()> {
//! let mut workflow: Workflow<()> = // ...
//! #   workflow;
//! let persisted: PersistedWorkflow = workflow.persist()?;
//! // The persisted workflow can be serialized:
//! let json = serde_json::to_string(&persisted)?;
//! let persisted: PersistedWorkflow = serde_json::from_str(&json)?;
//! // The workflow can then be instantiated again using a `WorkflowSpawner`:
//! let spawner: WorkflowSpawner<()> = // ...
//! #   spawner;
//! let restored_workflow: Workflow<()> = persisted.restore(&spawner)?;
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

mod data;
pub mod handle;
mod module;
pub mod receipt;
pub mod test;
mod utils;
mod workflow;

pub use crate::{
    data::{
        ConsumeError, ConsumeErrorKind, InboundChannelState, OutboundChannelState, PersistError,
        TaskState, TimerState,
    },
    module::{Clock, ExtendLinker, WorkflowEngine, WorkflowModule, WorkflowSpawner},
    workflow::{InitializingWorkflow, PersistedWorkflow, Workflow},
};
pub use tardigrade_shared::{FutureId, TaskId, TimerId, WakerId};
