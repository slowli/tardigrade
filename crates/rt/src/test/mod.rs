//! Helpers for workflow integration testing.
//!
//! # Examples
//!
//! Typically, it is useful to cache a [`WorkflowModule`](crate::engine::WorkflowModule)
//! among multiple tests. This can be performed as follows:
//!
//! ```no_run
//! use async_std::task;
//! use once_cell::sync::Lazy;
//! use tardigrade_rt::{test::*, engine::{Wasmtime, WasmtimeModule, WorkflowEngine}};
//!
//! static MODULE: Lazy<WasmtimeModule> = Lazy::new(|| {
//!     let module_bytes = ModuleCompiler::new(env!("CARGO_PKG_NAME"))
//!         .set_current_dir(env!("CARGO_MANIFEST_DIR"))
//!         .set_profile("wasm")
//!         .set_wasm_opt(WasmOpt::default())
//!         .compile();
//!     let engine = Wasmtime::default();
//!     let task = engine.create_module(module_bytes.into());
//!     task::block_on(task).unwrap()
//! });
//! // The module can then be used in tests
//! ```

#[cfg(feature = "test")]
mod compiler;
#[doc(hidden)] // not baked for external use yet
pub mod engine;

#[cfg(feature = "test")]
pub use self::compiler::{ModuleCompiler, WasmOpt};
