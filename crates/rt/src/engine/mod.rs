//! Workflow engine abstraction.
//!
//! # Overview
//!
//! A [`WorkflowEngine`] is responsible for:
//!
//! - Transforming a WASM [*module*](WorkflowModule) into one or more named
//!   workflow [*definitions*](DefineWorkflow)
//! - Instantiating zero or more workflow *instances* from a definition.
//! - [Running](RunWorkflow) and [persisting](PersistWorkflow) workflow instances
//!
//! The crate provides a [default engine implementation](Wasmtime)
//! powered by the [`wasmtime`] crate.
//!
//! [`wasmtime`]: https://docs.rs/wasmtime/

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use std::{fmt, sync::Arc, task::Poll};

mod wasmtime;

pub use self::wasmtime::{Wasmtime, WasmtimeDefinition, WasmtimeInstance, WasmtimeModule};
pub use crate::data::{
    ChildActions, PersistedWorkflowData, ReceiverActions, ReportedErrorKind, SenderActions,
    TaskActions, TimerActions, WorkflowData, WorkflowPoll,
};

use crate::storage::ModuleRecord;
use tardigrade::{interface::Interface, spawn::HostError, ChannelId, TaskId, WakerId, WorkflowId};

/// Workflow engine.
#[async_trait]
pub trait WorkflowEngine: 'static + Send + Sync {
    /// Instance of a workflow created by this engine.
    type Instance: RunWorkflow + PersistWorkflow;
    /// Spawner of [instances](Self::Instance) created by this engine.
    type Definition: DefineWorkflow<Instance = Self::Instance>;
    /// Module defining one or more workflows.
    type Module: WorkflowModule<Definition = Self::Definition>;

    /// Creates a new module based on the specified `bytes`.
    async fn create_module(&self, bytes: Arc<[u8]>) -> anyhow::Result<Self::Module>;

    /// Restores a module given the specified storage record.
    ///
    /// The default implementation calls [`Self::create_module()`] with `record.bytes`.
    async fn restore_module(&self, record: &ModuleRecord) -> anyhow::Result<Self::Module> {
        self.create_module(Arc::clone(&record.bytes)).await
    }
}

/// Workflow module.
pub trait WorkflowModule: IntoIterator<Item = (String, Self::Definition)> + Send {
    /// Workflow definition contained in a module of this type.
    type Definition: DefineWorkflow;
    /// Returns module bytes.
    fn bytes(&self) -> Arc<[u8]>;
}

/// Workflow definition.
pub trait DefineWorkflow: 'static + fmt::Debug + Send + Sync {
    /// Instance of a workflow created from this definition.
    type Instance: RunWorkflow + PersistWorkflow;

    /// Returns the interface of the workflow spawned from this definition.
    fn interface(&self) -> &Interface;

    /// Creates a workflow instance from this definition.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails, e.g., due to WASM linking errors.
    fn create_workflow(&self, data: WorkflowData) -> anyhow::Result<Self::Instance>;
}

/// Provides access to wrapped [`WorkflowData`].
pub trait AsWorkflowData {
    /// Provides a shared reference to the workflow data.
    fn data(&self) -> &WorkflowData;
    /// Provides an exclusive reference to the workflow data.
    fn data_mut(&mut self) -> &mut WorkflowData;
}

/// Workflow instance execution logic.
///
/// A workflow instance [wraps](AsWorkflowData) [`WorkflowData`] and can use its methods
/// to manipulate the workflow in response to calls in the workflow logic (e.g., polling
/// a next item for a channel receiver). Thus, `WorkflowData` methods conceptually map to
/// WASM module imports. Conversely, methods in the `RunWorkflow` trait conceptually map
/// to WASM module exports.
pub trait RunWorkflow: AsWorkflowData + Send {
    /// Creates the main workflow task and returns its ID.
    ///
    /// # Errors
    ///
    /// Returns an error if creating the main task fails.
    fn create_main_task(&mut self, raw_args: &[u8]) -> anyhow::Result<TaskId>;

    /// Polls a task with the specified ID.
    ///
    /// # Errors
    ///
    /// Returns an error if polling the task fails, e.g., due to a WASM trap.
    fn poll_task(&mut self, task_id: TaskId) -> anyhow::Result<Poll<()>>;

    /// Drops a task with the specified ID.
    fn drop_task(&mut self, task_id: TaskId);

    /// Wakes a waker with the specified ID.
    fn wake_waker(&mut self, waker_id: WakerId);

    /// Notifies that the definition with the specified `stub_id`, which was previously supplied
    /// to [`WorkflowData::FIXME()`], has been resolved with the specified `result`.
    fn resolve_definition(&mut self, stub_id: u64, result: Option<Interface>);

    /// Notifies that the child workflow with the specified `stub_id`, which was previously supplied
    /// to [`WorkflowData::create_workflow_stub()`], has been initialized
    /// with the specified `result`. Either the child was allocated the ID wrapped in `Ok(_)`, or
    /// an error has occurred during initialization.
    fn initialize_child(&mut self, stub_id: WorkflowId, result: Result<WorkflowId, HostError>);

    /// Notifies that the channel with the specified `stub_id`, which was previously supplied
    /// to [`WorkflowData::create_channel_stub()`], has been created and was allocated
    /// the provided `channel_id`.
    fn initialize_channel(&mut self, stub_id: ChannelId, channel_id: ChannelId);
}

/// Creating wakers in workflows. This is used in [`WorkflowPoll`] to unwrap the contained
/// [`Poll`] result.
pub trait CreateWaker: AsWorkflowData {
    /// Creates a new waker.
    ///
    /// # Errors
    ///
    /// Returns an error if creating the waker fails, e.g., due to a WASM trap.
    fn create_waker(&mut self) -> anyhow::Result<WakerId>;
}

/// Workflow persistence logic.
pub trait PersistWorkflow: AsWorkflowData {
    /// Persisted workflow data. This data should not include [`WorkflowData`] (it is persisted
    /// separately), but rather engine-specific data, such as the WASM memory.
    type Persisted: Serialize + DeserializeOwned;

    /// Persists this workflow.
    fn persist(&mut self) -> Self::Persisted;

    /// Restores this workflow from a previously persisted state.
    ///
    /// # Errors
    ///
    /// Returns an error if restoration fails for whatever reason (e.g., the persisted data format
    /// is not recognized).
    fn restore(&mut self, persisted: Self::Persisted) -> anyhow::Result<()>;
}
