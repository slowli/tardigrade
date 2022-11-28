//! Workflow engine abstraction.

#![allow(missing_docs, clippy::missing_errors_doc)]

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use std::{fmt, sync::Arc, task::Poll};

mod wasmtime;

pub use self::wasmtime::{Wasmtime, WasmtimeInstance, WasmtimeModule, WasmtimeSpawner};
pub use crate::data::{ReportedErrorKind, WorkflowData};

use crate::{storage::ModuleRecord, WorkflowSpawner};
use tardigrade::{TaskId, WakerId};

#[async_trait]
pub trait WorkflowEngine: 'static + Send + Sync {
    type Instance: RunWorkflow + PersistWorkflow;
    type Spawner: CreateWorkflow<Spawned = Self::Instance>;
    type Module: WorkflowModule<Spawner = Self::Spawner>;

    async fn create_module(&self, record: &ModuleRecord) -> anyhow::Result<Self::Module>;
}

pub trait WorkflowModule: IntoIterator<Item = (String, WorkflowSpawner<Self::Spawner>)> {
    type Spawner: CreateWorkflow;

    fn bytes(&self) -> Arc<[u8]>;
}

pub trait CreateWorkflow: 'static + fmt::Debug + Send + Sync {
    type Spawned: RunWorkflow + PersistWorkflow;

    fn create_workflow(&self, data: WorkflowData) -> anyhow::Result<Self::Spawned>;
}

pub trait AsWorkflowData {
    fn data(&self) -> &WorkflowData;

    fn data_mut(&mut self) -> &mut WorkflowData;
}

pub trait RunWorkflow: AsWorkflowData {
    fn create_main_task(&mut self, raw_args: &[u8]) -> anyhow::Result<TaskId>;

    fn poll_task(&mut self, task_id: TaskId) -> anyhow::Result<Poll<()>>;

    fn drop_task(&mut self, task_id: TaskId) -> anyhow::Result<()>;

    fn wake_waker(&mut self, waker_id: WakerId) -> anyhow::Result<()>;
}

pub trait CreateWaker: AsWorkflowData {
    fn create_waker(&mut self) -> anyhow::Result<WakerId>;
}

pub trait PersistWorkflow {
    type Persisted: Serialize + DeserializeOwned;

    fn persist(&mut self) -> Self::Persisted;

    fn restore(&mut self, persisted: Self::Persisted) -> anyhow::Result<()>;
}
