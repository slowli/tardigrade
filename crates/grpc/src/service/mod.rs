//! gRPC service implementation.

use async_stream::stream;
use futures::{stream::BoxStream, StreamExt};
use prost_types::Timestamp;
use tokio::task;
use tonic::{Request, Response, Status};

use std::collections::HashMap;

mod mapping;
mod storage;

#[cfg(test)]
mod tests;

pub use self::storage::StorageWrapper;

use self::mapping::from_timestamp;
use crate::proto::{
    channel_config, create_workflow_request, runtime_info::ClockType,
    runtime_service_server::RuntimeService, test_service_server::TestService, AbortWorkflowRequest,
    ChannelConfig, CreateWorkflowRequest, DeployModuleRequest, GetWorkflowRequest, HandleType,
    Module, RuntimeInfo, TickResult, TickWorkflowRequest, Workflow,
};
use tardigrade::{
    handle::{Handle, HandlePathBuf},
    spawn::{CreateChannel, CreateWorkflow},
    workflow::UntypedHandles,
    ChannelId, Raw, WorkflowId,
};
use tardigrade_rt::{
    engine::WorkflowEngine,
    handle::AnyWorkflowHandle,
    manager::{AsManager, DriveConfig, ManagerSpawner, WorkflowManager, WorkflowTickError},
    storage::{
        ReadModules, ReadWorkflows, Storage, StorageTransaction, StreamMessages, Streaming,
        TransactionAsStorage,
    },
    test::MockScheduler,
    Schedule, TokioScheduler,
};

type TxStorage<'a, S> = TransactionAsStorage<<S as Storage>::Transaction<'a>>;

type TxManager<'a, M> = WorkflowManager<
    <M as AsManager>::Engine,
    <M as AsManager>::Clock,
    TxStorage<'a, <M as AsManager>::Storage>,
>;

/// Scheduler that can be described.
pub trait WithClockType: Schedule {
    #[doc(hidden)] // implementation detail
    fn clock_type() -> ClockType;
}

impl WithClockType for TokioScheduler {
    fn clock_type() -> ClockType {
        ClockType::System
    }
}

impl WithClockType for MockScheduler {
    fn clock_type() -> ClockType {
        ClockType::Mock
    }
}

/// gRPC service wrapper for the [Tardigrade runtime](WorkflowManager).
///
/// # Examples
///
/// See [crate-level docs](index.html#examples) for the examples of usage.
#[derive(Debug, Clone)]
pub struct ManagerWrapper<M> {
    inner: M,
    has_driver: bool,
    clock_type: ClockType,
}

impl<S, M: AsManager<Storage = Streaming<S>>> ManagerWrapper<M>
where
    S: Storage + Clone + 'static,
    M::Clock: Schedule,
{
    /// Creates a new wrapper around the provided `manager`. Drives the manager in the background
    /// task using [`WorkflowManager::drive()`].
    pub fn new(mut manager: M) -> Self {
        let storage = manager.as_manager_mut().storage_mut();
        let mut commits_rx = storage.stream_commits();
        {
            let manager = manager.as_manager().clone();
            let mut config = DriveConfig::new();
            config.wait_for_workflows();
            task::spawn(async move {
                manager.drive(&mut commits_rx, config).await;
            });
        }

        Self {
            inner: manager,
            has_driver: true,
            clock_type: ClockType::Unspecified,
        }
    }

    /// Returns a service wrapper for the underlying storage.
    pub fn storage_wrapper(&self) -> StorageWrapper<Streaming<S>> {
        let storage = self.inner.as_manager().storage();
        StorageWrapper::new(storage.as_ref().clone())
    }
}

impl<M: AsManager> ManagerWrapper<M>
where
    M::Clock: WithClockType,
{
    /// Records the clock type used in the wrapped runtime.
    pub fn set_clock_type(&mut self) {
        self.clock_type = <M::Clock>::clock_type();
    }
}

/// Wraps the specified manager. Unlike [`Self::new()`], does not start any background tasks.
impl<S, M: AsManager<Storage = Streaming<S>>> From<M> for ManagerWrapper<M>
where
    S: Storage + Clone + 'static,
    M::Clock: Schedule,
{
    fn from(manager: M) -> Self {
        Self {
            inner: manager,
            has_driver: false,
            clock_type: ClockType::Unspecified,
        }
    }
}

impl<M: AsManager> ManagerWrapper<M>
where
    M::Storage: StreamMessages + Clone + 'static,
{
    #[tracing::instrument(level = "debug", skip(spawner), err)]
    async fn create_handles<'r, 'a: 'r>(
        spawner: &ManagerSpawner<'r, TxManager<'a, M>, Raw>,
        config: &HashMap<String, ChannelConfig>,
    ) -> Result<UntypedHandles<Raw>, Status> {
        let mut handles = UntypedHandles::<Raw>::new();
        for (path, channel) in config {
            let path = HandlePathBuf::from(path.as_str());
            let handle = match channel.reference {
                Some(channel_config::Reference::New(())) | None => {
                    Self::create_handle(spawner, channel.r#type, &path).await?
                }
                Some(channel_config::Reference::Existing(id)) => {
                    Self::get_channel(channel.r#type, id, &path)?
                }
            };
            handles.insert(path, handle);
        }
        Ok(handles)
    }

    #[tracing::instrument(level = "debug", skip(spawner), err)]
    async fn create_handle<'r, 'a: 'r>(
        spawner: &ManagerSpawner<'r, TxManager<'a, M>, Raw>,
        ty: i32,
        path: &HandlePathBuf,
    ) -> Result<Handle<ChannelId>, Status> {
        Ok(match HandleType::from_i32(ty) {
            Some(HandleType::Receiver) => {
                let (_, receiver) = spawner.new_channel().await;
                Handle::Receiver(receiver)
            }
            Some(HandleType::Sender) => {
                let (sender, _) = spawner.new_channel().await;
                Handle::Sender(sender)
            }
            Some(HandleType::Unspecified) | None => {
                let message = format!("Invalid handle type specified for handle at `{path}`");
                return Err(Status::invalid_argument(message));
            }
        })
    }

    #[tracing::instrument(level = "debug", err)]
    fn get_channel(
        ty: i32,
        id: ChannelId,
        path: &HandlePathBuf,
    ) -> Result<Handle<ChannelId>, Status> {
        Ok(match HandleType::from_i32(ty) {
            Some(HandleType::Receiver) => Handle::Receiver(id),

            Some(HandleType::Sender) => Handle::Sender(id),

            Some(HandleType::Unspecified) | None => {
                let message = format!("Invalid handle type specified for handle at `{path}`");
                return Err(Status::invalid_argument(message));
            }
        })
    }

    async fn do_get_workflow(&self, workflow_id: WorkflowId) -> Result<Response<Workflow>, Status> {
        let storage = self.inner.as_manager().storage();
        let storage = storage.as_ref();
        let transaction = storage.readonly_transaction().await;
        let workflow = transaction.workflow(workflow_id).await;

        let workflow = workflow
            .ok_or_else(|| Status::not_found(format!("workflow {workflow_id} does not exist")))?;
        let workflow = Workflow::from_record(workflow);
        Ok(Response::new(workflow))
    }

    async fn do_create_workflow(
        &self,
        request: Request<CreateWorkflowRequest>,
    ) -> Result<WorkflowId, Status> {
        let request = request.into_inner();
        let args = match request.args {
            None => return Err(Status::invalid_argument("arguments are not specified")),
            Some(create_workflow_request::Args::RawArgs(bytes)) => bytes,
            Some(create_workflow_request::Args::StrArgs(string)) => string.into_bytes(),
        };

        let definition_id = format!("{}::{}", request.module_id, request.name_in_module);
        let mut manager = self.inner.as_manager().clone();
        let tx_manager = manager.in_transaction().await;

        let spawner = tx_manager.raw_spawner();
        let builder = spawner.new_workflow::<()>(&definition_id).await;
        let builder = builder.map_err(|_| {
            let message = format!("workflow definition `{definition_id}` does not exist");
            Status::not_found(message)
        })?;

        let handles = Self::create_handles(&spawner, &request.channels).await?;
        let workflow_id = builder.build(args, handles).await;
        let workflow_id =
            workflow_id.map_err(|err| Status::invalid_argument(format!("{err:#}")))?;

        let Some(transaction) = tx_manager.into_storage().into_inner() else {
            return Err(Status::internal("cannot commit transaction"));
        };
        transaction.commit().await;
        Ok(workflow_id)
    }
}

#[tonic::async_trait]
impl<M> RuntimeService for ManagerWrapper<M>
where
    M: AsManager + 'static,
    M::Storage: StreamMessages + Clone + 'static,
{
    #[tracing::instrument(skip_all, err)]
    async fn get_info(&self, _request: Request<()>) -> Result<Response<RuntimeInfo>, Status> {
        Ok(Response::new(RuntimeInfo {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            has_driver: self.has_driver,
            clock_type: self.clock_type.into(),
        }))
    }

    #[tracing::instrument(skip_all, err, fields(request.id = request.get_ref().id))]
    async fn deploy_module(
        &self,
        request: Request<DeployModuleRequest>,
    ) -> Result<Response<Module>, Status> {
        let manager = self.inner.as_manager();
        let request = request.into_inner();
        let module = manager
            .engine()
            .create_module(request.bytes.into())
            .await
            .map_err(|err| Status::invalid_argument(format!("{err:#}")))?;

        let module = if request.dry_run {
            Module::from_engine_module(request.id, module)
        } else {
            let module = manager.insert_module(&request.id, module).await;
            Module::from_record(module)
        };
        Ok(Response::new(module))
    }

    type ListModulesStream = BoxStream<'static, Result<Module, Status>>;

    #[tracing::instrument(skip_all, err)]
    async fn list_modules(
        &self,
        _request: Request<()>,
    ) -> Result<Response<Self::ListModulesStream>, Status> {
        let storage = self.inner.as_manager().storage();
        let storage = storage.as_ref().clone();

        let modules = stream! {
            let transaction = storage.readonly_transaction().await;
            for await module in transaction.modules() {
                yield Ok(Module::from_record(module));
            }
        };
        Ok(Response::new(modules.boxed()))
    }

    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn create_workflow(
        &self,
        request: Request<CreateWorkflowRequest>,
    ) -> Result<Response<Workflow>, Status> {
        let workflow_id = self.do_create_workflow(request).await?;
        self.do_get_workflow(workflow_id).await
    }

    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn get_workflow(
        &self,
        request: Request<GetWorkflowRequest>,
    ) -> Result<Response<Workflow>, Status> {
        let workflow_id = request.get_ref().id;
        self.do_get_workflow(workflow_id).await
    }

    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn tick_workflow(
        &self,
        request: Request<TickWorkflowRequest>,
    ) -> Result<Response<TickResult>, Status> {
        let workflow_id = request.get_ref().workflow_id;

        let manager = self.inner.as_manager();
        let result = if let Some(workflow_id) = workflow_id {
            match manager.tick_workflow(workflow_id).await {
                Ok(result) => Ok(result),
                Err(WorkflowTickError::WouldBlock(err)) => Err(err),
                Err(WorkflowTickError::NotFound) => {
                    let message = format!("workflow {workflow_id} does not exist");
                    return Err(Status::not_found(message));
                }
                Err(err) => {
                    let message = format!("cannot tick workflow {workflow_id}: {err}");
                    return Err(Status::failed_precondition(message));
                }
            }
        } else {
            manager.tick().await
        };
        let result = result.map_err(|would_block| (workflow_id, would_block));

        let result = TickResult::from_data(result);
        Ok(Response::new(result))
    }

    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn abort_workflow(
        &self,
        request: Request<AbortWorkflowRequest>,
    ) -> Result<Response<Workflow>, Status> {
        let workflow_id = request.get_ref().id;

        let storage = self.inner.as_manager().storage();
        let workflow = storage.any_workflow(workflow_id).await;
        let workflow = workflow
            .ok_or_else(|| Status::not_found(format!("workflow {workflow_id} does not exist")))?;

        let result = match workflow {
            AnyWorkflowHandle::Active(workflow) => workflow.abort().await,
            AnyWorkflowHandle::Errored(workflow) => workflow.abort().await,
            AnyWorkflowHandle::Completed(_) => {
                let message = format!("workflow {workflow_id} is completed and cannot be aborted");
                return Err(Status::failed_precondition(message));
            }
            _ => {
                let message = format!("workflow {workflow_id} has unknown state");
                return Err(Status::unimplemented(message));
            }
        };
        result
            .map_err(|err| Status::failed_precondition(format!("cannot abort workflow: {err}")))?;

        self.do_get_workflow(workflow_id).await
    }
}

#[tonic::async_trait]
impl<M> TestService for ManagerWrapper<M>
where
    M: AsManager<Clock = MockScheduler> + 'static,
    M::Storage: StreamMessages + Clone + 'static,
{
    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn set_time(&self, request: Request<Timestamp>) -> Result<Response<()>, Status> {
        let timestamp = request.into_inner();
        let timestamp = from_timestamp(timestamp)
            .ok_or_else(|| Status::invalid_argument("provided timestamp is invalid"))?;

        let manager = self.inner.as_manager();
        manager.clock().set_now(timestamp);
        if !self.has_driver {
            manager.set_current_time(timestamp).await;
        }

        Ok(Response::new(()))
    }
}
