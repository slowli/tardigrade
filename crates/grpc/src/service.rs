//! gRPC service implementation.

use async_stream::stream;
use futures::{stream::BoxStream, StreamExt};
use tokio::task;
use tonic::{Request, Response, Status};

use std::collections::HashMap;

use crate::proto::{
    channel_config, tardigrade_channels_server::TardigradeChannels, tardigrade_server::Tardigrade,
    AbortWorkflowRequest, Channel, ChannelConfig, CloseChannelRequest, CreateChannelRequest,
    CreateWorkflowRequest, DeployModuleRequest, GetChannelRequest, GetWorkflowRequest, HandleType,
    Message, MessageRef, Module, PushMessagesRequest, StreamMessagesRequest, Workflow,
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
    manager::{AsManager, DriveConfig, ManagerSpawner, WorkflowManager},
    storage::{
        ReadModules, ReadWorkflows, Storage, StorageTransaction, StreamMessages, Streaming,
        TransactionAsStorage,
    },
    Schedule,
};

type TxStorage<'a, S> = TransactionAsStorage<<S as Storage>::Transaction<'a>>;

type TxManager<'a, M> = WorkflowManager<
    <M as AsManager>::Engine,
    <M as AsManager>::Clock,
    TxStorage<'a, <M as AsManager>::Storage>,
>;

#[derive(Debug, Clone)]
pub struct ManagerWrapper<M> {
    inner: M,
}

impl<S, M: AsManager<Storage = Streaming<S>>> ManagerWrapper<M>
where
    S: Storage + Clone + 'static,
    M::Clock: Schedule,
{
    /// Creates a new wrapper around the provided `manager`.
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

        Self { inner: manager }
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
        let workflow_id = builder.build(request.args, handles).await;
        let workflow_id = workflow_id.map_err(|err| Status::invalid_argument(err.to_string()))?;

        let Some(transaction) = tx_manager.into_storage().into_inner() else {
            return Err(Status::internal("cannot commit transaction"));
        };
        transaction.commit().await;
        Ok(workflow_id)
    }
}

#[tonic::async_trait]
impl<M> Tardigrade for ManagerWrapper<M>
where
    M: AsManager + 'static,
    M::Storage: StreamMessages + Clone + 'static,
{
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
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let module = manager.insert_module(&request.id, module).await;
        Ok(Response::new(Module::from_record(module)))
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
impl<M> TardigradeChannels for ManagerWrapper<M>
where
    M: AsManager + 'static,
    M::Storage: StreamMessages + Clone + 'static,
{
    #[tracing::instrument(skip_all, err)]
    async fn create_channel(
        &self,
        _request: Request<CreateChannelRequest>,
    ) -> Result<Response<Channel>, Status> {
        let manager = self.inner.as_manager();
        let (sender, _) = manager.storage().new_channel().await;
        let channel = sender.channel_info().clone();
        let channel = Channel::from_record(sender.channel_id(), channel);
        Ok(Response::new(channel))
    }

    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn get_channel(
        &self,
        request: Request<GetChannelRequest>,
    ) -> Result<Response<Channel>, Status> {
        let channel_id = request.get_ref().id;
        let manager = self.inner.as_manager();
        let channel = manager.storage().channel(channel_id).await;
        let channel = channel
            .ok_or_else(|| Status::not_found(format!("channel {channel_id} does not exist")))?;
        let channel = Channel::from_record(channel_id, channel);
        Ok(Response::new(channel))
    }

    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn close_channel(
        &self,
        request: Request<CloseChannelRequest>,
    ) -> Result<Response<Channel>, Status> {
        let channel_id = request.get_ref().id;
        let handle_to_close = HandleType::from_i32(request.get_ref().half)
            .ok_or_else(|| Status::invalid_argument("invalid channel half specified"))?;

        let storage = self.inner.as_manager().storage();
        match handle_to_close {
            HandleType::Sender => {
                let sender = storage.sender(channel_id).await.ok_or_else(|| {
                    Status::not_found(format!("channel {channel_id} does not exist"))
                })?;
                sender.close().await;
            }

            HandleType::Receiver => {
                let receiver = storage.receiver(channel_id).await.ok_or_else(|| {
                    Status::not_found(format!("channel {channel_id} does not exist"))
                })?;
                receiver.close().await;
            }

            HandleType::Unspecified => {
                let message = "invalid channel half specified";
                return Err(Status::invalid_argument(message));
            }
        }

        // The channel should exist at this point, but we handle errors just in case.
        let channel = storage
            .channel(channel_id)
            .await
            .ok_or_else(|| Status::not_found(format!("channel {channel_id} does not exist")))?;
        let channel = Channel::from_record(channel_id, channel);
        Ok(Response::new(channel))
    }

    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn get_message(&self, request: Request<MessageRef>) -> Result<Response<Message>, Status> {
        let channel_id = request.get_ref().channel_id;
        let message_idx = request.get_ref().index as usize;

        let manager = self.inner.as_manager();
        let receiver = manager.storage().receiver(channel_id).await;
        let receiver = receiver
            .ok_or_else(|| Status::not_found(format!("channel {channel_id} does not exist")))?;
        let receiver = receiver.into_owned();

        let message = receiver.receive_message(message_idx).await;
        let message = message.map_err(|err| Status::not_found(err.to_string()))?;
        let message = Message::from_data(channel_id, message);
        Ok(Response::new(message))
    }

    type StreamMessagesStream = BoxStream<'static, Result<Message, Status>>;

    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn stream_messages(
        &self,
        request: Request<StreamMessagesRequest>,
    ) -> Result<Response<Self::StreamMessagesStream>, Status> {
        let channel_id = request.get_ref().id;
        let start_index = request.get_ref().start_index as usize;

        let manager = self.inner.as_manager();
        let receiver = manager.storage().receiver(channel_id).await;
        let receiver = receiver
            .ok_or_else(|| Status::not_found(format!("channel {channel_id} does not exist")))?;

        let messages = receiver
            .stream_messages(start_index..)
            .map(move |message| Ok(Message::from_data(channel_id, message)))
            .boxed();
        Ok(Response::new(messages))
    }

    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn push_messages(
        &self,
        request: Request<PushMessagesRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let channel_id = request.channel_id;

        let manager = self.inner.as_manager();
        let sender = manager.storage().sender(channel_id).await;
        let sender = sender
            .ok_or_else(|| Status::not_found(format!("channel {channel_id} does not exist")))?;

        sender
            .send_all(request.payloads)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))?;
        Ok(Response::new(()))
    }
}
