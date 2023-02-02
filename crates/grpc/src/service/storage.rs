//! Storage wrapper and its service implementations.

use async_stream::stream;
use futures::{stream::BoxStream, FutureExt, StreamExt};
use tonic::{Request, Response, Status};

use std::{convert::Infallible, future::Future};

use crate::proto::{
    channels_service_server::ChannelsService, push_messages_request::pushed,
    update_worker_request::UpdateType, workers_service_server::WorkersService, Channel,
    CloseChannelRequest, CreateChannelRequest, GetChannelRequest, GetMessageRequest,
    GetOrCreateWorkerRequest, HandleType, Message, MessageCodec, PushMessagesRequest,
    StreamMessagesRequest, UpdateWorkerRequest, Worker,
};
use tardigrade::spawn::CreateChannel;
use tardigrade_rt::{
    handle::StorageRef,
    storage::{InProcessConnection, Storage, StreamMessages},
};
use tardigrade_worker::{WorkerRecord, WorkerStorageConnection, WorkerStoragePool};

/// gRPC service wrapper for the Tardigrade runtime [`Storage`].
#[derive(Debug, Clone)]
pub struct StorageWrapper<S> {
    storage: S,
}

impl<S> StorageWrapper<S>
where
    S: StreamMessages + Clone + 'static,
{
    pub(super) fn new(storage: S) -> Self {
        Self { storage }
    }
}

#[tonic::async_trait]
impl<S> ChannelsService for StorageWrapper<S>
where
    S: StreamMessages + Clone + 'static,
{
    #[tracing::instrument(skip_all, err)]
    async fn create_channel(
        &self,
        _request: Request<CreateChannelRequest>,
    ) -> Result<Response<Channel>, Status> {
        let storage = StorageRef::from(&self.storage);
        let (sender, _) = storage.new_channel().await;
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

        let storage = StorageRef::from(&self.storage);
        let channel = storage.channel(channel_id).await;
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

        let storage = StorageRef::from(&self.storage);
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
    async fn get_message(
        &self,
        request: Request<GetMessageRequest>,
    ) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let codec = MessageCodec::from_i32(request.codec)
            .ok_or_else(|| Status::invalid_argument("invalid message codec specified"))?;

        let reference = request
            .r#ref
            .ok_or_else(|| Status::invalid_argument("message reference is not specified"))?;
        let channel_id = reference.channel_id;

        let storage = StorageRef::from(&self.storage);
        let receiver = storage.receiver(channel_id).await;
        let receiver = receiver
            .ok_or_else(|| Status::not_found(format!("channel {channel_id} does not exist")))?;
        let receiver = receiver.into_owned();

        let message = receiver.receive_message(reference.index).await;
        let message = message.map_err(|err| Status::not_found(err.to_string()))?;
        let message = Message::try_from_data(channel_id, message, codec)?;
        Ok(Response::new(message))
    }

    type StreamMessagesStream = BoxStream<'static, Result<Message, Status>>;

    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn stream_messages(
        &self,
        request: Request<StreamMessagesRequest>,
    ) -> Result<Response<Self::StreamMessagesStream>, Status> {
        let request = request.get_ref();
        let codec = MessageCodec::from_i32(request.codec)
            .ok_or_else(|| Status::invalid_argument("invalid message codec specified"))?;
        let channel_id = request.channel_id;
        let start_index = request.start_index;

        let storage = StorageRef::from(&self.storage);
        let receiver = storage.receiver(channel_id).await;
        let receiver = receiver
            .ok_or_else(|| Status::not_found(format!("channel {channel_id} does not exist")))?;

        let messages = if request.follow {
            receiver
                .stream_messages(start_index..)
                .map(move |message| Message::try_from_data(channel_id, message, codec))
                .boxed()
        } else {
            let receiver = receiver.into_owned();
            let messages = stream! {
                let messages = receiver.receive_messages(start_index..);
                for await message in messages {
                    yield Message::try_from_data(channel_id, message, codec);
                }
            };
            messages.boxed()
        };
        Ok(Response::new(messages))
    }

    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn push_messages(
        &self,
        request: Request<PushMessagesRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let channel_id = request.channel_id;

        let storage = StorageRef::from(&self.storage);
        let sender = storage.sender(channel_id).await;
        let sender = sender
            .ok_or_else(|| Status::not_found(format!("channel {channel_id} does not exist")))?;

        let messages: Result<Vec<_>, _> = request
            .messages
            .into_iter()
            .map(|message| match message.payload {
                None => Err(Status::invalid_argument("no message payload specified")),
                Some(pushed::Payload::Raw(bytes)) => Ok(bytes),
                Some(pushed::Payload::Str(string)) => Ok(string.into_bytes()),
            })
            .collect();

        sender
            .send_all(messages?)
            .await
            .map_err(|err| Status::aborted(err.to_string()))?;
        Ok(Response::new(()))
    }
}

// `unwrap`s in this implementation are safe due to `Infallible` errors.
#[tonic::async_trait]
impl<S> WorkersService for StorageWrapper<S>
where
    S: Storage + Clone + 'static,
    InProcessConnection<S>: WorkerStoragePool<Error = Infallible>,
{
    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    async fn get_or_create_worker(
        &self,
        request: Request<GetOrCreateWorkerRequest>,
    ) -> Result<Response<Worker>, Status> {
        let request = request.into_inner();

        let storage = InProcessConnection(self.storage.clone());
        let worker = storage
            .connect()
            .then(|view| get_or_create_worker(view, request))
            .await?;
        let worker = Worker::from_data(worker);
        Ok(Response::new(worker))
    }

    #[tracing::instrument(skip_all, err, fields(request = ?request.get_ref()))]
    #[allow(clippy::cast_possible_truncation)]
    async fn update_worker(
        &self,
        request: Request<UpdateWorkerRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        match request.update_type {
            Some(UpdateType::Cursor(cursor)) => {
                let storage = InProcessConnection(self.storage.clone());
                storage
                    .connect()
                    .then(|view| update_worker(view, request.worker_id, cursor))
                    .await;
            }
            _ => return Err(Status::invalid_argument("invalid update type")),
        }

        Ok(Response::new(()))
    }
}

#[allow(clippy::manual_async_fn)] // necessary because of `Send` bound
fn get_or_create_worker<'a, V>(
    mut view: V,
    request: GetOrCreateWorkerRequest,
) -> impl Future<Output = Result<WorkerRecord, Status>> + Send + 'a
where
    V: 'a + WorkerStorageConnection<Error = Infallible>,
{
    async move {
        let worker = if request.create_if_missing {
            view.get_or_create_worker(&request.name).await.unwrap()
        } else {
            view.worker(&request.name).await.unwrap().ok_or_else(|| {
                let message = format!("worker with name `{}` does not exist", request.name);
                Status::not_found(message)
            })?
        };
        view.release().await;
        Ok(worker)
    }
}

#[allow(clippy::manual_async_fn)] // necessary because of `Send` bound
fn update_worker<'a, V>(
    mut view: V,
    worker_id: u64,
    cursor: u64,
) -> impl Future<Output = ()> + Send + 'a
where
    V: 'a + WorkerStorageConnection<Error = Infallible>,
{
    async move {
        view.update_worker_cursor(worker_id, cursor).await.unwrap();
        view.release().await;
    }
}
