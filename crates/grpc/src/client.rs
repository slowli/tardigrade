//! Client gRPC stubs.

use futures::StreamExt;
use tardigrade::ChannelId;
use tonic::{transport::Channel, Status};

use std::error;

use crate::proto::{
    channels_service_client::ChannelsServiceClient,
    message::Payload,
    push_messages_request::{pushed, Pushed},
    update_worker_request::UpdateType,
    workers_service_client::WorkersServiceClient,
    GetOrCreateWorkerRequest, MessageCodec, PushMessagesRequest, StreamMessagesRequest,
    UpdateWorkerRequest,
};
use tardigrade_worker::{MessageStream, WorkerConnection, WorkerRecord, WorkerStorageView};

/// gRPC client for the [Tardigrade runtime service](crate::ManagerService).
#[derive(Debug, Clone)]
pub struct Client {
    channels: ChannelsServiceClient<Channel>,
    workers: WorkersServiceClient<Channel>,
}

impl Client {
    /// Creates a new client with the specified endpoint to connect to.
    ///
    /// # Errors
    ///
    /// Returns an error if connecting to the endpoint fails.
    pub async fn new(uri: String) -> Result<Self, Box<dyn error::Error + Send + Sync>> {
        let channel = Channel::from_shared(uri)?.connect().await?;
        Ok(Self::from(channel))
    }
}

impl From<Channel> for Client {
    fn from(channel: Channel) -> Self {
        Self {
            channels: ChannelsServiceClient::new(channel.clone()),
            workers: WorkersServiceClient::new(channel),
        }
    }
}

#[tonic::async_trait]
impl WorkerConnection for Client {
    type Error = Status;
    type View<'a> = Self;

    async fn view(&self) -> Self::View<'_> {
        self.clone()
    }

    #[tracing::instrument(level = "debug")]
    #[allow(clippy::cast_possible_truncation)] // TODO: use `u64` for indexing messages
    fn stream_messages(
        &self,
        channel_id: ChannelId,
        start_idx: usize,
    ) -> MessageStream<Self::Error> {
        let mut client = self.channels.clone();
        let request = StreamMessagesRequest {
            channel_id,
            start_index: start_idx as u64,
            codec: MessageCodec::Unspecified.into(),
            follow: true,
        };

        let messages = async_stream::try_stream! {
            let response = client.stream_messages(request).await?;

            for await message in response.into_inner() {
                let message = message?;
                let reference = message.reference.ok_or_else(|| {
                    Status::invalid_argument("missing reference for returned message")
                })?;

                let payload = match message.payload {
                    Some(Payload::Raw(payload)) => Some(payload),
                    _ => None,
                };
                let payload = payload.ok_or_else(|| {
                    Status::invalid_argument("unexpected payload for returned message")
                })?;
                yield (reference.index as usize, payload);
            }
        };
        messages.boxed()
    }
}

#[tonic::async_trait]
impl WorkerStorageView for Client {
    type Error = Status;

    #[tracing::instrument(level = "debug", err)]
    #[allow(clippy::cast_possible_truncation)]
    async fn get_or_create_worker(&mut self, name: &str) -> Result<WorkerRecord, Self::Error> {
        let request = GetOrCreateWorkerRequest {
            name: name.to_owned(),
        };
        let worker = self.workers.get_or_create_worker(request).await?;

        let worker = worker.into_inner();
        Ok(WorkerRecord {
            id: worker.id,
            name: worker.name,
            inbound_channel_id: worker.inbound_channel_id,
            cursor: worker.cursor as usize,
        })
    }

    #[tracing::instrument(level = "debug", err)]
    async fn update_worker_cursor(
        &mut self,
        worker_id: u64,
        cursor: usize,
    ) -> Result<(), Self::Error> {
        let request = UpdateWorkerRequest {
            worker_id,
            update_type: Some(UpdateType::Cursor(cursor as u64)),
        };
        self.workers.update_worker(request).await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", err)]
    async fn push_message(
        &mut self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), Self::Error> {
        let request = PushMessagesRequest {
            channel_id,
            messages: vec![Pushed {
                payload: Some(pushed::Payload::Raw(message)),
            }],
        };
        self.channels.push_messages(request).await.map(drop)
    }

    async fn commit(self) {
        // Does nothing.
    }
}
