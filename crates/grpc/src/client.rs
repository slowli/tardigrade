//! Client gRPC stubs.

use futures::StreamExt;
use tardigrade::ChannelId;
use tonic::{transport::Channel, Code, Status};

use std::error;

use crate::proto::{
    channels_service_client::ChannelsServiceClient,
    message::Payload,
    push_messages_request::{pushed, Pushed},
    update_worker_request::UpdateType,
    workers_service_client::WorkersServiceClient,
    CloseChannelRequest, GetOrCreateWorkerRequest, HandleType, MessageCodec, PushMessagesRequest,
    StreamMessagesRequest, UpdateWorkerRequest,
};
use tardigrade_worker::{MessageStream, WorkerRecord, WorkerStorageConnection, WorkerStoragePool};

/// gRPC client for the [Tardigrade runtime service](crate::RuntimeWrapper).
///
/// The client does not expose all gRPC service methods directly; rather, it implements
/// the [`WorkerStoragePool`] and [`WorkerStorageConnection`] traits to enable
/// *remote* workers connecting to the runtime server via gRPC (as opposed to the in-process
/// workers enabled by the runtime crate).
///
/// A client can be created either from the given URI using [`Self::new()`], or [`From`]
/// an initialized [`Channel`]. The latter approach allows for more customization
/// (e.g., multiplexing the channel with other clients, or using non-TCP/IP transport such as
/// Unix domain sockets).
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

    #[allow(clippy::cast_possible_truncation)]
    async fn get_worker(
        &mut self,
        request: GetOrCreateWorkerRequest,
    ) -> Result<WorkerRecord, Status> {
        let worker = self.workers.get_or_create_worker(request).await?;

        let worker = worker.into_inner();
        Ok(WorkerRecord {
            id: worker.id,
            name: worker.name,
            inbound_channel_id: worker.inbound_channel_id,
            cursor: worker.cursor,
        })
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
impl WorkerStoragePool for Client {
    type Error = Status;
    type Connection<'a> = Self;

    async fn connect(&self) -> Self::Connection<'_> {
        self.clone()
    }

    #[tracing::instrument(level = "debug")]
    fn stream_messages(&self, channel_id: ChannelId, start_idx: u64) -> MessageStream<Self::Error> {
        let mut client = self.channels.clone();
        let request = StreamMessagesRequest {
            channel_id,
            start_index: start_idx,
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
                yield (reference.index, payload);
            }
        };
        messages.boxed()
    }
}

#[tonic::async_trait]
impl WorkerStorageConnection for Client {
    type Error = Status;

    #[tracing::instrument(level = "debug", err)]
    async fn worker(&mut self, name: &str) -> Result<Option<WorkerRecord>, Self::Error> {
        let request = GetOrCreateWorkerRequest {
            name: name.to_owned(),
            create_if_missing: false,
        };
        match self.get_worker(request).await {
            Ok(worker) => Ok(Some(worker)),
            Err(err) if err.code() == Code::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    #[tracing::instrument(level = "debug", err)]
    async fn get_or_create_worker(&mut self, name: &str) -> Result<WorkerRecord, Self::Error> {
        let request = GetOrCreateWorkerRequest {
            name: name.to_owned(),
            create_if_missing: true,
        };
        self.get_worker(request).await
    }

    #[tracing::instrument(level = "debug", err)]
    async fn update_worker_cursor(
        &mut self,
        worker_id: u64,
        cursor: u64,
    ) -> Result<(), Self::Error> {
        let request = UpdateWorkerRequest {
            worker_id,
            update_type: Some(UpdateType::Cursor(cursor)),
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

    #[tracing::instrument(level = "debug", err)]
    async fn close_response_channel(&mut self, channel_id: ChannelId) -> Result<bool, Self::Error> {
        let request = CloseChannelRequest {
            id: channel_id,
            half: HandleType::Sender.into(),
        };
        let channel = self.channels.close_channel(request).await?.into_inner();
        Ok(channel.is_closed)
    }

    async fn release(self) {
        // Does nothing.
    }
}
