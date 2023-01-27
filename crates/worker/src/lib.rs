//! Workers for external tasks in Tardigrade runtime.

use async_trait::async_trait;
use futures::TryStreamExt;

mod connection;

pub use crate::connection::{
    MessageStream, WorkerRecord, WorkerStorageConnection, WorkerStoragePool,
};

use tardigrade_shared::{self as shared, ChannelId, Codec};

/// Interface of a worker that can handle requests of a certain type.
pub trait WorkerInterface: Send {
    /// Registered name of this worker, such as `tardigrade.v1.Timer`.
    const REGISTERED_NAME: &'static str;

    /// Request input into this worker.
    type Request;
    /// Response output from this worker.
    type Response;
    /// Codec for requests / responses.
    type Codec: Codec<shared::Request<Self::Request>> + Codec<shared::Response<Self::Response>>;
}

/// Handler of requests of a certain type.
#[async_trait]
pub trait HandleRequest<C: WorkerStorageConnection>: WorkerInterface {
    /// Handles a request.
    async fn handle_request(
        &mut self,
        request: Request<Self::Request>,
        response: Response<'_, Self::Response, C>,
    );

    /// Handles request cancellation.
    #[allow(unused)]
    async fn handle_cancellation(&mut self, cancellation: Request<()>, connection: &mut C) {
        // Do nothing
    }
}

/// Request payload together with additional metadata allowing to identify the request.
#[derive(Debug)]
pub struct Request<T> {
    response_channel_id: ChannelId,
    request_id: u64,
    data: T,
}

impl<T> Request<T> {
    pub fn response_channel_id(&self) -> ChannelId {
        self.response_channel_id
    }

    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    pub fn get_ref(&self) -> &T {
        &self.data
    }

    pub fn into_inner(self) -> T {
        self.data
    }
}

#[derive(Debug)]
pub struct Response<'a, T, C> {
    request_metadata: Request<()>,
    connection: &'a mut C,
    response_mapper: fn(shared::Response<T>) -> Vec<u8>,
}

impl<T, C: WorkerStorageConnection> Response<'_, T, C> {
    /// Returns the underlying storage connection.
    pub fn connection(&mut self) -> &mut C {
        &mut *self.connection
    }

    /// Sends the provided response back to the requester.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(self.request_metadata = ?self.request_metadata)
    )]
    pub async fn send(self, data: T) {
        let response = shared::Response {
            id: self.request_metadata.request_id,
            data,
        };
        let response = (self.response_mapper)(response);
        let channel_id = self.request_metadata.response_channel_id;
        if let Err(err) = self.connection.push_message(channel_id, response).await {
            tracing::warn!(%err, "failed sending message");
        }
    }
}

#[derive(Debug)]
pub struct Worker<H, C> {
    id: u64,
    handler: H,
    connection: C,
}

impl<H, C> Worker<H, C>
where
    C: WorkerStoragePool,
    for<'conn> H: HandleRequest<C::Connection<'conn>>,
{
    pub fn new(handler: H, connection: C) -> Self {
        Self {
            id: 0,
            handler,
            connection,
        }
    }

    #[tracing::instrument(
        skip_all,
        err,
        fields(self.name = H::REGISTERED_NAME)
    )]
    pub async fn run(mut self) -> Result<(), C::Error> {
        let mut connection = self.connection.connect().await;
        let worker = connection.get_or_create_worker(H::REGISTERED_NAME).await?;
        connection.release().await;
        tracing::info!(
            self.name = H::REGISTERED_NAME,
            ?worker,
            "obtained worker record"
        );
        self.id = worker.id;

        let mut messages = self
            .connection
            .stream_messages(worker.inbound_channel_id, worker.cursor);
        while let Some((idx, raw_message)) = messages.try_next().await? {
            self.handle_message(idx, raw_message).await?;
        }
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, raw_message),
        err,
        fields(message.len = raw_message.len())
    )]
    async fn handle_message(&mut self, idx: usize, raw_message: Vec<u8>) -> Result<(), C::Error> {
        let message = <H::Codec>::try_decode_bytes(raw_message);
        let message: shared::Request<H::Request> = match message {
            Ok(message) => message,
            Err(err) => {
                tracing::warn!(%err, idx, "cannot deserialize inbound request");
                return Ok(());
            }
        };

        let mut connection = self.connection.connect().await;
        match message {
            shared::Request::New {
                response_channel_id,
                id,
                data,
            } => {
                let request = Request {
                    response_channel_id,
                    request_id: id,
                    data,
                };
                let response = Response {
                    request_metadata: Request {
                        response_channel_id,
                        request_id: id,
                        data: (),
                    },
                    response_mapper: <H::Codec>::encode_value,
                    connection: &mut connection,
                };
                self.handler.handle_request(request, response).await;
            }

            shared::Request::Cancel {
                response_channel_id,
                id,
            } => {
                let cancellation = Request {
                    response_channel_id,
                    request_id: id,
                    data: (),
                };
                self.handler
                    .handle_cancellation(cancellation, &mut connection)
                    .await;
            }
        }
        connection.update_worker_cursor(self.id, idx + 1).await?;
        connection.release().await;
        Ok(())
    }
}
