//! Workers for external tasks in Tardigrade runtime.
//!
//! A worker represents a piece of functionality external to Tardigrade workflows.
//! A workflow can connect to the worker by specifying
//! the [worker name](WorkerInterface::REGISTERED_NAME) in the relevant [sender specification].
//! A single worker can serve multiple workflows; i.e., it can be thought of as a server
//! in the client-server model, with workflows acting as clients. So far, workers
//! only implement only the request-response communication pattern. Similar to modern RPC
//! protocols (e.g., gRPC), communication is non-blocking; multiple requests may be simultaneously
//! in flight. Concurrency restrictions, if desired, can be enforced on the workflow side
//! (see `channels::Requests`) and/or on the handler level. As with other Tardigrade components,
//! passing messages via channels leads to lax worker availability requirements; a worker
//! does not need to be highly available or work without failures.
//!
//! Workers poll new requests using a connection to the Tardigrade runtime, either
//! an in-process one, or a remote one via the gRPC service wrapper. This variability
//! is encapsulated in [pool](WorkerStoragePool) and [connection](WorkerStorageConnection) traits.
//! In-process connections enable "exactly once" semantics for request handling; otherwise,
//! handling has "at least once" semantics (a request may be handled repeatedly if the worker
//! crashed during handling).
//!
//! [sender specification]: tardigrade_shared::interface::SenderSpec

// Documentation settings.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(html_root_url = "https://docs.rs/tardigrade-worker/0.1.0")]
// Linter settings.
#![warn(missing_debug_implementations, missing_docs, bare_trait_objects)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::must_use_candidate,
    clippy::module_name_repetitions,
    clippy::trait_duplication_in_bounds
)]

use async_trait::async_trait;
use futures::{Future, TryStreamExt};

use std::marker::PhantomData;

mod connection;

pub use crate::connection::{
    MessageStream, WorkerRecord, WorkerStorageConnection, WorkerStoragePool,
};

use tardigrade_shared::{self as shared, ChannelId, Codec, Json};

/// Interface of a worker that can handle requests of a certain type.
pub trait WorkerInterface: Send {
    /// Request input into this worker.
    type Request;
    /// Response output from this worker.
    type Response;
    /// Codec for requests / responses.
    type Codec: Codec<shared::Request<Self::Request>> + Codec<shared::Response<Self::Response>>;
}

/// Handler of requests of a certain type.
#[async_trait]
pub trait HandleRequest<P: WorkerStoragePool>: WorkerInterface {
    /// Initializes this handler.
    #[allow(unused)]
    async fn initialize(&mut self, connection_pool: &P) {
        // Do nothing
    }

    /// Handles a request.
    ///
    /// # Return value
    ///
    /// Returns `None` if the request will be handled asynchronously (e.g., in a separate
    /// task spawned using `task::spawn()` from `tokio` or `async-std`).
    async fn handle_request(&mut self, request: Request<Self>, connection: &mut P::Connection<'_>);

    /// Handles request cancellation.
    #[allow(unused)]
    async fn handle_cancellation(&mut self, connection: &mut P::Connection<'_>) {
        // Do nothing
    }
}

/// Request payload together with additional metadata allowing to identify the request.
#[derive(Debug)]
pub struct Request<T>
where
    T: WorkerInterface + ?Sized,
{
    response_channel_id: ChannelId,
    request_id: u64,
    data: T::Request,
}

impl<T> Request<T>
where
    T: WorkerInterface + ?Sized,
{
    /// Returns the ID of the channel that the response to this request should be sent to.
    pub fn response_channel_id(&self) -> ChannelId {
        self.response_channel_id
    }

    /// Returns the ID of this request. The ID should be unique for the same [response channel].
    ///
    /// [response channel]: Self::response_channel_id()
    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    /// Returns a shared reference to the request data.
    pub fn get_ref(&self) -> &T::Request {
        &self.data
    }

    /// Converts this request into data.
    pub fn into_parts(self) -> (T::Request, ResponseSender<T>) {
        let sender = ResponseSender {
            response_channel_id: self.response_channel_id,
            request_id: self.request_id,
            _interface: PhantomData,
        };
        (self.data, sender)
    }
}

/// Sender of responses.
#[derive(Debug)]
pub struct ResponseSender<T: ?Sized> {
    response_channel_id: ChannelId,
    request_id: u64,
    _interface: PhantomData<T>,
}

impl<T: WorkerInterface + ?Sized> ResponseSender<T> {
    /// Sends a response.
    ///
    /// # Errors
    ///
    /// Surfaces connection errors.
    pub async fn send<Conn: WorkerStorageConnection>(
        self,
        response: T::Response,
        connection: &mut Conn,
    ) -> Result<(), Conn::Error> {
        let response = <T::Codec>::encode_value(shared::Response {
            id: self.request_id,
            data: response,
        });
        connection
            .push_message(self.response_channel_id, response)
            .await
    }
}

/// [Worker handler](HandleRequest) based on a function.
#[derive(Debug)]
pub struct FnHandler<F, Req, C = Json> {
    function: F,
    _signature: PhantomData<fn(Req)>,
    _codec: PhantomData<C>,
}

impl<F, Req, Fut> FnHandler<F, Req>
where
    F: FnMut(Req) -> Fut + Send,
    Req: Send,
    Fut: Future + Send,
{
    /// Creates a new functional handler that uses the [`Json`] codec for requests / responses.
    pub fn json(function: F) -> Self {
        Self {
            function,
            _signature: PhantomData,
            _codec: PhantomData,
        }
    }
}

impl<F, Req, Fut, C> WorkerInterface for FnHandler<F, Req, C>
where
    F: FnMut(Req) -> Fut + Send,
    Req: Send,
    Fut: Future + Send,
    C: Codec<shared::Request<Req>> + Codec<shared::Response<Fut::Output>>,
{
    type Request = Req;
    type Response = Fut::Output;
    type Codec = C;
}

#[async_trait]
impl<F, Req, Fut, C, P> HandleRequest<P> for FnHandler<F, Req, C>
where
    F: FnMut(Req) -> Fut + Send,
    Req: Send,
    Fut: Future + Send,
    Fut::Output: Send,
    C: Codec<shared::Request<Req>> + Codec<shared::Response<Fut::Output>>,
    P: WorkerStoragePool + 'static,
{
    async fn handle_request(&mut self, request: Request<Self>, connection: &mut P::Connection<'_>) {
        let (data, sender) = request.into_parts();
        let response = (self.function)(data).await;
        sender.send(response, connection).await.ok();
    }
}

/// Worker encapsulating a handler and a connection to the Tardigrade runtime.
#[derive(Debug)]
pub struct Worker<H, C> {
    id: u64,
    handler: H,
    connection_pool: C,
}

impl<H, P> Worker<H, P>
where
    P: WorkerStoragePool,
    H: HandleRequest<P>,
{
    /// Creates a new worker.
    pub fn new(handler: H, connection: P) -> Self {
        Self {
            id: 0,
            handler,
            connection_pool: connection,
        }
    }

    /// Drives this worker to completion.
    ///
    /// # Errors
    ///
    /// Returns an error when further processing of requests is impossible. This is caused
    /// by the encapsulated [connection](WorkerStorageConnection) failures (e.g., connectivity
    /// issues).
    #[tracing::instrument(skip(self), err)]
    pub async fn listen(mut self, name: &str) -> Result<(), P::Error> {
        let mut connection = self.connection_pool.connect().await;
        let worker = connection.get_or_create_worker(name).await?;
        connection.release().await;
        tracing::info!(self.name = name, ?worker, "obtained worker record");
        self.id = worker.id;
        self.handler.initialize(&self.connection_pool).await;

        let mut messages = self
            .connection_pool
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
    async fn handle_message(&mut self, idx: usize, raw_message: Vec<u8>) -> Result<(), P::Error> {
        let message = <H::Codec>::try_decode_bytes(raw_message);
        let message: shared::Request<H::Request> = match message {
            Ok(message) => message,
            Err(err) => {
                tracing::warn!(%err, idx, "cannot deserialize inbound request");
                return Ok(());
            }
        };

        let mut connection = self.connection_pool.connect().await;
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
                self.handler.handle_request(request, &mut connection).await;
            }

            shared::Request::Cancel { .. } => {
                self.handler.handle_cancellation(&mut connection).await;
            }
        }
        connection.update_worker_cursor(self.id, idx + 1).await?;
        connection.release().await;
        Ok(())
    }
}
