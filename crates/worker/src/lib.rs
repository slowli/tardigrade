//! Workers for external tasks in Tardigrade runtime.
//!
//! A worker represents a piece of functionality external to Tardigrade workflows.
//! A workflow can connect to the worker by specifying
//! the worker name in the relevant [sender specification].
//! A single worker can serve multiple workflows; i.e., it can be thought of as a server
//! in the client-server model, with workflows acting as clients. So far, workers
//! only implement the request-response communication pattern. Similar to modern RPC
//! protocols (e.g., gRPC), communication is non-blocking; multiple requests may be simultaneously
//! in flight. Concurrency restrictions, if desired, can be enforced on the workflow side
//! (see `channels::Requests`) and/or on the handler level. As with other Tardigrade components,
//! passing messages via channels leads to lax worker availability requirements; a worker
//! does not need to be highly available or work without failures.
//!
//! # Implementation details
//!
//! Internally, a [`Worker`] is implemented storing a [`WorkerRecord`] in the same database
//! as other runtime-related entities (e.g., workflows). Worker records maintain the mapping
//! between the (unique) worker name and the inbound channel for requests sent to the worker,
//! which is used by the runtime to resolve channels in the workflow interface specification.
//! Additionally, a worker record contains the cursor position for the inbound channel,
//! similar to message consumers in Kafka.
//!
//! Workers poll new requests using a connection to the Tardigrade runtime, either
//! an in-process one, or a remote one via the gRPC service wrapper. This variability
//! is encapsulated in [pool](WorkerStoragePool) and [connection](WorkerStorageConnection) traits.
//! In-process connections enable "exactly once" semantics for request handling; otherwise,
//! handling has "at least once" semantics (a request may be handled repeatedly if the worker
//! crashes during handling).
//!
//! # Examples
//!
//! ## Sequential handler
//!
//! In this simplest implementation, the worker processes requests sequentially. This allows
//! "exactly once" processing, but can be restricting in terms of throughput.
//!
//! ```
//! # use async_trait::async_trait;
//! # use tardigrade_shared::Json;
//! # use tardigrade_worker::{HandleRequest, Request, WorkerInterface, WorkerStoragePool};
//! struct SequentialHandler {
//!     // fields snipped
//! }
//!
//! impl WorkerInterface for SequentialHandler {
//!     type Request = String;
//!     type Response = String;
//!     type Codec = Json;
//! }
//!
//! #[async_trait]
//! impl<P: WorkerStoragePool> HandleRequest<P> for SequentialHandler {
//!     async fn handle_request(
//!         &mut self,
//!         request: Request<Self>,
//!         connection: &mut P::Connection<'_>,
//!     ) {
//!         let (request, response_sx) = request.into_parts();
//!         let response = request.repeat(2);
//!         response_sx.send(response, connection).await.ok();
//!     }
//! }
//! ```
//!
//! ## Concurrent handler
//!
//! In this implementation, several requests can be handled simultaneously. As a downside,
//! sending response is not atomic with updating the worker cursor position (i.e.,
//! only "at least once" request processing is guaranteed).
//!
//! ```
//! # use async_trait::async_trait;
//! # use tardigrade_shared::Json;
//! # use tardigrade_worker::{
//! #     HandleRequest, Request, WorkerInterface, WorkerStorageConnection, WorkerStoragePool,
//! # };
//! struct ConcurrentHandler<P> {
//!     connection_pool: Option<P>,
//!     // other fields snipped
//! }
//!
//! impl<P: WorkerStoragePool + Clone> WorkerInterface for ConcurrentHandler<P> {
//!     type Request = String;
//!     type Response = String;
//!     type Codec = Json;
//! }
//!
//! #[async_trait]
//! impl<P: WorkerStoragePool + Clone> HandleRequest<P> for ConcurrentHandler<P> {
//!     async fn initialize(&mut self, connection_pool: &P) {
//!         self.connection_pool = Some(connection_pool.clone());
//!     }
//!
//!     async fn handle_request(
//!         &mut self,
//!         request: Request<Self>,
//!         _connection: &mut P::Connection<'_>,
//!     ) {
//!         let (request, response_sx) = request.into_parts();
//!         let connection_pool = self.connection_pool.clone().unwrap();
//!         tokio::task::spawn(async move {
//!             let response = request.repeat(2);
//!             let mut connection = connection_pool.connect().await;
//!             response_sx.send(response, &mut connection).await.ok();
//!             connection.release().await;
//!         });
//!     }
//! }
//! ```
//!
//! [sender specification]: tardigrade_shared::interface::SenderSpec

// Documentation settings.
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
use futures::{
    channel::oneshot::{self, Canceled},
    Future, TryStreamExt,
};

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
    /// Initializes this handler. This method will be called and awaited before any other methods
    /// are called for the handler instance.
    #[allow(unused)]
    async fn initialize(&mut self, connection_pool: &P) {
        // Do nothing
    }

    /// Handles a request.
    ///
    /// A request can be handled either synchronously (by calling [`Request::into_parts()`]
    /// and then the relevant [`ResponseSender`] method with `connection`), or asynchronously
    /// (by moving the [`ResponseSender`] into a task and creating a dedicated connection for
    /// sending the response).
    async fn handle_request(&mut self, request: Request<Self>, connection: &mut P::Connection<'_>);

    /// Handles request cancellation.
    #[allow(unused)]
    async fn handle_cancellation(
        &mut self,
        cancellation: Cancellation,
        connection: &mut P::Connection<'_>,
    ) {
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

    /// Converts this request into data and the sender for the response.
    pub fn into_parts(self) -> (T::Request, ResponseSender<T>) {
        let sender = ResponseSender {
            response_channel_id: self.response_channel_id,
            request_id: self.request_id,
            _interface: PhantomData,
        };
        (self.data, sender)
    }
}

/// Information about a request cancellation.
#[derive(Debug)]
pub struct Cancellation {
    response_channel_id: ChannelId,
    request_id: u64,
}

impl Cancellation {
    /// Returns the ID of the channel that the response to this request should be sent to.
    pub fn response_channel_id(&self) -> ChannelId {
        self.response_channel_id
    }

    /// Returns the ID of this request.
    ///
    /// - The ID should be unique for the same [response channel].
    /// - The request with the provided ID should be previously handled
    ///   by [`HandleRequest::handle_request()`].
    ///
    /// These conditions are up to the (potentially untrusted) workflow infrastructure code
    /// to enforce; i.e., they may be violated.
    ///
    /// [response channel]: Self::response_channel_id()
    pub fn request_id(&self) -> u64 {
        self.request_id
    }
}

/// Sender of responses for a [handler](HandleRequest).
///
/// A single sender can be used to send a single request. It can be retrieved using
/// [`Request::into_parts()`] and, if necessary, moved to an async task.
/// In this case, sending a response cannot be executed in the same transaction
/// as the worker state update, and the caller is responsible for procuring a [connection]
/// to the worker storage, e.g., by stashing a copy of the [connection pool] in
/// [`HandleRequest::initialize()`].
///
/// [connection]: WorkerStorageConnection
/// [connection pool]: WorkerStoragePool
#[must_use = "Response to the request should be sent using this sender"]
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

    /// Sends a response and then closes the response channel from the sender side.
    /// Returns whether
    ///
    /// If the workflow code is correctly implemented, closing the response channel
    /// leads to to all outstanding requests being dropped, and no new requests should come through.
    /// However, this is not currently checked by the framework code.
    ///
    /// # Errors
    ///
    /// Surfaces connection errors.
    pub async fn send_and_close<Conn: WorkerStorageConnection>(
        self,
        response: T::Response,
        connection: &mut Conn,
    ) -> Result<bool, Conn::Error> {
        let response_channel_id = self.response_channel_id;
        self.send(response, connection).await?;
        connection.close_response_channel(response_channel_id).await
    }
}

/// [Worker handler](HandleRequest) based on a function. Mostly useful for quick prototyping
/// and testing.
///
/// # Examples
///
/// ```
/// # use tardigrade_worker::{FnHandler, WorkerInterface};
/// fn uses_handler(handler: impl WorkerInterface) {
///     // snipped
/// }
///
/// let handler = FnHandler::json(|req: String| async move {
///     req.repeat(2)
/// });
/// uses_handler(handler);
/// ```
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
    P: WorkerStoragePool,
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
    ready_sx: Option<oneshot::Sender<()>>,
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
            ready_sx: None,
            handler,
            connection_pool: connection,
        }
    }

    /// Returns a future allowing to monitor the readiness state of this worker. The future
    /// will be resolved once the worker handler is [initialized](HandleRequest::initialize()).
    ///
    /// # Errors
    ///
    /// Returns a [`Canceled`] error in any of the following cases:
    ///
    /// - An error occurs before the handler initialization
    /// - If `wait_until_ready()` is called again
    #[allow(clippy::similar_names)]
    pub fn wait_until_ready(&mut self) -> impl Future<Output = Result<(), Canceled>> {
        let (ready_sx, ready_rx) = oneshot::channel();
        self.ready_sx = Some(ready_sx);
        ready_rx
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

        if let Some(ready_sx) = self.ready_sx.take() {
            ready_sx.send(()).ok();
        }

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

            shared::Request::Cancel {
                response_channel_id,
                id,
            } => {
                let cancellation = Cancellation {
                    response_channel_id,
                    request_id: id,
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
