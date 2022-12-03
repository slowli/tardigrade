//! Request / response communication via channels.

#![allow(clippy::similar_names)] // `*_sx` / `*_rx` is conventional naming

use futures::{
    channel::{
        mpsc,
        oneshot::{self, Canceled},
    },
    future,
    stream::{self, FusedStream},
    FutureExt, SinkExt, StreamExt,
};
use serde::{Deserialize, Serialize};

use std::{collections::HashMap, future::Future, marker::PhantomData};

use crate::{
    channel::{Receiver, Sender},
    task::{self, JoinHandle},
    workflow::{HandleFormat, InEnv, Wasm, WithHandle},
    ChannelId, Codec,
};

/// Container for a request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Request<T> {
    /// New request.
    New {
        /// Identifier associated with the value.
        #[serde(rename = "@id")]
        id: u64,
        /// Payload of the request.
        data: T,
        /// ID of the response channel.
        #[serde(rename = "rx")]
        response_channel_id: ChannelId,
    },
    /// Request cancellation.
    Cancel {
        /// Identifier associated with the value.
        #[serde(rename = "@id")]
        id: u64,
    },
}

/// Container for a response to the request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Response<T> {
    /// Identifier associated with the value.
    #[serde(rename = "@id")]
    pub id: u64,
    /// Wrapped value.
    pub data: T,
}

#[derive(Debug)]
struct RequestsHandle<Req, Resp> {
    requests_rx: mpsc::Receiver<(Req, oneshot::Sender<Resp>)>,
    capacity: usize,
}

impl<Req, Resp> RequestsHandle<Req, Resp> {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "run_requests", skip_all, fields(self.capacity = self.capacity))
    )]
    async fn run<C>(
        mut self,
        mut requests_sx: Sender<Request<Req>, C>,
        responses_rx: Receiver<Response<Resp>, C>,
    ) where
        C: Codec<Request<Req>> + Codec<Response<Resp>>,
    {
        const MAX_ALLOC_CAPACITY: usize = 16;

        let response_channel_id = responses_rx.channel_id();
        let mut responses_rx = responses_rx.fuse();
        let mut requests_rx = self.requests_rx.by_ref().left_stream();
        let allocated_capacity = self.capacity.min(MAX_ALLOC_CAPACITY);
        let mut pending_requests =
            HashMap::<u64, oneshot::Sender<Resp>>::with_capacity(allocated_capacity);
        let mut next_id = 0_u64;

        loop {
            let rx_cancellations = if pending_requests.is_empty() {
                // Need special processing since `future::select_all` panics if supplied
                // with an empty iterator.
                future::pending().left_future()
            } else {
                let cancellations = pending_requests.iter_mut().map(|(_, rx)| rx.cancellation());
                future::select_all(cancellations).fuse().right_future()
            };

            futures::select! {
                maybe_resp = responses_rx.next() => {
                    if let Some(Response { id, data }) = maybe_resp {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(id, "received response");

                        if let Some(sx) = pending_requests.remove(&id) {
                            sx.send(data).ok();
                        }
                    } else {
                        #[cfg(feature = "tracing")]
                        tracing::debug!("response channel closed");

                        // The responses channel has closed. This means that we'll never
                        // receive responses for remaining requests. We signal this by dropping
                        // respective `oneshot::Sender`s.
                        pending_requests.clear();
                        break;
                    }
                }

                maybe_req = requests_rx.next() => {
                    if let Some((req, sx)) = maybe_req {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(id = next_id, "received new request");

                        let data_to_send = Request::New {
                            id: next_id,
                            data: req,
                            response_channel_id,
                        };
                        pending_requests.insert(next_id, sx);
                        next_id += 1;
                        if requests_sx.send(data_to_send).await.is_err() {
                            #[cfg(feature = "tracing")]
                            tracing::debug!("requests channel closed");

                            // The requests channel has been closed by the other side.
                            // We cannot process new requests, but the outstanding ones
                            // can still be completed.
                            self.capacity = 0;
                            break;
                        }
                    }
                }

                _ = rx_cancellations.fuse() => { /* processing is performed below */ }
            }

            // Free up space for pending requests.
            let mut canceled_ids = vec![];
            pending_requests.retain(|&id, sx| {
                let is_canceled = sx.is_canceled();
                if is_canceled {
                    canceled_ids.push(id);
                }
                !is_canceled
            });

            #[cfg(feature = "tracing")]
            tracing::debug!(
                requests = pending_requests.len(),
                "removed cancelled pending requests"
            );
            let canceled_ids = canceled_ids
                .into_iter()
                .map(|id| Ok(Request::Cancel { id }));
            requests_sx
                .send_all(&mut stream::iter(canceled_ids))
                .await
                .ok();

            if self.requests_rx.is_terminated() && pending_requests.is_empty() {
                break;
            }

            // Do not take any more requests once we're at capacity.
            requests_rx = if pending_requests.len() < self.capacity {
                self.requests_rx.by_ref().left_stream()
            } else {
                #[cfg(feature = "tracing")]
                tracing::debug!("requests are at capacity; not accepting new requests");

                stream::pending().right_stream()
            };
        }
    }
}

/// Request sender based on a pair of channels. Can be used to call to external task executors.
///
/// # Examples
///
/// `Requests` instance can be built from a pair of sender / receiver channel halves:
///
/// ```
/// # use async_trait::async_trait;
/// # use serde::{Deserialize, Serialize};
/// #
/// # use tardigrade::{
/// #     channel::{Request, Requests, Response, Sender, Receiver},
/// #     task::{TaskResult, ErrorContextExt},
/// #     workflow::{
/// #         GetInterface, InEnv, SpawnWorkflow, TaskHandle, TakeHandle, Wasm, WorkflowEnv,
/// #         WorkflowFn,
/// #     },
/// #     Json,
/// # };
/// #[derive(Debug, Serialize, Deserialize)]
/// pub struct MyRequest {
///     // request fields...
/// }
/// #[derive(Debug, Serialize, Deserialize)]
/// pub struct MyResponse {
///     // response fields...
/// }
///
/// #[derive(Debug, GetInterface, TakeHandle)]
/// # #[tardigrade(handle = "MyHandle", auto_interface)]
/// pub struct MyWorkflow(());
///
/// #[derive(TakeHandle)]
/// #[tardigrade(derive(Debug))]
/// pub struct MyHandle<Env: WorkflowEnv> {
///     pub requests: InEnv<Sender<Request<MyRequest>, Json>, Env>,
///     pub responses: InEnv<Receiver<Response<MyResponse>, Json>, Env>,
/// }
/// # impl WorkflowFn for MyWorkflow {
/// #     type Args = ();
/// #     type Codec = Json;
/// # }
///
/// #[async_trait(?Send)]
/// impl SpawnWorkflow for MyWorkflow {
///     async fn spawn(_args: (), handle: MyHandle<Wasm>) -> TaskResult {
///         let (requests, _) = Requests::builder(handle.requests, handle.responses)
///             .with_capacity(4)
///             .with_task_name("handling_requests")
///             .build();
///         let response = requests
///             .request(MyRequest { /* ... */ })
///             .await
///             .context("request cancelled")?;
///         // Do something with the response...
/// #       Ok(())
///     }
/// }
/// ```
///
/// See [`RequestHandles`] docs for a higher-level alternative way to build `Requests`.
#[derive(Debug)]
pub struct Requests<Req, Resp> {
    requests_sx: mpsc::Sender<(Req, oneshot::Sender<Resp>)>,
}

impl<Req: 'static, Resp: 'static> Requests<Req, Resp> {
    /// Creates a new sender based on the specified channels.
    pub fn builder<C>(
        requests_sx: Sender<Request<Req>, C>,
        responses_rx: Receiver<Response<Resp>, C>,
    ) -> RequestsBuilder<'static, Req, Resp, C>
    where
        C: Codec<Request<Req>> + Codec<Response<Resp>>,
    {
        RequestsBuilder {
            requests_sx,
            responses_rx,
            capacity: 1,
            task_name: "_requests",
            _req: PhantomData,
        }
    }

    /// Performs a request and returns a future that resolves to the response.
    ///
    /// # Errors
    ///
    /// The returned future resolves to an error if the responses channel is closed before
    /// a response arrives.
    pub fn request(&self, request: Req) -> impl Future<Output = Result<Resp, Canceled>> {
        let (response_sx, response_rx) = oneshot::channel::<Resp>();
        let mut requests_sx = self.requests_sx.clone();
        async move {
            requests_sx
                .send((request, response_sx))
                .await
                .map_err(|_| Canceled)?;
            response_rx.await
        }
    }
}

/// Builder for [`Requests`].
#[derive(Debug)]
pub struct RequestsBuilder<'a, Req, Resp, C> {
    requests_sx: Sender<Request<Req>, C>,
    responses_rx: Receiver<Response<Resp>, C>,
    capacity: usize,
    task_name: &'a str,
    _req: PhantomData<Req>,
}

impl<'a, Req, Resp, C> RequestsBuilder<'a, Req, Resp, C>
where
    Req: 'static,
    Resp: 'static,
    C: Codec<Request<Req>> + Codec<Response<Resp>>,
{
    /// Specifies the capacity (max number of concurrently processed requests).
    /// The default requests capacity is 1.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0.
    #[must_use]
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0);
        self.capacity = capacity;
        self
    }

    /// Specifies a task name to use when [spawning a task](crate::task::spawn()) to support
    /// request / response processing. The default task name is `_requests`.
    #[must_use]
    pub fn with_task_name(mut self, task_name: &'a str) -> Self {
        self.task_name = task_name;
        self
    }

    /// Converts this builder into a [`Requests`] instance. A handle for the created background
    /// task is returned as well; it can be used to guarantee expected requests termination.
    /// Note that to avoid a deadlock, it usually makes sense to drop the `Requests` instance
    /// before `await`ing the task handle.
    pub fn build(self) -> (Requests<Req, Resp>, JoinHandle) {
        let (inner_sx, inner_rx) = mpsc::channel(self.capacity);
        let handle = RequestsHandle {
            requests_rx: inner_rx,
            capacity: self.capacity,
        };
        let task = task::spawn(
            self.task_name,
            handle.run(self.requests_sx, self.responses_rx),
        );

        let requests = Requests {
            requests_sx: inner_sx,
        };
        (requests, task)
    }
}

/// A pair consisting of a sender and a receiver that can be used to handle requests.
///
/// # Examples
///
/// `RequestHandles` can be used as a higher-level alternative to manually creating [`Requests`].
///
/// ```
/// # use serde::{Deserialize, Serialize};
/// # use std::error::Error;
/// # use tardigrade::{
/// #     channel::{SendError, RequestHandles}, workflow::{TakeHandle, Wasm, WorkflowEnv}, Json,
/// # };
/// #[derive(Serialize, Deserialize)]
/// pub struct MyRequest {
///     // request fields...
/// }
///
/// #[derive(TakeHandle)]
/// #[tardigrade(derive(Debug))]
/// pub struct MyHandle<Env: WorkflowEnv = Wasm> {
///     pub task: RequestHandles<MyRequest, (), Json, Env>,
///     // other handles...
/// }
///
/// // Usage in workflow code:
/// # async fn workflow_code(handle: MyHandle) -> Result<(), Box<dyn Error>> {
/// let handle: MyHandle = // ...
/// # handle;
/// let (requests, _) =
///     handle.task.process_requests().with_capacity(4).build();
/// requests.request(MyRequest { /* .. */ }).await?;
/// # Ok(())
/// # }
/// ```
#[derive(WithHandle)]
#[tardigrade(crate = "crate", derive(Debug, Clone))]
pub struct RequestHandles<Req, Resp, C, Fmt: HandleFormat = Wasm>
where
    C: Codec<Request<Req>> + Codec<Response<Resp>>,
{
    /// Requests sender.
    pub requests: InEnv<Sender<Request<Req>, C>, Fmt>,
    /// Responses receiver.
    pub responses: InEnv<Receiver<Response<Resp>, C>, Fmt>,
}

impl<Req: 'static, Resp: 'static, C> RequestHandles<Req, Resp, C>
where
    C: Codec<Request<Req>> + Codec<Response<Resp>>,
{
    /// Starts building a [`Requests`] processor based on the contained sender-receiver pair.
    pub fn process_requests(self) -> RequestsBuilder<'static, Req, Resp, C> {
        Requests::builder(self.requests, self.responses)
    }
}
