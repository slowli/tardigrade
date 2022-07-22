//! Request / response communication via channels.

#![allow(clippy::similar_names)] // `*_sx` / `*_rx` is conventional naming

use futures::{
    channel::{
        mpsc,
        oneshot::{self, Canceled},
    },
    future,
    stream::{self, FusedStream},
    FutureExt, Sink, SinkExt, Stream, StreamExt,
};
use serde::{Deserialize, Serialize};

use std::{collections::HashMap, convert::Infallible, future::Future, marker::PhantomData};

/// Container for a value together with a numeric ID.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WithId<T> {
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
    async fn run(
        mut self,
        mut requests_sx: impl Sink<WithId<Req>, Error = Infallible> + Unpin,
        responses_rx: impl Stream<Item = WithId<Resp>> + Unpin,
    ) {
        let mut responses_rx = responses_rx.fuse();
        let mut requests_rx = self.requests_rx.by_ref().left_stream();
        let mut pending_requests =
            HashMap::<u64, oneshot::Sender<Resp>>::with_capacity(self.capacity);
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
                    if let Some(WithId { id, data }) = maybe_resp {
                        if let Some(sx) = pending_requests.remove(&id) {
                            sx.send(data).ok();
                        }
                    } else {
                        // The responses channel has closed. This means that we'll never
                        // receive responses for remaining requests. We signal this by dropping
                        // respective `oneshot::Sender`s.
                        pending_requests.clear();
                        break;
                    }
                }

                maybe_req = requests_rx.next() => {
                    if let Some((req, sx)) = maybe_req {
                        let data_to_send = WithId {
                            id: next_id,
                            data: req,
                        };
                        pending_requests.insert(next_id, sx);
                        next_id += 1;
                        requests_sx.send(data_to_send).await.unwrap();
                    }
                }

                _ = rx_cancellations.fuse() => { /* processing is performed below */ }
            }

            // Free up space for pending requests.
            pending_requests.retain(|_, sx| !sx.is_canceled());

            if self.requests_rx.is_terminated() && pending_requests.is_empty() {
                break;
            }

            // Do not take any more requests once we're at capacity.
            requests_rx = if pending_requests.len() < self.capacity {
                self.requests_rx.by_ref().left_stream()
            } else {
                stream::pending().right_stream()
            };
        }
    }
}

/// Request sender based on a pair of channels. Can be used to call to external task executors.
///
/// # Examples
///
/// `Requests` instance can be built from a pair of inbound / outbound channels:
///
/// ```
/// # use serde::{Deserialize, Serialize};
/// # use tardigrade::{
/// #     channel::{Requests, Sender, Receiver, WithId},
/// #     workflow::{GetInterface, Handle, SpawnWorkflow, TaskHandle, Wasm},
/// #     Json,
/// # };
/// #[derive(Debug, Serialize, Deserialize)]
/// pub struct Request {
///     // request fields...
/// }
/// #[derive(Debug, Serialize, Deserialize)]
/// pub struct Response {
///     // response fields...
/// }
///
/// #[derive(Debug, GetInterface)]
/// # #[tardigrade(interface = r#"{
/// #     "v":0,
/// #     "in": { "responses": {} },
/// #     "out": { "requests": {} }
/// # }"#)]
/// pub struct MyWorkflow(());
///
/// #[tardigrade::handle(for = "MyWorkflow")]
/// #[derive(Debug)]
/// pub struct MyHandle<Env> {
///     pub requests: Handle<Sender<WithId<Request>, Json>, Env>,
///     pub responses: Handle<Receiver<WithId<Response>, Json>, Env>,
/// }
/// # #[tardigrade::init(for = "MyWorkflow", codec = "Json")]
/// # #[derive(Debug, Serialize, Deserialize)]
/// # pub struct Input {}
///
/// impl SpawnWorkflow for MyWorkflow {
///     fn spawn(handle: MyHandle<Wasm>) -> TaskHandle {
///         let requests = Requests::builder(handle.requests, handle.responses)
///             .with_capacity(4)
///             .with_task_name("handling_requests")
///             .build();
///         TaskHandle::new(async move {
///             match requests.request(Request { /* ... */ }).await {
///                 Ok(response) => { /* do something with response */ }
///                 Err(_) => { /* request has been cancelled */ }
///             }
///         })
///     }
/// }
/// ```
#[derive(Debug)]
pub struct Requests<Req, Resp> {
    requests_sx: mpsc::Sender<(Req, oneshot::Sender<Resp>)>,
}

impl<Req: 'static, Resp: 'static> Requests<Req, Resp> {
    /// Creates a new sender based on the specified channels.
    pub fn builder<Sx, Rx>(
        requests_sx: Sx,
        responses_rx: Rx,
    ) -> RequestsBuilder<'static, Req, Sx, Rx>
    where
        Sx: Sink<WithId<Req>, Error = Infallible> + Unpin + 'static,
        Rx: Stream<Item = WithId<Resp>> + Unpin + 'static,
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
pub struct RequestsBuilder<'a, Req, Sx, Rx> {
    requests_sx: Sx,
    responses_rx: Rx,
    capacity: usize,
    task_name: &'a str,
    _req: PhantomData<Req>,
}

impl<'a, Req, Resp, Sx, Rx> RequestsBuilder<'a, Req, Sx, Rx>
where
    Req: 'static,
    Resp: 'static,
    Sx: Sink<WithId<Req>, Error = Infallible> + Unpin + 'static,
    Rx: Stream<Item = WithId<Resp>> + Unpin + 'static,
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

    /// Specifies a task name to use when [spawning a task](crate::spawn) to support
    /// request / response processing. The default task name is `_requests`.
    #[must_use]
    pub fn with_task_name(mut self, task_name: &'a str) -> Self {
        self.task_name = task_name;
        self
    }

    /// Converts this builder into a [`Requests`] instance and launches the background task
    /// to support request / response processing.
    pub fn build(self) -> Requests<Req, Resp> {
        let (inner_sx, inner_rx) = mpsc::channel(self.capacity);
        let handle = RequestsHandle {
            requests_rx: inner_rx,
            capacity: self.capacity,
        };
        crate::spawn(
            self.task_name,
            handle.run(self.requests_sx, self.responses_rx),
        );

        Requests {
            requests_sx: inner_sx,
        }
    }
}
