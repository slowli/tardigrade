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

use std::{collections::HashMap, convert::Infallible, future::Future};

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
#[derive(Debug)]
pub struct Requests<Req, Resp> {
    requests_sx: mpsc::Sender<(Req, oneshot::Sender<Resp>)>,
}

impl<Req: 'static, Resp: 'static> Requests<Req, Resp> {
    /// Creates a new sender based on the specified channels.
    pub fn new<Sx, Rx>(capacity: usize, requests_sx: Sx, responses_rx: Rx) -> Self
    where
        Sx: Sink<WithId<Req>, Error = Infallible> + Unpin + 'static,
        Rx: Stream<Item = WithId<Resp>> + Unpin + 'static,
    {
        let (inner_sx, inner_rx) = mpsc::channel(capacity);
        let handle = RequestsHandle {
            requests_rx: inner_rx,
            capacity,
        };
        crate::spawn("_requests", handle.run(requests_sx, responses_rx));

        Self {
            requests_sx: inner_sx,
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
