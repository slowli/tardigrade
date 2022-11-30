//! Message channels for workflows.
//!
//! Channels have a similar interface to async [channels][future-chan] from the `futures` crate;
//! their ends implement [`Stream`] / [`Sink`] traits.
//! A workflow owns a single end of a channel (either a [`Receiver`] or
//! [`Sender`]s), while the other end is owned by the host environment or another workflow.
//!
//! A `Sender` or `Receiver` can be obtained from the environment using [`TakeHandle`] trait.
//! This process is usually automated via workflow types. When executed in WASM, channels
//! are backed by imported functions from the Tardigrade runtime. This is emulated for
//! the [test environment](crate::test).
//!
//! [future-chan]: https://docs.rs/futures/latest/futures/channel/index.html

use futures::{Sink, Stream};
use pin_project_lite::pin_project;

use std::{
    fmt,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    codec::{Decode, Encode, Raw},
    interface::AccessError,
    workflow::{TakeHandle, WithHandle, WorkflowEnv},
    ChannelId,
};

mod broadcast;
#[cfg(target_arch = "wasm32")]
#[path = "imp_wasm32.rs"]
pub(crate) mod imp;
#[cfg(not(target_arch = "wasm32"))]
#[path = "imp_mock.rs"]
pub(crate) mod imp;
mod requests;

pub use self::{
    broadcast::{BroadcastError, BroadcastPublisher, BroadcastSubscriber},
    requests::{Request, Requests, RequestsBuilder, Response},
};
pub use crate::error::SendError;

pin_project! {
    /// Receiver for a channel provided to the workflow.
    ///
    /// A receiver is characterized by the type of messages and the codec used to convert messages
    /// from / to bytes.
    pub struct Receiver<T, C> {
        #[pin]
        raw: imp::MpscReceiver,
        _ty: PhantomData<(T, C)>,
    }
}

/// [`Receiver`] of raw byte messages.
pub type RawReceiver = Receiver<Vec<u8>, Raw>;

impl<T, C: fmt::Debug> fmt::Debug for Receiver<T, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Receiver")
            .field("raw", &self.raw)
            .finish()
    }
}

impl<T, C: Decode<T>> Receiver<T, C> {
    pub(crate) fn new(raw: imp::MpscReceiver) -> Self {
        Self {
            raw,
            _ty: PhantomData,
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn from_env(name: &str) -> Result<Self, AccessError> {
        imp::MpscReceiver::from_env(name).map(Self::new)
    }

    pub(crate) fn from_raw(raw: RawReceiver) -> Self {
        Self {
            raw: raw.raw,
            _ty: PhantomData,
        }
    }

    pub(crate) fn channel_id(&self) -> ChannelId {
        self.raw.channel_id()
    }
}

impl RawReceiver {
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn from_resource(resource: externref::Resource<imp::MpscReceiver>) -> Self {
        Self::new(resource.into())
    }
}

impl<T, C: Decode<T>> Stream for Receiver<T, C> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projection = self.project();
        projection
            .raw
            .poll_next(cx)
            .map(|maybe_item| maybe_item.map(|item| C::decode_bytes(item)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw.size_hint()
    }
}

impl<T, C> WithHandle for Receiver<T, C>
where
    C: Encode<T> + Decode<T>,
{
    type Id = str;
    type Handle<Env: WorkflowEnv> = Env::Receiver<T, C>;
}

impl<T, C, Env: WorkflowEnv> TakeHandle<Env> for Receiver<T, C>
where
    C: Encode<T> + Decode<T>,
{
    fn take_handle(env: &mut Env, id: &Self::Id) -> Result<Self::Handle<Env>, AccessError> {
        env.take_receiver(id)
    }
}

pin_project! {
    /// Sender for a workflow channel provided to the workflow.
    ///
    /// A sender is characterized by the type of messages and the codec used to convert messages
    /// from / to bytes.
    ///
    /// Unlike [`Receiver`]s, `Sender` parts of the channel can be cloned. Another difference
    /// is ability to control readiness / flushing when sending messages via channel capacity;
    /// if the channel reaches its capacity of non-flushed messages, it becomes not ready
    /// to accept more messages.
    pub struct Sender<T, C> {
        #[pin]
        raw: imp::MpscSender,
        _ty: PhantomData<(T, C)>,
    }
}

/// [`Sender`] of raw byte messages.
pub type RawSender = Sender<Vec<u8>, Raw>;

impl<T, C> fmt::Debug for Sender<T, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Sender")
            .field("raw", &self.raw)
            .finish()
    }
}

impl<T, C: Encode<T>> Clone for Sender<T, C> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            _ty: PhantomData,
        }
    }
}

impl<T, C: Encode<T>> Sender<T, C> {
    pub(crate) fn new(raw: imp::MpscSender) -> Self {
        Self {
            raw,
            _ty: PhantomData,
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn from_env(name: &str) -> Result<Self, AccessError> {
        imp::MpscSender::from_env(name).map(Self::new)
    }

    pub(crate) fn from_raw(raw: RawSender) -> Self {
        Self {
            raw: raw.raw,
            _ty: PhantomData,
        }
    }

    pub(crate) fn into_raw(self) -> RawSender {
        RawSender {
            raw: self.raw,
            _ty: PhantomData,
        }
    }
}

impl RawSender {
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn from_resource(resource: externref::Resource<imp::MpscSender>) -> Self {
        Self::new(resource.into())
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn as_resource(&self) -> &externref::Resource<imp::MpscSender> {
        self.raw.as_resource()
    }
}

impl<T, C> WithHandle for Sender<T, C>
where
    C: Encode<T> + Decode<T>,
{
    type Id = str;
    type Handle<Env: WorkflowEnv> = Env::Sender<T, C>;
}

impl<T, C, Env: WorkflowEnv> TakeHandle<Env> for Sender<T, C>
where
    C: Encode<T> + Decode<T>,
{
    fn take_handle(env: &mut Env, id: &Self::Id) -> Result<Self::Handle<Env>, AccessError> {
        env.take_sender(id)
    }
}

impl<T, C: Encode<T>> Sink<T> for Sender<T, C> {
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().raw.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let projection = self.project();
        let bytes = C::encode_value(item);
        projection.raw.start_send(&bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().raw.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().raw.poll_close(cx)
    }
}
