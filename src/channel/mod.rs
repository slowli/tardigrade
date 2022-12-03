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
    convert::Infallible,
    fmt,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
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
    requests::{Request, RequestHandles, Requests, RequestsBuilder, Response},
};
pub use crate::error::SendError;

use crate::{
    codec::{Codec, Raw},
    interface::{AccessError, AccessErrorKind, Handle, HandlePath, ReceiverAt, SenderAt},
    workflow::{BuildHandles, HandleFormat, IntoRaw, TakeHandles, TryFromRaw, WithHandle},
    ChannelId,
};

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

impl<T, C: Codec<T>> TryFromRaw<RawReceiver> for Receiver<T, C> {
    type Error = Infallible;

    fn try_from_raw(raw: RawReceiver) -> Result<Self, Self::Error> {
        Ok(Self::from_raw(raw))
    }
}

impl<T, C: Codec<T>> IntoRaw<RawReceiver> for Receiver<T, C> {
    fn into_raw(self) -> RawReceiver {
        RawReceiver {
            raw: self.raw,
            _ty: PhantomData,
        }
    }
}

impl<T, C: fmt::Debug> fmt::Debug for Receiver<T, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Receiver")
            .field("raw", &self.raw)
            .finish()
    }
}

impl<T, C: Codec<T>> Receiver<T, C> {
    pub(crate) fn new(raw: imp::MpscReceiver) -> Self {
        Self {
            raw,
            _ty: PhantomData,
        }
    }

    /// Returns a receiver for a closed channel. This can be provided to spawned workflows to avoid
    /// allocating new channels.
    pub fn closed() -> Self {
        Self::new(imp::MpscReceiver::closed())
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn from_env(path: HandlePath<'_>) -> Result<Self, AccessError> {
        imp::MpscReceiver::from_env(path).map(Self::new)
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

impl<T, C: Codec<T>> Stream for Receiver<T, C> {
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

impl<T, C: Codec<T>> WithHandle for Receiver<T, C> {
    type Handle<Fmt: HandleFormat> = Fmt::Receiver<T, C>;

    fn take_from_untyped<Fmt: HandleFormat>(
        untyped: &mut dyn TakeHandles<Fmt>,
        path: HandlePath<'_>,
    ) -> Result<Self::Handle<Fmt>, AccessError> {
        let raw_receiver = untyped.take_receiver(path)?;
        <Fmt::Receiver<T, C>>::try_from_raw(raw_receiver)
            .map_err(|err| AccessErrorKind::custom(err.to_string()).with_location(ReceiverAt(path)))
    }

    fn insert_into_untyped<Fmt: HandleFormat>(
        handle: Self::Handle<Fmt>,
        untyped: &mut dyn BuildHandles<Fmt>,
        path: HandlePath<'_>,
    ) {
        let raw_receiver = handle.into_raw();
        untyped.insert_handle(path.to_owned(), Handle::Receiver(raw_receiver));
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

impl<T, C: Codec<T>> TryFromRaw<RawSender> for Sender<T, C> {
    type Error = Infallible;

    fn try_from_raw(raw: RawSender) -> Result<Self, Self::Error> {
        Ok(Self::from_raw(raw))
    }
}

impl<T, C: Codec<T>> IntoRaw<RawSender> for Sender<T, C> {
    fn into_raw(self) -> RawSender {
        RawSender {
            raw: self.raw,
            _ty: PhantomData,
        }
    }
}

impl<T, C> fmt::Debug for Sender<T, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Sender")
            .field("raw", &self.raw)
            .finish()
    }
}

impl<T, C: Codec<T>> Clone for Sender<T, C> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            _ty: PhantomData,
        }
    }
}

impl<T, C: Codec<T>> Sender<T, C> {
    pub(crate) fn new(raw: imp::MpscSender) -> Self {
        Self {
            raw,
            _ty: PhantomData,
        }
    }

    /// Returns a sender for a closed channel. This can be provided to spawned workflows to avoid
    /// allocating new channels.
    pub fn closed() -> Self {
        Self::new(imp::MpscSender::closed())
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn from_env(path: HandlePath<'_>) -> Result<Self, AccessError> {
        imp::MpscSender::from_env(path).map(Self::new)
    }

    pub(crate) fn from_raw(raw: RawSender) -> Self {
        Self {
            raw: raw.raw,
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

impl<T, C: Codec<T>> WithHandle for Sender<T, C> {
    type Handle<Fmt: HandleFormat> = Fmt::Sender<T, C>;

    fn take_from_untyped<Fmt: HandleFormat>(
        untyped: &mut dyn TakeHandles<Fmt>,
        path: HandlePath<'_>,
    ) -> Result<Self::Handle<Fmt>, AccessError> {
        let raw_sender = untyped.take_sender(path)?;
        <Fmt::Sender<T, C>>::try_from_raw(raw_sender)
            .map_err(|err| AccessErrorKind::custom(err.to_string()).with_location(SenderAt(path)))
    }

    fn insert_into_untyped<Fmt: HandleFormat>(
        handle: Self::Handle<Fmt>,
        untyped: &mut dyn BuildHandles<Fmt>,
        path: HandlePath<'_>,
    ) {
        let raw_sender = handle.into_raw();
        untyped.insert_handle(path.to_owned(), Handle::Sender(raw_sender));
    }
}

impl<T, C: Codec<T>> Sink<T> for Sender<T, C> {
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

/// Creates a new channel and returns its sender and receiver halves.
#[allow(clippy::unused_async)]
pub async fn channel<T, C: Codec<T>>() -> (Sender<T, C>, Receiver<T, C>) {
    let (raw_sender, raw_receiver) = imp::raw_channel();
    (Sender::new(raw_sender), Receiver::new(raw_receiver))
}
