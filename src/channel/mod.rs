//! MPSC channels.

use futures::{Sink, SinkExt, Stream};
use pin_project_lite::pin_project;

use std::{
    convert::Infallible,
    fmt,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    codec::{Decoder, Encoder},
    Wasm,
};
use tardigrade_shared::{
    workflow::{Interface, InterfaceErrors, TakeHandle, ValidateInterface},
    ChannelErrorKind, ChannelKind,
};

mod broadcast;
#[cfg(target_arch = "wasm32")]
#[path = "imp_wasm32.rs"]
mod imp;
#[cfg(not(target_arch = "wasm32"))]
#[path = "imp_mock.rs"]
pub(crate) mod imp;

pub use self::broadcast::{BroadcastError, BroadcastPublisher, BroadcastSubscriber};

pin_project! {
    #[derive(Debug)]
    #[repr(transparent)]
    pub struct RawReceiver {
        #[pin]
        inner: imp::MpscReceiver,
    }
}

impl RawReceiver {
    pub(crate) fn new(inner: imp::MpscReceiver) -> Self {
        Self { inner }
    }
}

impl Stream for RawReceiver {
    type Item = Vec<u8>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl TakeHandle<Wasm, &'static str> for RawReceiver {
    type Handle = Self;

    fn take_handle(env: &mut Wasm, id: &'static str) -> Self::Handle {
        Self::new(imp::MpscReceiver::take_handle(env, id))
    }
}

pin_project! {
    pub struct Receiver<T, C> {
        #[pin]
        raw: RawReceiver,
        codec: C,
        _item: PhantomData<T>,
    }
}

impl<T, C: fmt::Debug> fmt::Debug for Receiver<T, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Receiver")
            .field("raw", &self.raw)
            .field("codec", &self.codec)
            .finish()
    }
}

impl<T, C: Decoder<T>> Receiver<T, C> {
    pub fn new(raw: RawReceiver, codec: C) -> Self {
        Self {
            raw,
            codec,
            _item: PhantomData,
        }
    }
}

impl<T, C: Decoder<T>> Stream for Receiver<T, C> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projection = self.project();
        let codec = projection.codec;
        projection
            .raw
            .poll_next(cx)
            .map(|maybe_item| maybe_item.map(|item| codec.decode_bytes(item)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw.size_hint()
    }
}

impl<T, C> TakeHandle<Wasm, &'static str> for Receiver<T, C>
where
    C: Decoder<T> + Default,
{
    type Handle = Self;

    fn take_handle(env: &mut Wasm, id: &'static str) -> Self::Handle {
        Self::new(RawReceiver::take_handle(env, id), C::default())
    }
}

impl<T, C> ValidateInterface<&str> for Receiver<T, C>
where
    C: Encoder<T> + Decoder<T>,
{
    fn validate_interface(errors: &mut InterfaceErrors, interface: &Interface<()>, id: &str) {
        if interface.inbound_channel(id).is_none() {
            let err = ChannelErrorKind::Unknown.for_channel(ChannelKind::Inbound, id);
            errors.insert_error(err);
        }
    }
}

pin_project! {
    #[derive(Debug, Clone)]
    #[repr(transparent)]
    pub struct RawSender {
        #[pin]
        inner: imp::MpscSender,
    }
}

impl RawSender {
    pub(crate) fn new(inner: imp::MpscSender) -> Self {
        Self { inner }
    }
}

impl TakeHandle<Wasm, &'static str> for RawSender {
    type Handle = Self;

    fn take_handle(env: &mut Wasm, id: &'static str) -> Self::Handle {
        Self::new(imp::MpscSender::take_handle(env, id))
    }
}

impl Sink<&[u8]> for RawSender {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: &[u8]) -> Result<(), Self::Error> {
        self.project().inner.start_send(item.as_ref())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx)
    }
}

pin_project! {
    pub struct Sender<T, C> {
        #[pin]
        raw: RawSender,
        codec: C,
        _item: PhantomData<T>,
    }
}

impl<T, C: fmt::Debug> fmt::Debug for Sender<T, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Sender")
            .field("raw", &self.raw)
            .field("codec", &self.codec)
            .finish()
    }
}

impl<T, C: Encoder<T> + Clone> Clone for Sender<T, C> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            codec: self.codec.clone(),
            _item: PhantomData,
        }
    }
}

impl<T, C: Encoder<T>> Sender<T, C> {
    pub fn new(raw: RawSender, codec: C) -> Self {
        Self {
            raw,
            codec,
            _item: PhantomData,
        }
    }

    /// Infallible version of [`SinkExt::send()`] to make common invocation more fluent.
    pub async fn send(&mut self, message: T) {
        <Self as SinkExt<T>>::send(self, message).await.unwrap();
    }
}

impl<T, C> TakeHandle<Wasm, &'static str> for Sender<T, C>
where
    C: Encoder<T> + Default,
{
    type Handle = Self;

    fn take_handle(env: &mut Wasm, id: &'static str) -> Self::Handle {
        Self::new(RawSender::take_handle(env, id), C::default())
    }
}

impl<T, C: Encoder<T>> Sink<T> for Sender<T, C> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().raw.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let projection = self.project();
        let bytes = projection.codec.encode_value(item);
        projection.raw.start_send(&bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().raw.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().raw.poll_close(cx)
    }
}

impl<T, C> ValidateInterface<&str> for Sender<T, C>
where
    C: Encoder<T> + Decoder<T>,
{
    fn validate_interface(errors: &mut InterfaceErrors, interface: &Interface<()>, id: &str) {
        if interface.outbound_channel(id).is_none() {
            let err = ChannelErrorKind::Unknown.for_channel(ChannelKind::Outbound, id);
            errors.insert_error(err);
        }
    }
}
