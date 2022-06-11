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
    workflow::{InputsBuilder, Interface, PutHandle, TakeHandle, ValidateInterface, WithHandle},
    ChannelError, ChannelErrorKind, ChannelKind,
};

#[cfg(target_arch = "wasm32")]
#[path = "imp_wasm32.rs"]
mod imp;
#[cfg(not(target_arch = "wasm32"))]
#[path = "imp_mock.rs"]
pub(crate) mod imp;

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

impl WithHandle<Wasm> for RawReceiver {
    type Handle = Self;
}

impl TakeHandle<Wasm, &'static str> for RawReceiver {
    fn take_handle(env: &mut Wasm, id: &'static str) -> Self::Handle {
        Self::new(imp::MpscReceiver::take_handle(env, id))
    }
}

// TODO: maybe add initial messages?
impl WithHandle<InputsBuilder> for RawReceiver {
    type Handle = ();
}

impl PutHandle<InputsBuilder, &'static str> for RawReceiver {
    fn put_handle(_env: &mut InputsBuilder, _id: &'static str, _handle: Self::Handle) {
        // No handle necessary for initialization
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

impl<T, C> WithHandle<Wasm> for Receiver<T, C>
where
    C: Decoder<T> + Default,
{
    type Handle = Self;
}

impl<T, C> TakeHandle<Wasm, &'static str> for Receiver<T, C>
where
    C: Decoder<T> + Default,
{
    fn take_handle(env: &mut Wasm, id: &'static str) -> Self::Handle {
        Self::new(RawReceiver::take_handle(env, id), C::default())
    }
}

impl<T, C: Decoder<T>> WithHandle<InputsBuilder> for Receiver<T, C> {
    type Handle = ();
}

impl<T, C: Decoder<T>> PutHandle<InputsBuilder, &'static str> for Receiver<T, C> {
    fn put_handle(_env: &mut InputsBuilder, _id: &'static str, _handle: Self::Handle) {
        // No handle necessary for initialization
    }
}

impl<T, C> ValidateInterface<&str> for Receiver<T, C>
where
    C: Encoder<T> + Decoder<T>,
{
    type Error = ChannelError;

    fn validate_interface(interface: &Interface<()>, id: &str) -> Result<(), Self::Error> {
        interface
            .inbound_channel(id)
            .map(drop)
            .ok_or_else(|| ChannelErrorKind::Unknown.for_channel(ChannelKind::Inbound, id))
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

impl WithHandle<Wasm> for RawSender {
    type Handle = Self;
}

impl TakeHandle<Wasm, &'static str> for RawSender {
    fn take_handle(env: &mut Wasm, id: &'static str) -> Self::Handle {
        Self::new(imp::MpscSender::take_handle(env, id))
    }
}

impl WithHandle<InputsBuilder> for RawSender {
    type Handle = ();
}

impl PutHandle<InputsBuilder, &'static str> for RawSender {
    fn put_handle(_env: &mut InputsBuilder, _id: &'static str, _handle: Self::Handle) {
        // No handle necessary for initialization
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

impl<T, C> WithHandle<Wasm> for Sender<T, C>
where
    C: Encoder<T> + Default,
{
    type Handle = Self;
}

impl<T, C> TakeHandle<Wasm, &'static str> for Sender<T, C>
where
    C: Encoder<T> + Default,
{
    fn take_handle(env: &mut Wasm, id: &'static str) -> Self::Handle {
        Self::new(RawSender::take_handle(env, id), C::default())
    }
}

impl<T, C: Encoder<T>> WithHandle<InputsBuilder> for Sender<T, C> {
    type Handle = ();
}

impl<T, C: Encoder<T>> PutHandle<InputsBuilder, &'static str> for Sender<T, C> {
    fn put_handle(_env: &mut InputsBuilder, _id: &'static str, _handle: Self::Handle) {
        // No handle necessary for initialization
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
    type Error = ChannelError;

    fn validate_interface(interface: &Interface<()>, id: &str) -> Result<(), Self::Error> {
        interface
            .outbound_channel(id)
            .map(drop)
            .ok_or_else(|| ChannelErrorKind::Unknown.for_channel(ChannelKind::Outbound, id))
    }
}
