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
    codec::{Decoder, Encoder, Raw},
    Wasm,
};
use tardigrade_shared::{
    workflow::{InterfaceValidation, TakeHandle},
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
    pub struct Receiver<T, C> {
        #[pin]
        raw: imp::MpscReceiver,
        codec: C,
        _item: PhantomData<T>,
    }
}

pub type RawReceiver = Receiver<Vec<u8>, Raw>;

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
    pub(crate) fn new(raw: imp::MpscReceiver, codec: C) -> Self {
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

impl<T, C> TakeHandle<Wasm> for Receiver<T, C>
where
    C: Decoder<T> + Default,
{
    type Id = str;
    type Handle = Self;

    fn take_handle(env: &mut Wasm, id: &str) -> Self::Handle {
        Self::new(imp::MpscReceiver::take_handle(env, id), C::default())
    }
}

impl<T, C> TakeHandle<InterfaceValidation<'_>> for Receiver<T, C>
where
    C: Encoder<T> + Decoder<T>,
{
    type Id = str;
    type Handle = ();

    fn take_handle(env: &mut InterfaceValidation<'_>, id: &str) {
        if env.interface().inbound_channel(id).is_none() {
            let err = ChannelErrorKind::Unknown.for_channel(ChannelKind::Inbound, id);
            env.insert_error(err);
        }
    }
}

pin_project! {
    pub struct Sender<T, C> {
        #[pin]
        raw: imp::MpscSender,
        codec: C,
        _item: PhantomData<T>,
    }
}

pub type RawSender = Sender<Vec<u8>, Raw>;

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
    pub(crate) fn new(raw: imp::MpscSender, codec: C) -> Self {
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

impl<T, C> TakeHandle<Wasm> for Sender<T, C>
where
    C: Encoder<T> + Default,
{
    type Id = str;
    type Handle = Self;

    fn take_handle(env: &mut Wasm, id: &str) -> Self::Handle {
        Self::new(imp::MpscSender::take_handle(env, id), C::default())
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

impl<T, C> TakeHandle<InterfaceValidation<'_>> for Sender<T, C>
where
    C: Encoder<T> + Decoder<T>,
{
    type Id = str;
    type Handle = ();

    fn take_handle(env: &mut InterfaceValidation<'_>, id: &str) {
        if env.interface().outbound_channel(id).is_none() {
            let err = ChannelErrorKind::Unknown.for_channel(ChannelKind::Outbound, id);
            env.insert_error(err);
        }
    }
}
