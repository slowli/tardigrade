//! Message channels for workflows.
//!
//! Channels have a similar interface to async [channels][future-chan] from the `futures` crate;
//! their ends implement [`Stream`] / [`Sink`] traits.
//! A workflow owns a single end of a channel (either an inbound [`Receiver`] or outbound
//! [`Sender`]s), while the other end is owned by the host environment.
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
    interface::{
        AccessError, AccessErrorKind, InboundChannel, Interface, OutboundChannel, ValidateInterface,
    },
    workflow::{TakeHandle, Wasm},
};

mod broadcast;
#[cfg(target_arch = "wasm32")]
#[path = "imp_wasm32.rs"]
mod imp;
#[cfg(not(target_arch = "wasm32"))]
#[path = "imp_mock.rs"]
pub(crate) mod imp;
mod requests;

pub use self::{
    broadcast::{BroadcastError, BroadcastPublisher, BroadcastSubscriber},
    requests::{Requests, RequestsBuilder, WithId},
};
pub use tardigrade_shared::SendError;

pin_project! {
    /// Receiver for an inbound channel provided to the workflow.
    ///
    /// A receiver is characterized by the type of messages and the codec used to convert messages
    /// from / to bytes.
    pub struct Receiver<T, C> {
        #[pin]
        raw: imp::MpscReceiver,
        codec: C,
        _item: PhantomData<T>,
    }
}

/// [`Receiver`] of raw byte messages.
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

impl<T, C: Decode<T>> Receiver<T, C> {
    pub(crate) fn new(raw: imp::MpscReceiver, codec: C) -> Self {
        Self {
            raw,
            codec,
            _item: PhantomData,
        }
    }
}

impl RawReceiver {
    pub(crate) fn with_codec<T, C>(self, codec: C) -> Receiver<T, C>
    where
        C: Decode<T>,
    {
        Receiver::new(self.raw, codec)
    }
}

impl<T, C: Decode<T>> Stream for Receiver<T, C> {
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
    C: Decode<T> + Default,
{
    type Id = str;
    type Handle = Self;

    #[cfg(target_arch = "wasm32")]
    fn take_handle(env: &mut Wasm, id: &str) -> Result<Self::Handle, AccessError> {
        imp::MpscReceiver::take_handle(env, id).map(|raw| Self::new(raw, C::default()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn take_handle(env: &mut Wasm, id: &str) -> Result<Self::Handle, AccessError> {
        let raw = env
            .take_inbound_channel(id)
            .ok_or_else(|| AccessErrorKind::Unknown.with_location(InboundChannel(id)))?;
        Ok(raw.with_codec(C::default()))
    }
}

impl<T, C> ValidateInterface for Receiver<T, C>
where
    C: Encode<T> + Decode<T>,
{
    type Id = str;

    fn validate_interface(interface: &Interface<()>, id: &str) -> Result<(), AccessError> {
        if interface.inbound_channel(id).is_none() {
            let err = AccessErrorKind::Unknown.with_location(InboundChannel(id));
            return Err(err);
        }
        Ok(())
    }
}

pin_project! {
    /// Sender for an outbound channel provided to the workflow.
    ///
    /// A sender is characterized by the type of messages and the codec used to convert messages
    /// from / to bytes.
    ///
    /// Unlike [`Receiver`]s, `Sender` parts of the channel can be cloned. Another difference
    /// is ability to control readiness / flushing of outbound channels via channel capacity;
    /// if the outbound channel reaches its capacity of non-flushed messages, it becomes not ready
    /// to accept more messages.
    pub struct Sender<T, C> {
        #[pin]
        raw: imp::MpscSender,
        codec: C,
        _item: PhantomData<T>,
    }
}

/// [`Sender`] of raw byte messages.
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

impl<T, C: Encode<T> + Clone> Clone for Sender<T, C> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            codec: self.codec.clone(),
            _item: PhantomData,
        }
    }
}

impl<T, C: Encode<T>> Sender<T, C> {
    pub(crate) fn new(raw: imp::MpscSender, codec: C) -> Self {
        Self {
            raw,
            codec,
            _item: PhantomData,
        }
    }
}

impl RawSender {
    pub(crate) fn with_codec<T, C>(self, codec: C) -> Sender<T, C>
    where
        C: Encode<T>,
    {
        Sender::new(self.raw, codec)
    }
}

impl<T, C> TakeHandle<Wasm> for Sender<T, C>
where
    C: Encode<T> + Default,
{
    type Id = str;
    type Handle = Self;

    #[cfg(target_arch = "wasm32")]
    fn take_handle(env: &mut Wasm, id: &str) -> Result<Self::Handle, AccessError> {
        imp::MpscSender::take_handle(env, id).map(|raw| Self::new(raw, C::default()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn take_handle(env: &mut Wasm, id: &str) -> Result<Self::Handle, AccessError> {
        let raw = env
            .take_outbound_channel(id)
            .ok_or_else(|| AccessErrorKind::Unknown.with_location(OutboundChannel(id)))?;
        Ok(raw.with_codec(C::default()))
    }
}

impl<T, C: Encode<T>> Sink<T> for Sender<T, C> {
    type Error = SendError;

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

impl<T, C> ValidateInterface for Sender<T, C>
where
    C: Encode<T> + Decode<T>,
{
    type Id = str;

    fn validate_interface(interface: &Interface<()>, id: &str) -> Result<(), AccessError> {
        if interface.outbound_channel(id).is_none() {
            let err = AccessErrorKind::Unknown.with_location(OutboundChannel(id));
            Err(err)
        } else {
            Ok(())
        }
    }
}
