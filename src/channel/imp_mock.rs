//! Mock implementation of MPSC channels.

use futures::{channel::mpsc, Sink};
use pin_project_lite::pin_project;

use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    mock::{Runtime, TestHost},
    Wasm,
};
use tardigrade_shared::workflow::{TakeHandle, WithHandle};

pub type MpscReceiver = mpsc::UnboundedReceiver<Vec<u8>>;

impl WithHandle<Wasm> for MpscReceiver {
    type Handle = Self;
}

impl TakeHandle<Wasm, &'static str> for MpscReceiver {
    fn take_handle(_env: &mut Wasm, id: &'static str) -> Self {
        Runtime::with_mut(|rt| rt.take_inbound_channel(id)).unwrap()
    }
}

impl WithHandle<TestHost> for MpscReceiver {
    type Handle = MpscSender;
}

impl TakeHandle<TestHost, &'static str> for MpscReceiver {
    fn take_handle(_env: &mut TestHost, id: &'static str) -> Self::Handle {
        let inner = Runtime::with_mut(|rt| rt.sender_for_inbound_channel(id)).unwrap();
        MpscSender { inner }
    }
}

pin_project! {
    #[derive(Debug, Clone)]
    #[repr(transparent)]
    pub struct MpscSender {
        #[pin]
        inner: mpsc::UnboundedSender<Vec<u8>>,
    }
}

impl WithHandle<Wasm> for MpscSender {
    type Handle = Self;
}

impl TakeHandle<Wasm, &'static str> for MpscSender {
    fn take_handle(_env: &mut Wasm, id: &'static str) -> Self {
        let inner = Runtime::with_mut(|rt| rt.outbound_channel(id)).unwrap();
        Self { inner }
    }
}

impl WithHandle<TestHost> for MpscSender {
    type Handle = MpscReceiver;
}

impl TakeHandle<TestHost, &'static str> for MpscSender {
    fn take_handle(_env: &mut TestHost, id: &'static str) -> Self::Handle {
        Runtime::with_mut(|rt| rt.take_receiver_for_outbound_channel(id)).unwrap()
    }
}

impl Sink<&[u8]> for MpscSender {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_ready(cx)
            .map_err(|err| panic!("Error while polling `Sender` readiness: {}", err))
    }

    fn start_send(self: Pin<&mut Self>, item: &[u8]) -> Result<(), Self::Error> {
        self.project()
            .inner
            .start_send(item.to_vec())
            .map_err(|err| panic!("Error sending item via `Sender`: {}", err))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_flush(cx)
            .map_err(|err| panic!("Error while flushing `Sender`: {}", err))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_flush(cx)
            .map_err(|err| panic!("Error while closing `Sender`: {}", err))
    }
}
