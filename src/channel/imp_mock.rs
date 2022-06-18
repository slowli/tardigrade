//! Mock implementation of MPSC channels.

use futures::{channel::mpsc, Sink};
use pin_project_lite::pin_project;

use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    test::{Runtime, TestHost},
    Wasm,
};
use tardigrade_shared::workflow::TakeHandle;

pub type MpscReceiver = mpsc::UnboundedReceiver<Vec<u8>>;

impl TakeHandle<Wasm, &'static str> for MpscReceiver {
    type Handle = Self;

    fn take_handle(_env: &mut Wasm, id: &'static str) -> Self {
        Runtime::with_mut(|rt| rt.take_inbound_channel(id)).unwrap()
    }
}

impl TakeHandle<TestHost, &'static str> for MpscReceiver {
    type Handle = MpscSender;

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

impl MpscSender {
    fn drop_err(_: Result<(), mpsc::SendError>) -> Result<(), Infallible> {
        // We can have the receiver dropped in tests (implicitly when the test is finished,
        // or explicitly in the test code). Since we cannot observe channel state after this,
        // we just drop upstream errors to emulate WASM sandbox, in which channels
        // are never dropped.
        Ok(())
    }
}

impl TakeHandle<Wasm, &'static str> for MpscSender {
    type Handle = Self;

    fn take_handle(_env: &mut Wasm, id: &'static str) -> Self {
        let inner = Runtime::with_mut(|rt| rt.outbound_channel(id)).unwrap();
        Self { inner }
    }
}

impl TakeHandle<TestHost, &'static str> for MpscSender {
    type Handle = MpscReceiver;

    fn take_handle(_env: &mut TestHost, id: &'static str) -> Self::Handle {
        Runtime::with_mut(|rt| rt.take_receiver_for_outbound_channel(id)).unwrap()
    }
}

impl Sink<&[u8]> for MpscSender {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx).map(Self::drop_err)
    }

    fn start_send(self: Pin<&mut Self>, item: &[u8]) -> Result<(), Self::Error> {
        let res = self.project().inner.start_send(item.to_vec());
        Self::drop_err(res)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map(Self::drop_err)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx).map(Self::drop_err)
    }
}
