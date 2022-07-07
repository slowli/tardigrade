//! Mock implementation of MPSC channels.

use futures::{channel::mpsc, Sink};
use pin_project_lite::pin_project;

use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    interface::AccessError,
    test::{Runtime, TestHost},
    workflow::{TakeHandle, Wasm},
};

pub type MpscReceiver = mpsc::UnboundedReceiver<Vec<u8>>;

impl TakeHandle<Wasm> for MpscReceiver {
    type Id = str;
    type Handle = Self;

    fn take_handle(_env: &mut Wasm, id: &str) -> Result<Self, AccessError> {
        Runtime::with_mut(|rt| rt.take_inbound_channel(id))
    }
}

impl TakeHandle<TestHost> for MpscReceiver {
    type Id = str;
    type Handle = MpscSender;

    fn take_handle(_env: &mut TestHost, id: &str) -> Result<Self::Handle, AccessError> {
        let inner = Runtime::with_mut(|rt| rt.sender_for_inbound_channel(id))?;
        Ok(MpscSender { inner })
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
    #[allow(clippy::unnecessary_wraps)] // defined for convenience
    fn drop_err(_: Result<(), mpsc::SendError>) -> Result<(), Infallible> {
        // We can have the receiver dropped in tests (implicitly when the test is finished,
        // or explicitly in the test code). Since we cannot observe channel state after this,
        // we just drop upstream errors to emulate WASM sandbox, in which channels
        // are never dropped.
        Ok(())
    }
}

impl TakeHandle<Wasm> for MpscSender {
    type Id = str;
    type Handle = Self;

    fn take_handle(_env: &mut Wasm, id: &str) -> Result<Self, AccessError> {
        let inner = Runtime::with_mut(|rt| rt.outbound_channel(id))?;
        Ok(Self { inner })
    }
}

impl TakeHandle<TestHost> for MpscSender {
    type Id = str;
    type Handle = MpscReceiver;

    fn take_handle(_env: &mut TestHost, id: &str) -> Result<Self::Handle, AccessError> {
        Runtime::with_mut(|rt| rt.take_receiver_for_outbound_channel(id))
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
