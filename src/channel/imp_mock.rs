//! Mock implementation of MPSC channels.

use futures::{channel::mpsc, Sink};
use pin_project_lite::pin_project;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    channel::SendError,
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
        let inner = Runtime::with_mut(|rt| rt.take_sender_for_inbound_channel(id))?;
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
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_ready(cx)
            .map_err(|err| convert_send_err(&err))
    }

    fn start_send(self: Pin<&mut Self>, item: &[u8]) -> Result<(), Self::Error> {
        let res = self.project().inner.start_send(item.to_vec());
        res.map_err(|err| convert_send_err(&err))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_flush(cx)
            .map_err(|err| convert_send_err(&err))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_close(cx)
            .map_err(|err| convert_send_err(&err))
    }
}

fn convert_send_err(err: &mpsc::SendError) -> SendError {
    if err.is_full() {
        SendError::Full
    } else {
        SendError::Closed
    }
}
