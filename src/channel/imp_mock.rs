//! Mock implementation of MPSC channels.

use futures::{channel::mpsc, Sink};
use pin_project_lite::pin_project;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::channel::SendError;

pub type MpscReceiver = mpsc::UnboundedReceiver<Vec<u8>>;

pin_project! {
    #[derive(Debug, Clone)]
    #[repr(transparent)]
    pub struct MpscSender {
        #[pin]
        inner: mpsc::UnboundedSender<Vec<u8>>,
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

pub(crate) fn raw_channel() -> (MpscSender, MpscReceiver) {
    let (sx, rx) = mpsc::unbounded();
    (MpscSender { inner: sx }, rx)
}
