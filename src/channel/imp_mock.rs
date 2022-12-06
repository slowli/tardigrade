//! Mock implementation of MPSC channels.

use futures::{channel::mpsc, Sink, Stream};
use pin_project_lite::pin_project;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::{channel::SendError, ChannelId};

pin_project! {
    #[derive(Debug)]
    pub struct MpscReceiver {
        channel_id: ChannelId,
        #[pin]
        inner: mpsc::UnboundedReceiver<Vec<u8>>,
    }
}

impl MpscReceiver {
    pub(super) fn channel_id(&self) -> ChannelId {
        self.channel_id
    }
}

impl Stream for MpscReceiver {
    type Item = Vec<u8>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

pin_project! {
    #[derive(Debug, Clone)]
    pub struct MpscSender {
        channel_id: ChannelId,
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

pub(crate) fn raw_channel(channel_id: ChannelId) -> (MpscSender, MpscReceiver) {
    let (sx, rx) = mpsc::unbounded();
    let rx = MpscReceiver {
        channel_id,
        inner: rx,
    };
    let sx = MpscSender {
        channel_id,
        inner: sx,
    };
    (sx, rx)
}
