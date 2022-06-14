//! WASM implementation of MPSC channels.

use futures::{Sink, Stream};

use std::{
    convert::Infallible,
    mem::ManuallyDrop,
    pin::Pin,
    task::{Context, Poll},
};

use crate::context::Wasm;
use tardigrade_shared::{
    workflow::{TakeHandle, WithHandle},
    ChannelKind, IntoWasm, RawChannelResult,
};

#[derive(Debug)]
pub struct MpscReceiver {
    channel_name: &'static str,
}

impl WithHandle<Wasm> for MpscReceiver {
    type Handle = Self;
}

impl TakeHandle<Wasm, &'static str> for MpscReceiver {
    fn take_handle(_env: &mut Wasm, id: &'static str) -> Self {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "mpsc_receiver::get"]
            fn mpsc_receiver_get(channel_name_ptr: *const u8, channel_name_len: usize) -> i32;
        }

        let result = unsafe {
            let result = mpsc_receiver_get(id.as_ptr(), id.len());
            RawChannelResult::from_abi_in_wasm(result)
                .map(|()| Self { channel_name: id })
                .map_err(|kind| kind.for_channel(ChannelKind::Inbound, id))
        };
        result.unwrap()
    }
}

impl Stream for MpscReceiver {
    type Item = Vec<u8>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        #[link(wasm_import_module = "tardigrade_rt")]
        #[allow(improper_ctypes)]
        extern "C" {
            #[link_name = "mpsc_receiver::poll_next"]
            fn mpsc_receiver_poll_next(
                channel_name_ptr: *const u8,
                channel_name_len: usize,
                cx: *mut Context<'_>,
            ) -> i64;
        }

        unsafe {
            let result =
                mpsc_receiver_poll_next(self.channel_name.as_ptr(), self.channel_name.len(), cx);
            IntoWasm::from_abi_in_wasm(result)
        }
    }
}

/// Unbounded sender end of an MPSC channel. Guaranteed to never close.
#[derive(Debug, Clone)]
pub struct MpscSender {
    channel_name: &'static str,
}

impl WithHandle<Wasm> for MpscSender {
    type Handle = Self;
}

impl TakeHandle<Wasm, &'static str> for MpscSender {
    fn take_handle(_env: &mut Wasm, id: &'static str) -> Self {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "mpsc_sender::get"]
            fn mpsc_sender_get(channel_name_ptr: *const u8, channel_name_len: usize) -> i32;
        }

        let result = unsafe {
            let result = mpsc_sender_get(id.as_ptr(), id.len());
            RawChannelResult::from_abi_in_wasm(result)
                .map(|()| Self { channel_name: id })
                .map_err(|kind| kind.for_channel(ChannelKind::Outbound, id))
        };
        result.unwrap()
    }
}

impl MpscSender {
    fn do_send(&self, message: &[u8]) {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "mpsc_sender::start_send"]
            fn mpsc_sender_start_send(
                channel_name_ptr: *const u8,
                channel_name_len: usize,
                message_ptr: *const u8,
                message_len: usize,
            );
        }

        unsafe {
            mpsc_sender_start_send(
                self.channel_name.as_ptr(),
                self.channel_name.len(),
                message.as_ptr(),
                message.len(),
            );
        }
    }
}

impl Sink<&[u8]> for MpscSender {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        #[link(wasm_import_module = "tardigrade_rt")]
        #[allow(improper_ctypes)]
        extern "C" {
            #[link_name = "mpsc_sender::poll_ready"]
            fn mpsc_sender_poll_ready(
                channel_name_ptr: *const u8,
                channel_name_len: usize,
                cx: *mut Context<'_>,
            ) -> i32;
        }

        unsafe {
            let poll_result =
                mpsc_sender_poll_ready(self.channel_name.as_ptr(), self.channel_name.len(), cx);
            Poll::<()>::from_abi_in_wasm(poll_result).map(Ok)
        }
    }

    fn start_send(self: Pin<&mut Self>, item: &[u8]) -> Result<(), Self::Error> {
        self.do_send(item.as_ref());
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        #[link(wasm_import_module = "tardigrade_rt")]
        #[allow(improper_ctypes)]
        extern "C" {
            #[link_name = "mpsc_sender::poll_flush"]
            fn mpsc_sender_poll_flush(
                channel_name_ptr: *const u8,
                channel_name_len: usize,
                cx: *mut Context<'_>,
            ) -> i32;
        }

        unsafe {
            let poll_result =
                mpsc_sender_poll_flush(self.channel_name.as_ptr(), self.channel_name.len(), cx);
            Poll::<()>::from_abi_in_wasm(poll_result).map(Ok)
        }
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Channel cannot really be closed, so we always return OK
        Poll::Ready(Ok(()))
    }
}

#[no_mangle]
#[export_name = "tardigrade_rt::alloc_bytes"]
pub extern "C" fn __tardigrade_rt__alloc_bytes(capacity: usize) -> *mut u8 {
    let bytes = Vec::<u8>::with_capacity(capacity);
    let mut bytes = ManuallyDrop::new(bytes);
    bytes.as_mut_ptr()
}
