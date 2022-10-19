//! WASM implementation of MPSC channels.

use externref::{externref, Resource};
use futures::{Sink, Stream};

use std::{
    mem::ManuallyDrop,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    abi::IntoWasm,
    channel::SendError,
    interface::{AccessError, AccessErrorKind, InboundChannel, OutboundChannel},
    spawn::imp::RemoteWorkflow,
    workflow::{TakeHandle, Wasm},
};

pub(crate) static mut ACCESS_ERROR_PAD: i64 = 0;

#[externref]
#[link(wasm_import_module = "tardigrade_rt")]
extern "C" {
    #[link_name = "mpsc_receiver::get"]
    pub(crate) fn mpsc_receiver_get(
        workflow: Option<&Resource<RemoteWorkflow>>,
        channel_name_ptr: *const u8,
        channel_name_len: usize,
        error_ptr: *mut i64,
    ) -> Option<Resource<MpscReceiver>>;
}

#[derive(Debug)]
pub struct MpscReceiver {
    resource: Resource<Self>,
}

impl From<Resource<Self>> for MpscReceiver {
    fn from(resource: Resource<Self>) -> Self {
        Self { resource }
    }
}

impl TakeHandle<Wasm> for MpscReceiver {
    type Id = str;
    type Handle = Self;

    fn take_handle(_env: &mut Wasm, id: &str) -> Result<Self, AccessError> {
        unsafe {
            let resource = mpsc_receiver_get(None, id.as_ptr(), id.len(), &mut ACCESS_ERROR_PAD);
            Result::<(), AccessErrorKind>::from_abi_in_wasm(ACCESS_ERROR_PAD)
                .map(|()| resource.unwrap().into())
                .map_err(|kind| kind.with_location(InboundChannel(id)))
        }
    }
}

impl Stream for MpscReceiver {
    type Item = Vec<u8>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        #[allow(improper_ctypes)]
        extern "C" {
            #[link_name = "mpsc_receiver::poll_next"]
            fn mpsc_receiver_poll_next(
                receiver: &Resource<MpscReceiver>,
                cx: *mut Context<'_>,
            ) -> i64;
        }

        unsafe {
            let result = mpsc_receiver_poll_next(&self.resource, cx);
            IntoWasm::from_abi_in_wasm(result)
        }
    }
}

#[externref]
#[link(wasm_import_module = "tardigrade_rt")]
extern "C" {
    #[link_name = "mpsc_sender::get"]
    pub(crate) fn mpsc_sender_get(
        workflow: Option<&Resource<RemoteWorkflow>>,
        channel_name_ptr: *const u8,
        channel_name_len: usize,
        error_ptr: *mut i64,
    ) -> Option<Resource<MpscSender>>;
}

/// Unbounded sender end of an MPSC channel. Guaranteed to never close.
#[derive(Debug, Clone)]
pub struct MpscSender {
    resource: Arc<Resource<Self>>,
}

impl From<Resource<Self>> for MpscSender {
    fn from(resource: Resource<Self>) -> Self {
        Self {
            resource: Arc::new(resource),
        }
    }
}

impl MpscSender {
    pub(super) fn as_resource(&self) -> &Resource<Self> {
        &self.resource
    }
}

impl TakeHandle<Wasm> for MpscSender {
    type Id = str;
    type Handle = Self;

    fn take_handle(_env: &mut Wasm, id: &str) -> Result<Self, AccessError> {
        unsafe {
            let resource = mpsc_sender_get(None, id.as_ptr(), id.len(), &mut ACCESS_ERROR_PAD);
            Result::<(), AccessErrorKind>::from_abi_in_wasm(ACCESS_ERROR_PAD)
                .map(|()| resource.unwrap().into())
                .map_err(|kind| kind.with_location(OutboundChannel(id)))
        }
    }
}

impl Sink<&[u8]> for MpscSender {
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        #[allow(improper_ctypes)]
        extern "C" {
            #[link_name = "mpsc_sender::poll_ready"]
            fn mpsc_sender_poll_ready(sender: &Resource<MpscSender>, cx: *mut Context<'_>) -> i32;
        }

        unsafe {
            let poll_result = mpsc_sender_poll_ready(&self.resource, cx);
            Poll::<Result<(), SendError>>::from_abi_in_wasm(poll_result)
        }
    }

    fn start_send(self: Pin<&mut Self>, item: &[u8]) -> Result<(), Self::Error> {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "mpsc_sender::start_send"]
            fn mpsc_sender_start_send(
                sender: &Resource<MpscSender>,
                message_ptr: *const u8,
                message_len: usize,
            ) -> i32;
        }

        unsafe {
            let result = mpsc_sender_start_send(&self.resource, item.as_ptr(), item.len());
            Result::<(), SendError>::from_abi_in_wasm(result)
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        #[allow(improper_ctypes)]
        extern "C" {
            #[link_name = "mpsc_sender::poll_flush"]
            fn mpsc_sender_poll_flush(sender: &Resource<MpscSender>, cx: *mut Context<'_>) -> i32;
        }

        unsafe {
            let poll_result = mpsc_sender_poll_flush(&self.resource, cx);
            Poll::<Result<(), SendError>>::from_abi_in_wasm(poll_result)
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
