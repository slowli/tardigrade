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
    interface::{AccessError, AccessErrorKind, HandlePath, ReceiverAt, SenderAt},
    spawn::imp::RemoteWorkflow,
    ChannelId,
};

pub(crate) static mut ACCESS_ERROR_PAD: i64 = 0;

#[externref]
#[link(wasm_import_module = "tardigrade_rt")]
extern "C" {
    #[link_name = "mpsc_receiver::get"]
    pub(crate) fn mpsc_receiver_get(
        workflow: Option<&Resource<RemoteWorkflow>>,
        path_ptr: *const u8,
        path_len: usize,
        error_ptr: *mut i64,
    ) -> Option<Resource<MpscReceiver>>;
}

#[derive(Debug)]
pub(crate) struct MpscReceiver {
    resource: Resource<Self>,
}

impl From<Resource<Self>> for MpscReceiver {
    fn from(resource: Resource<Self>) -> Self {
        Self { resource }
    }
}

impl MpscReceiver {
    pub(super) fn from_env(path: HandlePath<'_>) -> Result<Self, AccessError> {
        let path_str = path.to_cow_string();
        unsafe {
            let resource = mpsc_receiver_get(
                None,
                path_str.as_ptr(),
                path_str.len(),
                &mut ACCESS_ERROR_PAD,
            );
            Result::<(), AccessErrorKind>::from_abi_in_wasm(ACCESS_ERROR_PAD)
                .map(|()| resource.unwrap().into())
                .map_err(|kind| kind.with_location(ReceiverAt(path)))
        }
    }

    pub(super) fn channel_id(&self) -> ChannelId {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "resource::id"]
            fn mpsc_receiver_id(receiver: &Resource<MpscReceiver>) -> ChannelId;
        }

        unsafe { mpsc_receiver_id(&self.resource) }
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
        path_ptr: *const u8,
        path_len: usize,
        error_ptr: *mut i64,
    ) -> Option<Resource<MpscSender>>;
}

/// Unbounded sender end of an MPSC channel.
#[derive(Debug, Clone)]
pub(crate) struct MpscSender {
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
    pub(super) fn from_env(path: HandlePath<'_>) -> Result<Self, AccessError> {
        let path_str = path.to_cow_string();
        unsafe {
            let resource = mpsc_sender_get(
                None,
                path_str.as_ptr(),
                path_str.len(),
                &mut ACCESS_ERROR_PAD,
            );
            Result::<(), AccessErrorKind>::from_abi_in_wasm(ACCESS_ERROR_PAD)
                .map(|()| resource.unwrap().into())
                .map_err(|kind| kind.with_location(SenderAt(path)))
        }
    }

    pub(super) fn as_resource(&self) -> &Resource<Self> {
        &self.resource
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
