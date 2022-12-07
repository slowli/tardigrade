//! WASM implementation of MPSC channels.

use externref::{externref, Resource};
use futures::{Sink, Stream};
use once_cell::unsync::Lazy;

use std::{
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    abi::IntoWasm,
    channel::SendError,
    wasm_utils::{Registry, StubState},
    ChannelId,
};

#[derive(Debug)]
pub struct MpscReceiver {
    resource: Resource<Self>,
}

impl From<Resource<Self>> for MpscReceiver {
    fn from(resource: Resource<Self>) -> Self {
        Self { resource }
    }
}

impl MpscReceiver {
    pub(super) fn closed() -> Self {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "mpsc_receiver::closed"]
            fn closed_mpsc_receiver() -> Resource<MpscReceiver>;
        }

        let resource = unsafe { closed_mpsc_receiver() };
        Self { resource }
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

    pub(super) fn into_resource(self) -> Resource<Self> {
        self.resource
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

/// Unbounded sender end of an MPSC channel.
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
    pub(super) fn closed() -> Self {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "mpsc_sender::closed"]
            fn closed_mpsc_sender() -> Resource<MpscSender>;
        }

        let resource = unsafe { closed_mpsc_sender() };
        Self {
            resource: Arc::new(resource),
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

#[export_name = "tardigrade_rt::alloc_bytes"]
pub extern "C" fn __tardigrade_rt_alloc_bytes(capacity: usize) -> *mut u8 {
    let bytes = Vec::<u8>::with_capacity(capacity);
    let mut bytes = ManuallyDrop::new(bytes);
    bytes.as_mut_ptr()
}

type ChannelStub = StubState<(MpscSender, MpscReceiver)>;

// There may be multiple concurrent channels being created.
static mut CHANNELS: Lazy<Registry<ChannelStub>> = Lazy::new(|| Registry::with_capacity(4));

#[derive(Debug)]
struct NewChannel {
    stub_id: u64,
}

impl NewChannel {
    fn new() -> Self {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "channel::new"]
            fn new_channel(stub_id: u64);
        }

        let stub_id = unsafe { CHANNELS.insert(ChannelStub::default()) };
        unsafe {
            new_channel(stub_id);
        }
        Self { stub_id }
    }
}

impl Future for NewChannel {
    type Output = (MpscSender, MpscReceiver);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let poll_result = unsafe { CHANNELS.get_mut(self.stub_id).poll(cx) };
        if poll_result.is_ready() {
            unsafe {
                CHANNELS.remove(self.stub_id);
            }
        }
        poll_result
    }
}

#[externref]
#[export_name = "tardigrade_rt::init_channel"]
pub unsafe extern "C" fn __tardigrade_rt_init_channel(
    stub_id: u64,
    sender: Resource<MpscSender>,
    receiver: Resource<MpscReceiver>,
) {
    let sender = MpscSender {
        resource: Arc::new(sender),
    };
    let receiver = MpscReceiver { resource: receiver };
    CHANNELS.get_mut(stub_id).set_value((sender, receiver));
}

pub(super) async fn raw_channel() -> (MpscSender, MpscReceiver) {
    NewChannel::new().await
}
