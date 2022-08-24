//! WASM bindings for spawning child workflows.

use externref::{externref, Resource};

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::RemoteHandle;
use crate::channel::{
    imp::{mpsc_receiver_get, mpsc_sender_get, ACCESS_ERROR_PAD},
    RawReceiver, RawSender,
};
use tardigrade_shared::{abi::IntoWasm, interface::ChannelKind, JoinError, SpawnError};

static mut SPAWN_ERROR_PAD: i64 = 0;

pub(super) fn workflow_interface(id: &str) -> Option<Vec<u8>> {
    #[link(wasm_import_module = "tardigrade_rt")]
    extern "C" {
        #[link_name = "workflow::interface"]
        fn get_workflow_interface(id_ptr: *const u8, id_len: usize) -> u64;
    }

    unsafe {
        let raw = get_workflow_interface(id.as_ptr(), id.len());
        if raw == 0 {
            None
        } else {
            let ptr = (raw >> 32) as *mut u8;
            let len = (raw & 0xffff_ffff) as usize;
            Some(Vec::from_raw_parts(ptr, len, len))
        }
    }
}

#[externref]
#[link(wasm_import_module = "tardigrade_rt")]
extern "C" {
    // FIXME: also support forwarding
    #[link_name = "workflow::spawner::set_handle"]
    fn set_handle_in_spawner(
        spawner: &Resource<Spawner>,
        channel_kind: ChannelKind,
        name_ptr: *const u8,
        name_len: usize,
    );
}

#[derive(Debug)]
pub(super) struct Spawner {
    resource: Resource<Self>,
}

impl Spawner {
    #[allow(clippy::needless_pass_by_value)] // for uniformity with the mock impl
    pub fn new(id: &str, args: Vec<u8>) -> Self {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "workflow::spawner"]
            fn get_workflow_spawner(
                id_ptr: *const u8,
                id_len: usize,
                args_ptr: *const u8,
                args_len: usize,
            ) -> Resource<Spawner>;
        }

        let resource =
            unsafe { get_workflow_spawner(id.as_ptr(), id.len(), args.as_ptr(), args.len()) };
        Self { resource }
    }

    pub fn close_inbound_channel(&self, channel_name: &str) {
        unsafe {
            set_handle_in_spawner(
                &self.resource,
                ChannelKind::Inbound,
                channel_name.as_ptr(),
                channel_name.len(),
            );
        }
    }

    pub fn close_outbound_channel(&self, channel_name: &str) {
        unsafe {
            set_handle_in_spawner(
                &self.resource,
                ChannelKind::Outbound,
                channel_name.as_ptr(),
                channel_name.len(),
            );
        }
    }

    pub fn spawn(self) -> Result<RemoteWorkflow, SpawnError> {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "workflow::spawn"]
            fn spawn(
                spawner: Resource<Spawner>,
                error_ptr: *mut i64,
            ) -> Option<Resource<RemoteWorkflow>>;
        }

        unsafe {
            let workflow = spawn(self.resource, &mut SPAWN_ERROR_PAD);
            Result::<(), SpawnError>::from_abi_in_wasm(SPAWN_ERROR_PAD).map(|()| RemoteWorkflow {
                resource: workflow.unwrap(),
            })
        }
    }
}

#[derive(Debug)]
pub(crate) struct RemoteWorkflow {
    resource: Resource<Self>,
}

impl RemoteWorkflow {
    pub fn take_inbound_channel(&mut self, channel_name: &str) -> RemoteHandle<RawSender> {
        let channel = unsafe {
            mpsc_sender_get(
                Some(&self.resource),
                channel_name.as_ptr(),
                channel_name.len(),
                &mut ACCESS_ERROR_PAD,
            )
        };

        if unsafe { ACCESS_ERROR_PAD } != 0 {
            // Got an error, meaning that the channel does not exist
            debug_assert!(channel.is_none());
            RemoteHandle::None
        } else if let Some(channel) = channel {
            RemoteHandle::Some(RawSender::from_resource(channel))
        } else {
            RemoteHandle::NotCaptured
        }
    }

    pub fn take_outbound_channel(&mut self, channel_name: &str) -> RemoteHandle<RawReceiver> {
        let channel = unsafe {
            mpsc_receiver_get(
                Some(&self.resource),
                channel_name.as_ptr(),
                channel_name.len(),
                &mut ACCESS_ERROR_PAD,
            )
        };

        if unsafe { ACCESS_ERROR_PAD } != 0 {
            // Got an error, meaning that the channel does not exist
            debug_assert!(channel.is_none());
            RemoteHandle::None
        } else if let Some(channel) = channel {
            RemoteHandle::Some(RawReceiver::from_resource(channel))
        } else {
            RemoteHandle::NotCaptured
        }
    }
}

impl Future for RemoteWorkflow {
    type Output = Result<(), JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[link(wasm_import_module = "tardigrade_rt")]
        #[allow(improper_ctypes)]
        extern "C" {
            #[link_name = "workflow::poll_completion"]
            fn workflow_poll_completion(
                workflow: &Resource<RemoteWorkflow>,
                cx: *mut Context<'_>,
            ) -> i64;
        }

        unsafe {
            let poll_result = workflow_poll_completion(&self.resource, cx);
            IntoWasm::from_abi_in_wasm(poll_result)
        }
    }
}
