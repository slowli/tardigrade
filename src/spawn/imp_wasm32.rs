//! WASM bindings for spawning child workflows.

use externref::{externref, Resource};

use std::{
    borrow::Cow,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::{ChannelHandles, ManageInterfaces, ManageWorkflows, RemoteHandle, Workflows};
use crate::channel::{
    imp::{mpsc_receiver_get, mpsc_sender_get, ACCESS_ERROR_PAD},
    RawReceiver, RawSender,
};
use tardigrade_shared::{
    abi::{IntoWasm, TryFromWasm},
    interface::{ChannelKind, Interface},
    JoinError, SpawnError,
};

static mut SPAWN_ERROR_PAD: i64 = 0;

impl ManageInterfaces for Workflows {
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface<()>>> {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "workflow::interface"]
            fn get_workflow_interface(id_ptr: *const u8, id_len: usize) -> u64;
        }

        let raw_interface = unsafe {
            let raw = get_workflow_interface(definition_id.as_ptr(), definition_id.len());
            if raw == 0 {
                None
            } else {
                let ptr = (raw >> 32) as *mut u8;
                let len = (raw & 0xffff_ffff) as usize;
                Some(Vec::from_raw_parts(ptr, len, len))
            }
        };
        raw_interface.map(|bytes| Cow::Owned(Interface::from_bytes(&bytes)))
    }
}

impl ManageWorkflows<'_, ()> for Workflows {
    type Handle = super::RemoteWorkflow;

    fn create_workflow(
        &self,
        definition_id: &str,
        args: Vec<u8>,
        handles: &ChannelHandles,
    ) -> Result<Self::Handle, SpawnError> {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "workflow::spawn"]
            fn spawn(
                id_ptr: *const u8,
                id_len: usize,
                args_ptr: *const u8,
                args_len: usize,
                handles: Resource<ChannelHandles>,
                error_ptr: *mut i64,
            ) -> Option<Resource<RemoteWorkflow>>;
        }

        let result = unsafe {
            let workflow = spawn(
                definition_id.as_ptr(),
                definition_id.len(),
                args.as_ptr(),
                args.len(),
                transform_handles(handles),
                &mut SPAWN_ERROR_PAD,
            );
            Result::<(), SpawnError>::from_abi_in_wasm(SPAWN_ERROR_PAD).map(|()| RemoteWorkflow {
                resource: workflow.unwrap(),
            })
        };
        result.map(|inner| super::RemoteWorkflow { inner })
    }
}

unsafe fn transform_handles(handles: &ChannelHandles) -> Resource<ChannelHandles> {
    #[externref]
    #[link(wasm_import_module = "tardigrade_rt")]
    extern "C" {
        #[link_name = "workflow::create_handles"]
        fn create_handles() -> Resource<ChannelHandles>;

        // FIXME: also support forwarding
        #[link_name = "workflow::set_handle"]
        fn set_handle(
            spawner: &Resource<ChannelHandles>,
            channel_kind: ChannelKind,
            name_ptr: *const u8,
            name_len: usize,
            config: i32,
        );
    }

    let resource = create_handles();
    for (name, config) in &handles.inbound {
        set_handle(
            &resource,
            ChannelKind::Inbound,
            name.as_ptr(),
            name.len(),
            config.into_abi_in_wasm(),
        );
    }
    for (name, config) in &handles.outbound {
        set_handle(
            &resource,
            ChannelKind::Outbound,
            name.as_ptr(),
            name.len(),
            config.into_abi_in_wasm(),
        );
    }
    resource
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
        #[externref]
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
