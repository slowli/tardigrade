//! WASM bindings for spawning child workflows.

use async_trait::async_trait;
use externref::{externref, Resource};

use std::{
    borrow::Cow,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::{
    ChannelSpawnConfig, ChannelsConfig, HostError, ManageInterfaces, ManageWorkflows, Remote,
    Workflows,
};
use crate::{
    abi::{IntoWasm, PollTask, TryFromWasm},
    channel::{
        imp::{mpsc_receiver_get, mpsc_sender_get, MpscSender, ACCESS_ERROR_PAD},
        RawReceiver, RawSender,
    },
    interface::{
        AccessError, AccessErrorKind, ChannelHalf, HandlePath, Interface, ReceiverAt, SenderAt,
    },
    task::{JoinError, TaskError},
};

static mut HOST_ERROR_PAD: i64 = 0;

impl ManageInterfaces for Workflows {
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "workflow::interface"]
            fn get_workflow_interface(id_ptr: *const u8, id_len: usize) -> i64;
        }

        let raw_interface = unsafe {
            let raw = get_workflow_interface(definition_id.as_ptr(), definition_id.len());
            Option::<Vec<u8>>::from_abi_in_wasm(raw)
        };
        raw_interface.map(|bytes| Cow::Owned(Interface::from_bytes(&bytes)))
    }
}

#[async_trait]
impl ManageWorkflows<()> for Workflows {
    type Handle<'s> = super::RemoteWorkflow;
    type Error = HostError;

    async fn create_workflow(
        &self,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<RawReceiver, RawSender>,
    ) -> Result<Self::Handle<'_>, Self::Error> {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "workflow::spawn"]
            fn spawn(
                id_ptr: *const u8,
                id_len: usize,
                args_ptr: *const u8,
                args_len: usize,
                handles: Resource<ChannelsConfig<RawReceiver, RawSender>>,
            ) -> Resource<RemoteWorkflow>;
        }

        let stub = unsafe {
            spawn(
                definition_id.as_ptr(),
                definition_id.len(),
                args.as_ptr(),
                args.len(),
                config_into_resource(channels),
            )
        };
        WorkflowInit { stub }.await
    }
}

#[derive(Debug)]
struct WorkflowInit {
    stub: Resource<RemoteWorkflow>,
}

impl Future for WorkflowInit {
    type Output = Result<super::RemoteWorkflow, HostError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[externref]
        #[link(wasm_import_module = "tardigrade_rt")]
        #[allow(improper_ctypes)]
        extern "C" {
            #[link_name = "workflow::poll_init"]
            fn poll_init(
                stub: &Resource<RemoteWorkflow>,
                cx: *mut Context<'_>,
                error_ptr: *mut i64,
            ) -> Option<Resource<RemoteWorkflow>>;
        }

        unsafe {
            let poll_result = poll_init(&self.stub, cx, &mut HOST_ERROR_PAD);
            if let Some(resource) = poll_result {
                Poll::Ready(Ok(super::RemoteWorkflow {
                    inner: RemoteWorkflow { resource },
                }))
            } else {
                let host_result = Result::<(), HostError>::from_abi_in_wasm(HOST_ERROR_PAD);
                match host_result {
                    Ok(()) => Poll::Pending,
                    Err(err) => Poll::Ready(Err(err)),
                }
            }
        }
    }
}

#[externref]
#[link(wasm_import_module = "tardigrade_rt")]
extern "C" {
    #[link_name = "workflow::create_handles"]
    fn create_handles() -> Resource<ChannelsConfig<RawReceiver, RawSender>>;

    #[link_name = "workflow::insert_handle"]
    fn insert_handle(
        spawner: &Resource<ChannelsConfig<RawReceiver, RawSender>>,
        channel_kind: i32,
        path_ptr: *const u8,
        path_len: usize,
        is_closed: bool,
    );

    #[link_name = "workflow::copy_sender_handle"]
    fn copy_sender_handle(
        spawner: &Resource<ChannelsConfig<RawReceiver, RawSender>>,
        name_ptr: *const u8,
        name_len: usize,
        sender: &Resource<MpscSender>,
    );
}

impl ChannelSpawnConfig<RawReceiver> {
    unsafe fn configure(
        self,
        spawner: &Resource<ChannelsConfig<RawReceiver, RawSender>>,
        path: HandlePath<'_>,
    ) {
        match self {
            Self::New | Self::Closed => {
                let path = path.to_cow_string();
                insert_handle(
                    spawner,
                    ChannelHalf::Receiver.into_abi_in_wasm(),
                    path.as_ptr(),
                    path.len(),
                    matches!(self, Self::Closed),
                );
            }
            Self::Existing(_) => todo!(),
        }
    }
}

impl ChannelSpawnConfig<RawSender> {
    unsafe fn configure(
        self,
        spawner: &Resource<ChannelsConfig<RawReceiver, RawSender>>,
        path: HandlePath<'_>,
    ) {
        let path = path.to_cow_string();
        match self {
            Self::New | Self::Closed => {
                insert_handle(
                    spawner,
                    ChannelHalf::Sender.into_abi_in_wasm(),
                    path.as_ptr(),
                    path.len(),
                    matches!(self, Self::Closed),
                );
            }
            Self::Existing(sender) => {
                copy_sender_handle(spawner, path.as_ptr(), path.len(), sender.as_resource());
            }
        }
    }
}

unsafe fn config_into_resource(
    config: ChannelsConfig<RawReceiver, RawSender>,
) -> Resource<ChannelsConfig<RawReceiver, RawSender>> {
    let resource = create_handles();
    for (path, config) in config {
        let path = path.as_ref();
        config
            .map_receiver(|config| config.configure(&resource, path))
            .map_sender(|config| config.configure(&resource, path));
    }
    resource
}

#[derive(Debug)]
pub(crate) struct RemoteWorkflow {
    resource: Resource<Self>,
}

impl RemoteWorkflow {
    pub fn take_receiver(
        &mut self,
        path: HandlePath<'_>,
    ) -> Result<Remote<RawSender>, AccessError> {
        let path_str = path.to_cow_string();
        let channel = unsafe {
            mpsc_sender_get(
                Some(&self.resource),
                path_str.as_ptr(),
                path_str.len(),
                &mut ACCESS_ERROR_PAD,
            )
        };

        unsafe {
            Result::<(), AccessErrorKind>::from_abi_in_wasm(ACCESS_ERROR_PAD)
                .map_err(|kind| kind.with_location(ReceiverAt(path)))?;
        }
        if let Some(channel) = channel {
            Ok(Remote::Some(RawSender::from_resource(channel)))
        } else {
            Ok(Remote::NotCaptured)
        }
    }

    pub fn take_sender(
        &mut self,
        path: HandlePath<'_>,
    ) -> Result<Remote<RawReceiver>, AccessError> {
        let path_str = path.to_cow_string();
        let channel = unsafe {
            mpsc_receiver_get(
                Some(&self.resource),
                path_str.as_ptr(),
                path_str.len(),
                &mut ACCESS_ERROR_PAD,
            )
        };

        unsafe {
            Result::<(), AccessErrorKind>::from_abi_in_wasm(ACCESS_ERROR_PAD)
                .map_err(|kind| kind.with_location(SenderAt(path)))?;
        }
        if let Some(channel) = channel {
            Ok(Remote::Some(RawReceiver::from_resource(channel)))
        } else {
            Ok(Remote::NotCaptured)
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

            #[link_name = "workflow::completion_error"]
            fn workflow_completion_error(workflow: &Resource<RemoteWorkflow>) -> i64;
        }

        unsafe {
            let poll_result = workflow_poll_completion(&self.resource, cx);
            let poll_result = PollTask::from_abi_in_wasm(poll_result);
            poll_result.map(|res| {
                res.map_err(|_| JoinError::Aborted).and_then(|()| {
                    let maybe_err = workflow_completion_error(&self.resource);
                    if maybe_err == 0 {
                        Ok(())
                    } else {
                        let err = TaskError::from_abi_in_wasm(maybe_err);
                        Err(JoinError::Err(err))
                    }
                })
            })
        }
    }
}
