//! WASM bindings for spawning child workflows.

use externref::{externref, Resource};
use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use once_cell::unsync::Lazy;

use std::{
    borrow::Cow,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::{HostError, ManageInterfaces, Workflows};
use crate::{
    abi::{IntoWasm, PollTask},
    handle::HandlePath,
    interface::Interface,
    task::{JoinError, TaskError},
    wasm_utils::{HostHandles, Registry, StubState},
    workflow::{Wasm, WithHandle, WorkflowFn},
    Codec, WorkflowId,
};

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

type WorkflowStub = StubState<Result<RemoteWorkflow, HostError>>;

static mut WORKFLOWS: Lazy<Registry<WorkflowStub>> = Lazy::new(|| Registry::with_capacity(1));

pub(super) fn new_workflow<W: WorkflowFn + WithHandle>(
    definition_id: &str,
    args: W::Args,
    handles: W::Handle<Wasm>,
) -> BoxFuture<'static, Result<super::RemoteWorkflow, HostError>> {
    #[externref]
    #[link(wasm_import_module = "tardigrade_rt")]
    extern "C" {
        #[link_name = "workflow::spawn"]
        fn spawn_workflow(
            stub_id: WorkflowId,
            definition_id_ptr: *const u8,
            definition_id_len: usize,
            args_ptr: *const u8,
            args_len: usize,
            handles: Resource<HostHandles>,
        );
    }

    let raw_args = <W::Codec>::encode_value(args);
    let mut host_handles = HostHandles::new();
    W::insert_into_untyped(handles, &mut host_handles, HandlePath::EMPTY);
    let stub_id = unsafe { WORKFLOWS.insert(WorkflowStub::default()) };
    unsafe {
        spawn_workflow(
            stub_id,
            definition_id.as_ptr(),
            definition_id.len(),
            raw_args.as_ptr(),
            raw_args.len(),
            host_handles.into_resource(),
        );
    }
    NewWorkflow { stub_id }
        .map_ok(|inner| super::RemoteWorkflow { inner })
        .boxed()
}

#[derive(Debug)]
struct NewWorkflow {
    stub_id: WorkflowId,
}

impl Future for NewWorkflow {
    type Output = Result<RemoteWorkflow, HostError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let poll_result = unsafe { WORKFLOWS.get_mut(self.stub_id).poll(cx) };
        if poll_result.is_ready() {
            unsafe {
                WORKFLOWS.remove(self.stub_id);
            }
        }
        poll_result
    }
}

#[externref]
#[export_name = "tardigrade_rt::init_child"]
pub unsafe extern "C" fn __tardigrade_rt_init_child(
    stub_id: WorkflowId,
    child: Option<Resource<RemoteWorkflow>>,
    error: i64,
) {
    let result = Result::<(), HostError>::from_abi_in_wasm(error).map(|()| RemoteWorkflow {
        resource: child.unwrap(),
    });
    WORKFLOWS.get_mut(stub_id).set_value(result);
}

#[derive(Debug)]
pub struct RemoteWorkflow {
    resource: Resource<Self>,
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
