//! Mock implementations for spawning child workflows.

use futures::future::{self, BoxFuture, FutureExt};
use pin_project_lite::pin_project;

use std::{
    borrow::Cow,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::{ManageInterfaces, Workflows};
use crate::{
    error::HostError,
    interface::Interface,
    task::{JoinError, JoinHandle},
    test::Runtime,
    workflow::{Wasm, WithHandle, WorkflowFn},
    Codec,
};

impl ManageInterfaces for Workflows {
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        Runtime::with(|rt| {
            rt.workflow_registry()
                .interface(definition_id)
                .map(|interface| Cow::Owned(interface.clone()))
        })
    }
}

pub(super) fn new_workflow<W: WorkflowFn + WithHandle>(
    definition_id: &str,
    args: W::Args,
    handles: W::Handle<Wasm>,
) -> BoxFuture<'static, Result<super::RemoteWorkflow, HostError>> {
    let raw_args = <W::Codec>::encode_value(args);
    let raw_handles = W::into_untyped(handles);
    let main_task =
        Runtime::with_mut(|rt| rt.create_workflow(definition_id, raw_args, raw_handles));
    let main_task = JoinHandle::from_handle(main_task);
    future::ok(super::RemoteWorkflow::from_main_task(main_task)).boxed()
}

pin_project! {
    #[derive(Debug)]
    pub(super) struct RemoteWorkflow {
        #[pin]
        main_task: JoinHandle,
    }
}

impl super::RemoteWorkflow {
    fn from_main_task(main_task: JoinHandle) -> Self {
        Self {
            inner: RemoteWorkflow { main_task },
        }
    }
}

impl Future for RemoteWorkflow {
    type Output = Result<(), JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().main_task.poll(cx)
    }
}
