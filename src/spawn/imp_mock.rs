//! Mock implementations for spawning child workflows.

use pin_project_lite::pin_project;

use std::{
    borrow::Cow,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tardigrade_shared::interface::Interface;

use super::{ManageInterfaces, Workflows};
use crate::{
    error::HostError,
    task::{JoinError, JoinHandle},
    test::Runtime,
    workflow::{UntypedHandles, Wasm},
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

#[allow(clippy::unused_async)] // for symmetry with the wasm32 impl
pub(super) async fn new_workflow(
    definition_id: &str,
    args: Vec<u8>,
    handles: UntypedHandles<Wasm>,
) -> Result<super::RemoteWorkflow, HostError> {
    let main_task = Runtime::with_mut(|rt| rt.create_workflow(definition_id, args, handles));
    let main_task = JoinHandle::from_handle(main_task);
    Ok(super::RemoteWorkflow::from_main_task(main_task))
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
