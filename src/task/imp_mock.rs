//! Mock implementation of task APIs.

use futures::future::RemoteHandle;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    task::{JoinError, TaskResult},
    test::Runtime,
};

#[derive(Debug)]
pub(super) struct JoinHandle(Option<RemoteHandle<TaskResult>>);

impl JoinHandle {
    pub fn from_handle(handle: RemoteHandle<TaskResult>) -> Self {
        Self(Some(handle))
    }

    pub fn abort(&mut self) {
        self.0.take(); // drops the handle, thus aborting the task.
    }

    fn project(self: Pin<&mut Self>) -> Option<Pin<&mut RemoteHandle<TaskResult>>> {
        Some(Pin::new(self.get_mut().0.as_mut()?))
    }
}

impl Drop for JoinHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.forget();
        }
    }
}

impl Future for JoinHandle {
    type Output = Result<(), JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(handle) = self.project() {
            handle.poll(cx).map_err(JoinError::Err)
        } else {
            Poll::Ready(Err(JoinError::Aborted))
        }
    }
}

pub(super) fn spawn(
    _task_name: &str,
    task: impl Future<Output = TaskResult> + 'static,
) -> JoinHandle {
    let handle = Runtime::with_mut(|rt| rt.spawn_task(task));
    JoinHandle::from_handle(handle)
}
