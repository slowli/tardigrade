//! Mock implementation of task APIs.

use futures::future::RemoteHandle;

use std::{
    future::Future,
    panic::{self, AssertUnwindSafe},
    pin::Pin,
    task::{Context, Poll},
};

use crate::test::Runtime;
use tardigrade_shared::JoinError;

#[derive(Debug)]
pub struct JoinHandle<T>(Option<RemoteHandle<T>>);

impl<T> JoinHandle<T> {
    pub fn abort(&mut self) {
        self.0.take(); // drops the handle, thus aborting the task.
    }

    fn project(self: Pin<&mut Self>) -> Option<Pin<&mut RemoteHandle<T>>> {
        Some(Pin::new(self.get_mut().0.as_mut()?))
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.forget();
        }
    }
}

impl<T: 'static> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(handle) = self.project() {
            panic::catch_unwind(AssertUnwindSafe(|| handle.poll(cx))).map_or_else(
                |_| Poll::Ready(Err(JoinError::Trapped)),
                |poll| poll.map(Ok),
            )
        } else {
            Poll::Ready(Err(JoinError::Aborted))
        }
    }
}

pub fn spawn<T: 'static>(
    _task_name: &str,
    task: impl Future<Output = T> + 'static,
) -> JoinHandle<T> {
    let handle = Runtime::with_mut(|rt| rt.spawn_task(task));
    JoinHandle(Some(handle))
}
