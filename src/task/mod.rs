//! Tasks.

use futures::FutureExt;
use pin_project_lite::pin_project;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(target_arch = "wasm32")]
#[path = "imp_wasm32.rs"]
pub(crate) mod imp;
#[cfg(not(target_arch = "wasm32"))]
#[path = "imp_mock.rs"]
mod imp;

pub use tardigrade_shared::{ErrorLocation, JoinError, TaskError, TaskResult};

pin_project! {
    /// Handle to a spawned task.
    ///
    /// The handle can be used to abort the task, or to wait for task completion.
    #[derive(Debug)]
    pub struct JoinHandle {
        #[pin]
        inner: imp::JoinHandle,
    }
}

impl JoinHandle {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn from_handle(handle: futures::future::RemoteHandle<TaskResult>) -> Self {
        Self {
            inner: imp::JoinHandle::from_handle(handle),
        }
    }

    /// Aborts the task.
    pub fn abort(&mut self) {
        self.inner.abort();
    }
}

impl Future for JoinHandle {
    type Output = Result<(), JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/// Spawns a new task and returns a handle that can be used to wait for its completion
/// or abort the task.
pub fn spawn(task_name: &str, task: impl Future<Output = ()> + 'static) -> JoinHandle {
    try_spawn(task_name, task.map(Ok))
}

/// Spawns a new fallible task and returns a handle that can be used to wait for its completion
/// or abort the task.
pub fn try_spawn(task_name: &str, task: impl Future<Output = TaskResult> + 'static) -> JoinHandle {
    JoinHandle {
        inner: imp::spawn(task_name, task),
    }
}

/// Yields execution of the current task, allowing to switch to other tasks.
pub async fn yield_now() {
    #[derive(Debug)]
    struct Yield {
        yielded: bool,
    }

    impl Future for Yield {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    Yield { yielded: false }.await;
}
