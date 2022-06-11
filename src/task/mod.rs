//! Tasks.

use pin_project_lite::pin_project;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tardigrade_shared::JoinError;

#[cfg(all(target_arch = "wasm32", not(target_os = "emscripten")))]
#[path = "imp_wasm32.rs"]
pub(crate) mod imp;
#[cfg(not(all(target_arch = "wasm32", not(target_os = "emscripten"))))]
#[path = "imp_mock.rs"]
mod imp;

pin_project! {
    /// Handle to a spawned task.
    #[derive(Debug)]
    #[repr(transparent)]
    pub struct JoinHandle<T> {
        #[pin]
        inner: imp::JoinHandle<T>,
    }
}

impl<T> JoinHandle<T> {
    /// Aborts the task.
    pub fn abort(&mut self) {
        self.inner.abort();
    }
}

impl<T: 'static> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/// Spawns a new task.
pub fn spawn<T: 'static>(
    task_name: &str,
    task: impl Future<Output = T> + 'static,
) -> JoinHandle<T> {
    JoinHandle {
        inner: imp::spawn(task_name, task),
    }
}

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

    Yield { yielded: false }.await
}
