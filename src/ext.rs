//! `Future` extensions.

use pin_project_lite::pin_project;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tardigrade_shared::{FutureId, IntoAbiOnStack, TracedFutureUpdate};

pub trait FutureExt: Sized {
    fn trace(self, description: impl AsRef<str>) -> Traced<Self>;
}

impl<T: Future> FutureExt for T {
    fn trace(self, description: impl AsRef<str>) -> Traced<Self> {
        Traced::new(self, description.as_ref())
    }
}

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "tardigrade_rt")]
extern "C" {
    fn traced_future_update(future_id: FutureId, update: i32);
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn traced_future_update(_future_id: FutureId, _update: i32) {
    // Does nothing
}

#[derive(Debug)]
struct TracedFutureHandle(FutureId);

impl TracedFutureHandle {
    pub fn new(description: &str) -> Self {
        #[cfg(target_arch = "wasm32")]
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            fn traced_future_new(description_ptr: *const u8, description_len: usize) -> FutureId;
        }

        #[cfg(not(target_arch = "wasm32"))]
        unsafe fn traced_future_new(
            _description_ptr: *const u8,
            _description_len: usize,
        ) -> FutureId {
            0 // This ID is never used, so it is safe to return a constant
        }

        let id = unsafe { traced_future_new(description.as_ptr(), description.len()) };
        Self(id)
    }

    pub fn trace_polling(&self, update: TracedFutureUpdate) {
        unsafe {
            traced_future_update(self.0, update.into_abi());
        }
    }
}

impl Drop for TracedFutureHandle {
    fn drop(&mut self) {
        unsafe {
            traced_future_update(self.0, TracedFutureUpdate::Dropped.into_abi());
        }
    }
}

pin_project! {
    #[derive(Debug)]
    pub struct Traced<F> {
        id: TracedFutureHandle,
        #[pin]
        inner: F,
    }
}

impl<F: Future> Traced<F> {
    fn new(inner: F, description: &str) -> Self {
        let id = TracedFutureHandle::new(description);
        Self { id, inner }
    }
}

impl<F: Future> Future for Traced<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let projection = self.project();
        projection.id.trace_polling(TracedFutureUpdate::Polling);
        let poll_result = projection.inner.poll(cx);
        let update = TracedFutureUpdate::Polled(if poll_result.is_ready() {
            Poll::Ready(())
        } else {
            Poll::Pending
        });
        projection.id.trace_polling(update);
        poll_result
    }
}
