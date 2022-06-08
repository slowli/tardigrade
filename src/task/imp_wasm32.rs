//! WASM implementation of task APIs.

use futures::future::{FutureExt, RemoteHandle};
use pin_project_lite::pin_project;

use std::{
    future::Future,
    mem,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use tardigrade_shared::{FromAbi, JoinError};

type PinnedFuture = Pin<Box<dyn Future<Output = ()>>>;

/// Pointer to a task (i.e., a top-level `PinnedFuture`).
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct TaskPointer(i64);

impl TaskPointer {
    pub fn new(task: impl Future<Output = ()> + 'static) -> Self {
        let mut task = Box::pin(task) as PinnedFuture;
        let int_ptr = unsafe {
            // SAFETY: `task_ref` is not used to violate `Pin` invariants here
            // or in the methods below.
            let task_ptr = task.as_mut().get_unchecked_mut() as *mut dyn Future<Output = ()>;
            // `i64` has no special cases, hence this transform is safe as well.
            mem::transmute(task_ptr)
        };
        mem::forget(task); // The task will be dropped via `drop_task`
        Self(int_ptr)
    }

    fn deref_and_poll(&self, cx: &mut Context<'_>) -> Poll<()> {
        let task = unsafe {
            // SAFETY: Transmute is safe because of usage; we only ever convert pointers
            // obtained via `TaskPointer::new`. For same reason, deref and wrapping into `Pin`
            // are safe as well.
            let task_ptr: *mut dyn Future<Output = ()> = mem::transmute(self.0);
            Pin::new_unchecked(&mut *task_ptr)
        };
        task.poll(cx)
    }

    fn drop_task(self) {
        unsafe {
            // SAFETY: Transmute is safe because of usage; we only ever convert pointers
            // obtained via `TaskPointer::new`. For same reason, converting into `Box`
            // is safe as well.
            let task_ptr: *mut dyn Future<Output = ()> = mem::transmute(self.0);
            drop(Box::from_raw(task_ptr));
        }
    }
}

/// [`Context`](core::task::Context) analogue connected to the host env.
///
/// Internally, the context is a pointer to the currently executing task. This pointer is
/// never dereferenced and is only used to wake the task.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct HostContext(TaskPointer);

impl Wake for HostContext {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            fn task_wake(task: TaskPointer);
        }

        unsafe { task_wake(self.0) }
    }
}

#[derive(Debug)]
struct RawTaskHandle {
    ptr: TaskPointer,
}

impl RawTaskHandle {
    fn abort(&self) {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            fn task_schedule_abortion(task: TaskPointer);
        }

        unsafe { task_schedule_abortion(self.ptr) }
    }
}

impl Future for RawTaskHandle {
    type Output = Result<(), JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[link(wasm_import_module = "tardigrade_rt")]
        #[allow(improper_ctypes)]
        extern "C" {
            fn task_poll_completion(task: TaskPointer, cx: *mut Context<'_>) -> i64;
        }

        unsafe {
            let poll_result = task_poll_completion(self.ptr, cx);
            FromAbi::from_abi(poll_result)
        }
    }
}

fn spawn_raw(task_name: &str, task: impl Future<Output = ()> + 'static) -> RawTaskHandle {
    #[link(wasm_import_module = "tardigrade_rt")]
    extern "C" {
        fn task_spawn(task_name_ptr: *const u8, task_name_len: usize, task: TaskPointer);
    }

    let task_ptr = TaskPointer::new(task);
    unsafe { task_spawn(task_name.as_ptr(), task_name.len(), task_ptr) };
    RawTaskHandle { ptr: task_ptr }
}

pin_project! {
    #[derive(Debug)]
    pub(super) struct JoinHandle<T> {
        raw: Option<RawTaskHandle>,
        #[pin]
        handle: RemoteHandle<T>,
    }
}

impl<T> JoinHandle<T> {
    pub fn abort(&mut self) {
        if let Some(raw) = &self.raw {
            raw.abort();
        }
    }
}

impl<T: 'static> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let projection = self.project();
        if let Some(raw) = projection.raw {
            match raw.poll_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => {
                    projection.raw.take();
                }
            }
        }
        projection.handle.poll(cx).map(Ok)
    }
}

pub(super) fn spawn<T: 'static>(
    task_name: &str,
    task: impl Future<Output = T> + 'static,
) -> JoinHandle<T> {
    let (remote, handle) = task.remote_handle();
    JoinHandle {
        raw: Some(spawn_raw(task_name, remote)),
        handle,
    }
}

/// Polls the specified task.
///
/// # Safety
///
/// Calls to this method and `__drop_task` must be linearly ordered (no recursion).
#[no_mangle]
pub extern "C" fn __tardigrade_rt__poll_task(task: TaskPointer, cx: HostContext) -> bool {
    let waker = Waker::from(Arc::new(cx));
    let mut cx = Context::from_waker(&waker);
    match task.deref_and_poll(&mut cx) {
        Poll::Pending => false,
        Poll::Ready(()) => true,
    }
}

/// Drops the specified task.
///
/// # Safety
///
/// Calls to this method and `__poll_task` must be linearly ordered (no recursion).
#[no_mangle]
pub extern "C" fn __tardigrade_rt__drop_task(task: TaskPointer) {
    task.drop_task();
}

/// Equivalent of `cx.waker().clone()`. The returned waker is owned by the host.
#[no_mangle]
pub extern "C" fn __tardigrade_rt__context_create_waker(cx: *mut Context<'_>) -> *mut Waker {
    let cx = unsafe { &mut *cx };
    let waker = Box::new(cx.waker().clone());
    Box::leak(waker)
}

/// Equivalent of `waker.wake()`.
#[no_mangle]
pub extern "C" fn __tardigrade_rt__waker_wake(waker: *mut Waker) {
    let waker = unsafe { Box::from_raw(waker) };
    waker.wake();
}
