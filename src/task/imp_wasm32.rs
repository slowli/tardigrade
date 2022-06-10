//! WASM implementation of task APIs.

use futures::future::{FutureExt, RemoteHandle};
use once_cell::unsync::Lazy;
use pin_project_lite::pin_project;
use slab::Slab;

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use tardigrade_shared::{FromAbi, JoinError, TaskId, WakerId};

/// Container similar to `Slab`, but with guarantee that returned item keys are never reused.
#[derive(Debug)]
struct Registry<T> {
    items: Slab<T>,
    item_counter: u32,
}

impl<T> Registry<T> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            items: Slab::with_capacity(capacity),
            item_counter: 0,
        }
    }

    fn insert(&mut self, value: T) -> u64 {
        let slab_key = self.items.insert(value);
        let item_counter = self.item_counter;
        self.item_counter += 1;
        (u64::from(item_counter) << 32) + u64::try_from(slab_key).unwrap()
    }

    fn get_mut(&mut self, key: u64) -> &mut T {
        let slab_key = (key & 0x_ffff_ffff) as usize;
        self.items
            .get_mut(slab_key)
            .expect("registry item not found")
    }

    fn remove(&mut self, key: u64) -> T {
        let slab_key = (key & 0x_ffff_ffff) as usize;
        self.items.remove(slab_key)
    }
}

type PinnedFuture = Pin<Box<dyn Future<Output = ()>>>;

static mut TASKS: Lazy<Registry<Option<PinnedFuture>>> = Lazy::new(|| {
    // Pre-allocate capacity for the main task.
    Registry::with_capacity(1)
});

/// [`Context`](core::task::Context) analogue connected to the host env.
///
/// Internally, the context is the ID of the currently executing task.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct HostContext(TaskId);

impl Wake for HostContext {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            fn task_wake(task: TaskId);
        }

        unsafe { task_wake(self.0) }
    }
}

/// Handle to a spawned task / `PinnedFuture`.
#[derive(Debug)]
#[repr(transparent)]
pub struct RawTaskHandle(TaskId);

impl RawTaskHandle {
    pub fn new(future: impl Future<Output = ()> + 'static) -> Self {
        let task_id = unsafe { TASKS.insert(Some(Box::pin(future))) };
        Self(task_id)
    }

    fn deref_and_poll(self, cx: &mut Context<'_>) -> Poll<()> {
        // We need to temporarily remove the future from the registry so that it can
        // be mutated independently (other registry tasks may be polled while the task is
        // being polled).
        let task = unsafe { TASKS.get_mut(self.0).take() };
        let mut task = task.expect("task attempted to poll itself");
        let poll_result = task.as_mut().poll(cx);
        unsafe {
            *TASKS.get_mut(self.0) = Some(task);
        }
        poll_result
    }

    fn drop_ref(self) {
        unsafe {
            TASKS.remove(self.0);
        }
    }

    fn abort(&self) {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            fn task_schedule_abortion(task: TaskId);
        }

        unsafe { task_schedule_abortion(self.0) }
    }
}

impl Future for RawTaskHandle {
    type Output = Result<(), JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[link(wasm_import_module = "tardigrade_rt")]
        #[allow(improper_ctypes)]
        extern "C" {
            fn task_poll_completion(task: TaskId, cx: *mut Context<'_>) -> i64;
        }

        unsafe {
            let poll_result = task_poll_completion(self.0, cx);
            FromAbi::from_abi(poll_result)
        }
    }
}

fn spawn_raw(task_name: &str, task: impl Future<Output = ()> + 'static) -> RawTaskHandle {
    #[link(wasm_import_module = "tardigrade_rt")]
    extern "C" {
        fn task_spawn(task_name_ptr: *const u8, task_name_len: usize, task: TaskId);
    }

    let handle = RawTaskHandle::new(task);
    unsafe { task_spawn(task_name.as_ptr(), task_name.len(), handle.0) };
    handle
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
pub extern "C" fn __tardigrade_rt__poll_task(task: RawTaskHandle, cx: HostContext) -> bool {
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
pub extern "C" fn __tardigrade_rt__drop_task(task: RawTaskHandle) {
    task.drop_ref();
}

static mut WAKERS: Lazy<Registry<Waker>> = Lazy::new(|| {
    // 4 looks like a somewhat reasonable number of alive `Waker`s.
    Registry::with_capacity(4)
});

/// Equivalent of `cx.waker().clone()`.
#[no_mangle]
pub extern "C" fn __tardigrade_rt__context_create_waker(cx: *mut Context<'_>) -> WakerId {
    let cx = unsafe { &mut *cx };
    let waker = cx.waker().clone();
    unsafe { WAKERS.insert(waker) }
}

/// Equivalent of `waker.wake()`.
#[no_mangle]
pub extern "C" fn __tardigrade_rt__waker_wake(waker: WakerId) {
    let waker = unsafe { WAKERS.remove(waker) };
    waker.wake();
}
