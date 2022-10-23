//! WASM implementation of task APIs.

use futures::future::{Aborted, FutureExt, RemoteHandle, TryFutureExt};
use once_cell::unsync::Lazy;
use slab::Slab;

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use crate::{
    abi::{IntoWasm, PollTask, TryFromWasm},
    task::{JoinError, TaskError, TaskResult},
    TaskId, WakerId,
};

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
            #[link_name = "task::wake"]
            fn task_wake(task: TaskId);
        }

        unsafe { task_wake(self.0) }
    }
}

#[cold]
fn report_task_error(err: &TaskError) {
    #[link(wasm_import_module = "tardigrade_rt")]
    extern "C" {
        #[link_name = "task::report_error"]
        fn report_error(
            message_ptr: *const u8,
            message_len: usize,
            filename_ptr: *const u8,
            filename_len: usize,
            line: u32,
            column: u32,
        );
    }

    let message = err.cause().to_string();
    unsafe {
        report_error(
            message.as_ptr(),
            message.len(),
            err.location().filename.as_ptr(),
            err.location().filename.len(),
            err.location().line,
            err.location().column,
        );
    }

    for context in err.contexts() {
        unsafe {
            report_error(
                context.message().as_ptr(),
                context.message().len(),
                context.location().filename.as_ptr(),
                context.location().filename.len(),
                context.location().line,
                context.location().column,
            );
        }
    }
}

/// Handle to a spawned task / `PinnedFuture`.
#[derive(Debug)]
#[repr(transparent)]
pub struct RawTaskHandle(TaskId);

impl RawTaskHandle {
    fn new(future: impl Future<Output = ()> + 'static) -> Self {
        let task_id = unsafe { TASKS.insert(Some(Box::pin(future))) };
        Self(task_id)
    }

    pub(crate) fn for_main_task(task: impl Future<Output = TaskResult> + 'static) -> Self {
        let task = task.map(|res| {
            if let Err(err) = res {
                report_task_error(&err);
            }
        });
        Self::new(task)
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

    pub fn abort(&mut self) {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "task::abort"]
            fn task_abort(task: TaskId);
        }

        unsafe { task_abort(self.0) }
    }
}

impl Future for RawTaskHandle {
    type Output = Result<(), Aborted>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[link(wasm_import_module = "tardigrade_rt")]
        #[allow(improper_ctypes)]
        extern "C" {
            #[link_name = "task::poll_completion"]
            fn task_poll_completion(task: TaskId, cx: *mut Context<'_>) -> i64;
        }

        unsafe {
            let poll_result = task_poll_completion(self.0, cx);
            PollTask::from_abi_in_wasm(poll_result)
        }
    }
}

fn spawn_raw(task_name: &str, task: impl Future<Output = ()> + 'static) -> RawTaskHandle {
    #[link(wasm_import_module = "tardigrade_rt")]
    extern "C" {
        #[link_name = "task::spawn"]
        fn task_spawn(task_name_ptr: *const u8, task_name_len: usize, task: TaskId);
    }

    let handle = RawTaskHandle::new(task);
    unsafe { task_spawn(task_name.as_ptr(), task_name.len(), handle.0) };
    handle
}

#[derive(Debug)]
pub(super) struct JoinHandle {
    raw: Option<RawTaskHandle>,
    handle: Option<RemoteHandle<TaskResult>>,
}

impl JoinHandle {
    pub fn abort(&mut self) {
        if let Some(raw) = &mut self.raw {
            raw.abort();
        }
    }

    // Because we implement `Drop`, we cannot use `pin_project` macro, so we implement
    // field projections manually.
    fn project_raw(self: Pin<&mut Self>) -> &mut Option<RawTaskHandle> {
        // SAFETY: `raw` is never considered pinned
        unsafe { &mut self.get_unchecked_mut().raw }
    }

    fn project_handle(self: Pin<&mut Self>) -> Pin<&mut RemoteHandle<TaskResult>> {
        // SAFETY: map function is simple field access with always succeeding `unwrap()`,
        // which satisfies the `map_unchecked_mut` contract
        unsafe { self.map_unchecked_mut(|this| this.handle.as_mut().unwrap()) }
    }
}

impl Drop for JoinHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.forget(); // Prevent the task to be aborted
        }
    }
}

impl Future for JoinHandle {
    type Output = Result<(), JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(raw) = self.as_mut().project_raw() {
            match raw.poll_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Ready(Err(JoinError::Aborted)),
                Poll::Ready(Ok(())) => {
                    self.as_mut().project_raw().take();
                }
            }
        }
        self.project_handle().poll(cx).map_err(JoinError::Err)
    }
}

pub(super) fn spawn(
    task_name: &str,
    task: impl Future<Output = TaskResult> + 'static,
) -> JoinHandle {
    let (remote, handle) = task.inspect_err(report_task_error).remote_handle();
    JoinHandle {
        raw: Some(spawn_raw(task_name, remote)),
        handle: Some(handle),
    }
}

/// Polls the specified task.
///
/// # Safety
///
/// Calls to this method and `__drop_task` must be linearly ordered (no recursion).
#[no_mangle]
#[export_name = "tardigrade_rt::poll_task"]
#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "poll_task", skip_all, fields(task_id = task.0))
)]
pub extern "C" fn __tardigrade_rt__poll_task(task: RawTaskHandle) -> i32 {
    let waker = Waker::from(Arc::new(HostContext(task.0)));
    let mut cx = Context::from_waker(&waker);
    let poll_result = task.deref_and_poll(&mut cx);

    #[cfg(feature = "tracing")]
    tracing::info!(result = ?poll_result);

    TryFromWasm::into_abi_in_wasm(poll_result)
}

/// Drops the specified task.
///
/// # Safety
///
/// Calls to this method and `__poll_task` must be linearly ordered (no recursion).
#[no_mangle]
#[export_name = "tardigrade_rt::drop_task"]
#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "debug", name = "drop_task", skip_all, fields(task_id = task.0))
)]
pub extern "C" fn __tardigrade_rt__drop_task(task: RawTaskHandle) {
    task.drop_ref();
}

static mut WAKERS: Lazy<Registry<Waker>> = Lazy::new(|| {
    // 4 looks like a somewhat reasonable number of alive `Waker`s.
    Registry::with_capacity(4)
});

/// Equivalent of `cx.waker().clone()`.
#[no_mangle]
#[export_name = "tardigrade_rt::create_waker"]
#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "debug", name = "create_waker", skip(cx), ret)
)]
pub extern "C" fn __tardigrade_rt__create_waker(cx: *mut Context<'_>) -> WakerId {
    let cx = unsafe { &mut *cx };
    let waker = cx.waker().clone();
    unsafe { WAKERS.insert(waker) }
}

/// Equivalent of `waker.wake()`.
#[no_mangle]
#[export_name = "tardigrade_rt::wake_waker"]
#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "debug", name = "wake_waker")
)]
pub extern "C" fn __tardigrade_rt__wake_waker(waker: WakerId) {
    let waker = unsafe { WAKERS.remove(waker) };
    waker.wake();
}

/// Equivalent of `drop(waker)`.
#[no_mangle]
#[export_name = "tardigrade_rt::drop_waker"]
#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "debug", name = "drop_waker")
)]
pub extern "C" fn __tardigrade_rt__drop_waker(waker: WakerId) {
    unsafe { WAKERS.remove(waker) };
}
