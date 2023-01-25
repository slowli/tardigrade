//! Helpers for workflow integration testing.
//!
//! # Examples
//!
//! Typically, it is useful to cache a [`WorkflowModule`](crate::engine::WorkflowModule)
//! among multiple tests. This can be performed as follows:
//!
//! ```no_run
//! use async_std::task;
//! use once_cell::sync::Lazy;
//! use tardigrade_rt::{test::*, engine::{Wasmtime, WasmtimeModule, WorkflowEngine}};
//!
//! static MODULE: Lazy<WasmtimeModule> = Lazy::new(|| {
//!     let module_bytes = ModuleCompiler::new(env!("CARGO_PKG_NAME"))
//!         .set_current_dir(env!("CARGO_MANIFEST_DIR"))
//!         .set_profile("wasm")
//!         .set_wasm_opt(WasmOpt::default())
//!         .compile();
//!     let engine = Wasmtime::default();
//!     let task = engine.create_module(module_bytes.into());
//!     task::block_on(task).unwrap()
//! });
//! // The module can then be used in tests
//! ```

use chrono::{DateTime, Utc};
use futures::{channel::mpsc, Stream};

use std::{
    ops,
    sync::{Arc, Mutex},
};

#[cfg(feature = "test")]
mod compiler;
#[doc(hidden)] // not baked for external use yet
pub mod engine;

#[cfg(feature = "test")]
pub use self::compiler::{ModuleCompiler, WasmOpt};

use crate::{Clock, Schedule, TimerFuture};
use tardigrade::test::MockScheduler as SchedulerBase;

/// Mock [wall clock](Clock) and [scheduler](Schedule).
///
/// # Examples
///
/// A primary use case is to use the scheduler with a [`Runtime`]
/// for integration testing:
///
/// [`Runtime`]: crate::runtime::Runtime
///
/// ```
/// # use async_std::task;
/// # use futures::StreamExt;
/// # use std::sync::Arc;
/// #
/// # use tardigrade::{handle::{SenderAt, WithIndexing}, spawn::CreateWorkflow};
/// # use tardigrade_rt::{
/// #     engine::{Wasmtime, WasmtimeModule},
/// #     runtime::{DriveConfig, Runtime},
/// #     storage::{LocalStorage, Streaming},
/// #     handle::WorkflowHandle, test::MockScheduler,
/// # };
/// # async fn test_wrapper(module: WasmtimeModule) -> anyhow::Result<()> {
/// // We need `Streaming` storage to drive a `Runtime`.
/// let storage = Arc::new(LocalStorage::default());
/// let (mut storage, storage_task) = Streaming::new(storage);
/// let mut commits_rx = storage.stream_commits();
/// task::spawn(storage_task);
///
/// // Set the mocked wall clock for the workflow runtime.
/// let scheduler = MockScheduler::default();
/// let runtime = Runtime::builder(Wasmtime::default(), storage)
///     .with_clock(scheduler.clone())
///     .build();
///
/// let inputs: Vec<u8> = // ...
/// #   vec![];
/// let spawner = runtime.spawner();
/// let builder = spawner.new_workflow::<()>("test::Workflow").await?;
/// let (handles, self_handles) = builder.handles(|_| {}).await;
/// builder.build(inputs, handles).await?;
///
/// // Spin up the driver to execute the `workflow`.
/// let mut self_handles = self_handles.with_indexing();
/// let events_rx = self_handles.remove(SenderAt("events")).unwrap();
/// let mut events_rx = events_rx.stream_messages(0..);
///
/// let runtime = runtime.clone();
/// task::spawn(async move {
///     runtime.drive(&mut commits_rx, DriveConfig::new()).await
/// });
///
/// // Advance mocked wall clock.
/// let now = scheduler.now();
/// scheduler.set_now(now + chrono::Duration::seconds(1));
/// // This can lead to the workflow progressing, e.g., by emitting messages
/// let message = events_rx.next().await.unwrap();
/// // Assert on `message`...
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct MockScheduler {
    inner: Arc<Mutex<SchedulerBase>>,
    new_expirations_sx: mpsc::UnboundedSender<DateTime<Utc>>,
}

impl Default for MockScheduler {
    fn default() -> Self {
        Self {
            inner: Arc::default(),
            new_expirations_sx: mpsc::unbounded().0,
        }
    }
}

#[cfg_attr(test, allow(dead_code))] // some pub methods are not used in tests
impl MockScheduler {
    /// Creates a mock scheduler together with a stream that notifies the consumer
    /// about new timer expirations.
    pub fn with_expirations() -> (Self, impl Stream<Item = DateTime<Utc>> + Unpin) {
        let (new_expirations_sx, rx) = mpsc::unbounded();
        let this = Self {
            inner: Arc::default(),
            new_expirations_sx,
        };
        (this, rx)
    }

    fn inner(&self) -> impl ops::DerefMut<Target = SchedulerBase> + '_ {
        self.inner.lock().unwrap()
    }

    /// Returns the expiration for the nearest timer, or `None` if there are no active timers.
    pub fn next_timer_expiration(&self) -> Option<DateTime<Utc>> {
        self.inner().next_timer_expiration()
    }

    /// Returns the current timestamp.
    pub fn now(&self) -> DateTime<Utc> {
        self.inner().now()
    }

    /// Sets the current timestamp for the scheduler.
    pub fn set_now(&self, now: DateTime<Utc>) {
        self.inner().set_now(now);
    }
}

impl Clock for MockScheduler {
    fn now(&self) -> DateTime<Utc> {
        self.now()
    }
}

impl Schedule for MockScheduler {
    fn create_timer(&self, expires_at: DateTime<Utc>) -> TimerFuture {
        use futures::{future, FutureExt};

        let mut guard = self.inner();
        let now = guard.now();
        if now >= expires_at {
            Box::pin(future::ready(now))
        } else {
            self.new_expirations_sx.unbounded_send(expires_at).ok();
            Box::pin(guard.insert_timer(expires_at).then(|res| match res {
                Ok(timestamp) => future::ready(timestamp).left_future(),
                Err(_) => future::pending().right_future(),
                // ^ An error can occur when the mock scheduler is dropped, usually at the end
                // of a test. In this case the timer never expires.
            }))
        }
    }
}
