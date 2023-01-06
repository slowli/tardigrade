//! Async backends.

#[cfg(any(test, feature = "test"))]
mod mock {
    use chrono::{DateTime, Utc};
    use futures::{channel::mpsc, Stream};

    use std::{
        ops,
        sync::{Arc, Mutex},
    };

    use crate::{Clock, Schedule, TimerFuture};
    use tardigrade::test::MockScheduler as SchedulerBase;

    /// Mock [wall clock](Clock) and [scheduler](Schedule).
    ///
    /// # Examples
    ///
    /// A primary use case is to use the scheduler with a [`WorkflowManager`]
    /// for integration testing:
    ///
    /// [`WorkflowManager`]: crate::manager::WorkflowManager
    ///
    /// ```
    /// # use async_std::task;
    /// # use futures::StreamExt;
    /// # use std::sync::Arc;
    /// #
    /// # use tardigrade::{handle::{SenderAt, WithIndexing}, spawn::CreateWorkflow};
    /// # use tardigrade_rt::{
    /// #     engine::{Wasmtime, WasmtimeModule},
    /// #     manager::{DriveConfig, WorkflowManager},
    /// #     storage::{LocalStorage, Streaming},
    /// #     handle::WorkflowHandle, test::MockScheduler,
    /// # };
    /// # async fn test_wrapper(module: WasmtimeModule) -> anyhow::Result<()> {
    /// // We need `Streaming` storage to drive a `WorkflowManager`.
    /// let storage = Arc::new(LocalStorage::default());
    /// let (mut storage, storage_task) = Streaming::new(storage);
    /// let mut commits_rx = storage.stream_commits();
    /// task::spawn(storage_task);
    ///
    /// // Set the mocked wall clock for the workflow manager.
    /// let scheduler = MockScheduler::default();
    /// let manager = WorkflowManager::builder(Wasmtime::default(), storage)
    ///     .with_clock(scheduler.clone())
    ///     .build();
    /// let manager = Arc::new(manager); // to simplify handle management
    ///
    /// let inputs: Vec<u8> = // ...
    /// #   vec![];
    /// let spawner = manager.spawner();
    /// let builder = spawner.new_workflow::<()>("test::Workflow").await?;
    /// let (handles, self_handles) = builder.handles(|_| {}).await;
    /// builder.build(inputs, handles).await?;
    ///
    /// // Spin up the driver to execute the `workflow`.
    /// let mut self_handles = self_handles.with_indexing();
    /// let events_rx = self_handles.remove(SenderAt("events")).unwrap();
    /// let mut events_rx = events_rx.stream_messages(0..);
    ///
    /// let manager = manager.clone();
    /// task::spawn(async move {
    ///     manager.drive(&mut commits_rx, DriveConfig::new()).await
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
}

#[cfg(any(test, feature = "test"))]
pub use self::mock::MockScheduler;

#[cfg(feature = "async-io")]
mod async_io {
    use async_io::Timer;
    use chrono::{DateTime, Utc};
    use futures::{future, FutureExt};

    use std::time::{Instant, SystemTime};

    use crate::{Clock, Schedule, TimerFuture};

    /// [Scheduler](Schedule) implementation from [`async-io`] (a part of [`async-std`] suite).
    ///
    /// [`async-io`]: https://docs.rs/async-io/
    /// [`async-std`]: https://docs.rs/async-std/
    #[cfg_attr(docsrs, doc(cfg(feature = "async-io")))]
    #[derive(Debug)]
    pub struct AsyncIoScheduler;

    impl Clock for AsyncIoScheduler {
        fn now(&self) -> DateTime<Utc> {
            Utc::now()
        }
    }

    impl Schedule for AsyncIoScheduler {
        fn create_timer(&self, timestamp: DateTime<Utc>) -> TimerFuture {
            let timestamp = SystemTime::from(timestamp);
            let (now_instant, now) = (Instant::now(), SystemTime::now());
            match timestamp.duration_since(now) {
                Ok(diff) => {
                    let timer = Timer::at(now_instant + diff);
                    let timer = FutureExt::map(timer, move |instant| {
                        let new_time = now + (instant - now_instant);
                        new_time.into()
                    });
                    Box::pin(timer)
                }
                Err(_) => Box::pin(future::ready(now.into())),
            }
        }
    }
}

#[cfg(feature = "async-io")]
pub use self::async_io::AsyncIoScheduler;

#[cfg(feature = "tokio")]
mod tokio {
    use chrono::{DateTime, Utc};
    use futures::{future, FutureExt};
    use tokio::time::{sleep_until, Instant};

    use std::time::SystemTime;

    use crate::{Clock, Schedule, TimerFuture};

    /// [Scheduler](Schedule) implementation from [`tokio`].
    ///
    /// [`tokio`]: https://docs.rs/tokio/
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    #[derive(Debug)]
    pub struct TokioScheduler;

    impl Clock for TokioScheduler {
        fn now(&self) -> DateTime<Utc> {
            Utc::now()
        }
    }

    impl Schedule for TokioScheduler {
        fn create_timer(&self, timestamp: DateTime<Utc>) -> TimerFuture {
            let timestamp = SystemTime::from(timestamp);
            let (now_instant, now) = (Instant::now(), SystemTime::now());
            match timestamp.duration_since(now) {
                Ok(diff) => {
                    let timer = sleep_until(now_instant + diff);
                    let timer = timer.map(move |()| {
                        let instant = Instant::now();
                        let new_time = now + (instant - now_instant);
                        new_time.into()
                    });
                    Box::pin(timer)
                }
                Err(_) => Box::pin(future::ready(now.into())),
            }
        }
    }
}

#[cfg(feature = "tokio")]
pub use self::tokio::TokioScheduler;
