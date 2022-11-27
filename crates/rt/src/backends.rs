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
    /// A primary use case is to use the scheduler with a [`Driver`] for integration testing:
    ///
    /// [`Driver`]: crate::driver::Driver
    ///
    /// ```
    /// # use async_std::task;
    /// # use futures::TryStreamExt;
    /// # use tardigrade::{interface::SenderName, spawn::ManageWorkflowsExt};
    /// # use tardigrade_rt::{
    /// #     driver::Driver, manager::{WorkflowHandle, WorkflowManager}, storage::LocalStorage,
    /// #     test::MockScheduler, WorkflowModule,
    /// # };
    /// # async fn test_wrapper(module: WorkflowModule) -> anyhow::Result<()> {
    /// let scheduler = MockScheduler::default();
    /// // Set the mocked wall clock for the workflow manager.
    /// let storage = LocalStorage::default();
    /// let mut manager = WorkflowManager::builder(storage)
    ///     .with_clock(scheduler.clone())
    ///     .build()
    ///     .await?;
    /// let inputs: Vec<u8> = // ...
    /// #   vec![];
    /// let mut workflow = manager
    ///     .new_workflow::<()>("test::Workflow", inputs)?
    ///     .build()
    ///     .await?;
    ///
    /// // Spin up the driver to execute the `workflow`.
    /// let mut driver = Driver::new();
    /// let mut handle = workflow.handle();
    /// let mut events_rx = handle.remove(SenderName("events"))
    ///     .unwrap()
    ///     .into_stream(&mut driver);
    /// task::spawn(async move { driver.drive(&mut manager).await });
    ///
    /// // Advance mocked wall clock.
    /// let now = scheduler.now();
    /// scheduler.set_now(now + chrono::Duration::seconds(1));
    /// // This can lead to the workflow progressing, e.g., by emitting messages
    /// let message: Option<Vec<u8>> = events_rx.try_next().await?;
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
