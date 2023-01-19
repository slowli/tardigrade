//! Async backends.

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
