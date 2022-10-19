//! Time-related futures.

use chrono::{DateTime, Utc};
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

#[cfg(target_arch = "wasm32")]
mod imp {
    use chrono::{DateTime, TimeZone, Utc};

    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    use crate::{abi::IntoWasm, TimerId};

    #[derive(Debug)]
    pub struct Timer(TimerId);

    impl Timer {
        pub(super) fn at(timestamp: DateTime<Utc>) -> Self {
            #[link(wasm_import_module = "tardigrade_rt")]
            extern "C" {
                #[link_name = "timer::new"]
                fn timer_new(timestamp_millis: i64) -> TimerId;
            }

            Self(unsafe { timer_new(timestamp.timestamp_millis()) })
        }
    }

    impl Future for Timer {
        type Output = DateTime<Utc>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            #[link(wasm_import_module = "tardigrade_rt")]
            #[allow(improper_ctypes)]
            extern "C" {
                #[link_name = "timer::poll"]
                fn timer_poll(timer_id: TimerId, cx: *mut Context<'_>) -> i64;
            }

            unsafe {
                let result = timer_poll(self.0, cx);
                IntoWasm::from_abi_in_wasm(result)
            }
        }
    }

    impl Drop for Timer {
        fn drop(&mut self) {
            #[link(wasm_import_module = "tardigrade_rt")]
            extern "C" {
                #[link_name = "timer::drop"]
                fn timer_drop(timer_id: TimerId);
            }

            unsafe { timer_drop(self.0) }
        }
    }

    pub(super) fn now() -> DateTime<Utc> {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "timer::now"]
            fn timer_now() -> i64;
        }

        Utc.timestamp_millis(unsafe { timer_now() })
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use chrono::{DateTime, Utc};
    use futures::channel::oneshot;
    use pin_project_lite::pin_project;

    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    use crate::test::Runtime;

    pin_project! {
        #[derive(Debug)]
        #[repr(transparent)]
        pub struct Timer {
            #[pin]
            inner: oneshot::Receiver<DateTime<Utc>>,
        }
    }

    impl Timer {
        pub(super) fn at(timestamp: DateTime<Utc>) -> Self {
            Runtime::with_mut(|rt| Self {
                inner: rt.scheduler().insert_timer(timestamp),
            })
        }
    }

    impl Future for Timer {
        type Output = DateTime<Utc>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.project()
                .inner
                .poll(cx)
                .map(|result| result.expect("runtime clock dropped before executor"))
        }
    }

    pub(super) fn now() -> DateTime<Utc> {
        Runtime::with_mut(|rt| rt.scheduler().now())
    }
}

/// Definition of a timer used by a workflow.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimerDefinition {
    /// Expiration timestamp of the timer.
    pub expires_at: DateTime<Utc>,
}

pin_project! {
    /// A future emitting time events.
    #[derive(Debug)]
    #[repr(transparent)]
    pub struct Timer {
        #[pin]
        inner: imp::Timer,
    }
}

impl Timer {
    /// Creates a timer that wakes up after the specified `duration`.
    ///
    /// # Panics
    ///
    /// Panics if `duration` cannot be represented as `chrono::Duration` (shouldn't normally
    /// happen).
    pub fn after(duration: Duration) -> Self {
        let duration = chrono::Duration::from_std(duration).expect("duration overflow");
        Self::at(now() + duration)
    }

    /// Creates a timer that wakes up at the specified `timestamp`.
    pub fn at(timestamp: DateTime<Utc>) -> Self {
        Self {
            inner: imp::Timer::at(timestamp),
        }
    }
}

impl Future for Timer {
    type Output = DateTime<Utc>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/// Returns the current timestamp as per the wall clock associated with the workflow.
pub fn now() -> DateTime<Utc> {
    imp::now()
}

/// Sleeps for the specified `duration`.
pub fn sleep(duration: Duration) -> Timer {
    Timer::after(duration)
}
