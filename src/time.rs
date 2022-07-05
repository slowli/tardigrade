//! Time-related futures.

use pin_project_lite::pin_project;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

#[cfg(target_arch = "wasm32")]
mod imp {
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use tardigrade_shared::{abi::IntoWasm, TimerId, TimerKind};

    #[derive(Debug)]
    pub struct Sleep(TimerId);

    impl Future for Sleep {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            #[link(wasm_import_module = "tardigrade_rt")]
            #[allow(improper_ctypes)]
            extern "C" {
                #[link_name = "timer::poll"]
                fn timer_poll(timer_id: TimerId, cx: *mut Context<'_>) -> i32;
            }

            unsafe {
                let result = timer_poll(self.0, cx);
                IntoWasm::from_abi_in_wasm(result)
            }
        }
    }

    impl Drop for Sleep {
        fn drop(&mut self) {
            #[link(wasm_import_module = "tardigrade_rt")]
            extern "C" {
                #[link_name = "timer::drop"]
                fn timer_drop(timer_id: TimerId);
            }

            unsafe { timer_drop(self.0) }
        }
    }

    pub fn sleep(duration: Duration) -> Sleep {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            #[link_name = "timer::new"]
            fn timer_new(timer_kind: TimerKind, timer_value: i64) -> TimerId;
        }

        Sleep(unsafe {
            timer_new(
                TimerKind::Duration,
                i64::try_from(duration.as_millis()).expect("duration is too large"),
            )
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use futures::channel::oneshot;
    use pin_project_lite::pin_project;

    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use crate::test::Runtime;

    pin_project! {
        #[derive(Debug)]
        #[repr(transparent)]
        pub struct Sleep {
            #[pin]
            inner: oneshot::Receiver<()>,
        }
    }

    impl Future for Sleep {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.project()
                .inner
                .poll(cx)
                .map(|result| result.expect("runtime clock dropped before executor"))
        }
    }

    pub fn sleep(duration: Duration) -> Sleep {
        Runtime::with_mut(|rt| Sleep {
            inner: rt.insert_timer(duration),
        })
    }
}

pin_project! {
    /// Future returned by [`sleep`].
    #[derive(Debug)]
    #[repr(transparent)]
    pub struct Sleep {
        #[pin]
        inner: imp::Sleep,
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/// Sleeps for the specified `duration`.
pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        inner: imp::sleep(duration),
    }
}
