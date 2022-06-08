//! Time-related futures.

use pin_project_lite::pin_project;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

#[cfg(all(target_arch = "wasm32", not(target_os = "emscripten")))]
mod imp {
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use tardigrade_shared::{FromAbi, TimerKind};

    #[derive(Debug)]
    pub struct Sleep(i64);

    impl Future for Sleep {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            #[link(wasm_import_module = "tardigrade_rt")]
            #[allow(improper_ctypes)]
            extern "C" {
                fn timer_poll(timer_ptr: i64, cx: *mut Context<'_>) -> i32;
            }

            unsafe {
                let result = timer_poll(self.0, cx);
                FromAbi::from_abi(result)
            }
        }
    }

    impl Drop for Sleep {
        fn drop(&mut self) {
            #[link(wasm_import_module = "tardigrade_rt")]
            extern "C" {
                fn timer_drop(timer_ptr: i64);
            }

            unsafe { timer_drop(self.0) }
        }
    }

    pub fn sleep(timer_name: &str, duration: Duration) -> Sleep {
        #[link(wasm_import_module = "tardigrade_rt")]
        extern "C" {
            fn timer_new(
                timer_name_ptr: *const u8,
                timer_name_len: usize,
                timer_kind: TimerKind,
                timer_value: i64,
            ) -> i64;
        }

        Sleep(unsafe {
            timer_new(
                timer_name.as_ptr(),
                timer_name.len(),
                TimerKind::Duration,
                i64::try_from(duration.as_millis()).expect("duration is too large"),
            )
        })
    }
}

#[cfg(not(all(target_arch = "wasm32", not(target_os = "emscripten"))))]
mod imp {
    use futures::channel::oneshot;
    use pin_project_lite::pin_project;

    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use crate::mock::Runtime;

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

    pub fn sleep(_timer_name: &str, duration: Duration) -> Sleep {
        Runtime::with_mut(|rt| Sleep {
            inner: rt.insert_timer(duration),
        })
    }
}

pin_project! {
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

pub fn sleep(timer_name: &str, duration: Duration) -> Sleep {
    Sleep {
        inner: imp::sleep(timer_name, duration),
    }
}
