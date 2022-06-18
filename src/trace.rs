//! Tracing.

use futures::{FutureExt, SinkExt};

use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
    thread,
};

pub use tardigrade_shared::trace::{
    FutureState, FutureUpdate, FutureUpdateError, FutureUpdateKind, TracedFuture, TracedFutures,
};

use crate::{channel::Sender, Encoder, Wasm};
use tardigrade_shared::{
    workflow::{Interface, InterfaceErrors, TakeHandle, ValidateInterface},
    ChannelErrorKind, ChannelKind, FutureId,
};

#[derive(Debug)]
pub struct Tracer<C> {
    sender: Sender<FutureUpdate, C>,
}

impl<C: Encoder<FutureUpdate> + Clone> Clone for Tracer<C> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<C: Encoder<FutureUpdate>> Tracer<C> {
    fn trace(&mut self, update: FutureUpdate) {
        self.sender
            .feed(update)
            .now_or_never()
            .expect("tracing channel not ready")
            .ok(); // we don't have a recourse if the tracing channel is gone
    }
}

impl<C> TakeHandle<Wasm, &'static str> for Tracer<C>
where
    C: Encoder<FutureUpdate> + Default,
{
    type Handle = Self;

    fn take_handle(env: &mut Wasm, id: &'static str) -> Self::Handle {
        Self {
            sender: Sender::take_handle(env, id),
        }
    }
}

impl<C> ValidateInterface<&str> for Tracer<C>
where
    C: Encoder<FutureUpdate> + Default,
{
    fn validate_interface(errors: &mut InterfaceErrors, interface: &Interface<()>, id: &str) {
        if let Some(spec) = interface.outbound_channel(id) {
            if spec.capacity != None {
                let err = format!(
                    "unexpected channel capacity: {:?}, expected infinite capacity (`None`)",
                    spec.capacity
                );
                let err = ChannelErrorKind::custom(err).for_channel(ChannelKind::Outbound, id);
                errors.insert_error(err);
            }
        } else {
            let err = ChannelErrorKind::Unknown.for_channel(ChannelKind::Outbound, id);
            errors.insert_error(err);
        }
    }
}

#[derive(Debug)]
pub struct Traced<F, C: Encoder<FutureUpdate>> {
    id: FutureId,
    tracer: Tracer<C>,
    inner: F,
}

impl<F, C: Encoder<FutureUpdate>> Drop for Traced<F, C> {
    fn drop(&mut self) {
        if thread::panicking() {
            return; // If we're in a panicking thread, the trace receiver may be gone.
        }

        self.tracer.trace(FutureUpdate {
            id: self.id,
            kind: FutureUpdateKind::Dropped,
        });
    }
}

impl<F: Future, C: Encoder<FutureUpdate>> Traced<F, C> {
    pub(crate) fn new(inner: F, mut tracer: Tracer<C>, name: String) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        tracer.trace(FutureUpdate {
            id,
            kind: FutureUpdateKind::Created { name },
        });
        Self { id, tracer, inner }
    }

    // Because we implement `Drop`, we cannot use `pin_project` macro, so we implement
    // field projections manually
    fn project_inner(self: Pin<&mut Self>) -> Pin<&mut F> {
        // SAFETY: map function is simple field access, which satisfies
        // the `map_unchecked_mut` contract
        unsafe { self.map_unchecked_mut(|this| &mut this.inner) }
    }

    fn project_tracer(self: Pin<&mut Self>) -> &mut Tracer<C> {
        // SAFETY: tracer is never considered pinned
        unsafe { &mut self.get_unchecked_mut().tracer }
    }
}

impl<F: Future, C: Encoder<FutureUpdate>> Future for Traced<F, C> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let id = self.id;
        self.as_mut().project_tracer().trace(FutureUpdate {
            id,
            kind: FutureUpdateKind::Polling,
        });

        let poll_result = self.as_mut().project_inner().poll(cx);
        self.project_tracer().trace(FutureUpdate {
            id,
            kind: FutureUpdateKind::Polled {
                is_ready: poll_result.is_ready(),
            },
        });
        poll_result
    }
}
