//! Tracing.

use futures::{FutureExt, SinkExt};

use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
};

use crate::{channel::Sender, Encoder, Wasm};
use tardigrade_shared::{
    workflow::{TakeHandle, WithHandle},
    FutureId, TracedFutureUpdate, TracedFutureUpdateKind,
};

#[derive(Debug)]
pub struct Tracer<C> {
    sender: Sender<TracedFutureUpdate, C>,
}

impl<C: Encoder<TracedFutureUpdate> + Clone> Clone for Tracer<C> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<C: Encoder<TracedFutureUpdate>> Tracer<C> {
    fn trace(&mut self, update: TracedFutureUpdate) {
        self.sender
            .feed(update)
            .now_or_never()
            .expect("tracing channel not ready")
            .unwrap(); // the sink is infallible
    }
}

impl<C> WithHandle<Wasm> for Tracer<C>
where
    C: Encoder<TracedFutureUpdate> + Default,
{
    type Handle = Self;
}

impl<C> TakeHandle<Wasm, &'static str> for Tracer<C>
where
    C: Encoder<TracedFutureUpdate> + Default,
{
    fn take_handle(env: &mut Wasm, id: &'static str) -> Self::Handle {
        Self {
            sender: Sender::take_handle(env, id),
        }
    }
}

#[derive(Debug)]
pub struct Traced<F, C: Encoder<TracedFutureUpdate>> {
    id: FutureId,
    tracer: Tracer<C>,
    inner: F,
}

impl<F, C: Encoder<TracedFutureUpdate>> Drop for Traced<F, C> {
    fn drop(&mut self) {
        self.tracer.trace(TracedFutureUpdate {
            id: self.id,
            kind: TracedFutureUpdateKind::Dropped,
        });
    }
}

impl<F: Future, C: Encoder<TracedFutureUpdate>> Traced<F, C> {
    pub(crate) fn new(inner: F, mut tracer: Tracer<C>, name: String) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        tracer.trace(TracedFutureUpdate {
            id,
            kind: TracedFutureUpdateKind::Created { name },
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

impl<F: Future, C: Encoder<TracedFutureUpdate>> Future for Traced<F, C> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let id = self.id;
        self.as_mut().project_tracer().trace(TracedFutureUpdate {
            id,
            kind: TracedFutureUpdateKind::Polling,
        });

        let poll_result = self.as_mut().project_inner().poll(cx);
        self.project_tracer().trace(TracedFutureUpdate {
            id,
            kind: TracedFutureUpdateKind::Polled {
                is_ready: poll_result.is_ready(),
            },
        });
        poll_result
    }
}
