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

use crate::{
    channel::Sender,
    interface::{AccessError, AccessErrorKind, Interface, OutboundChannel, ValidateInterface},
    workflow::{EnvExtensions, ExtendEnv, TakeHandle, Wasm},
    Encode,
};
use tardigrade_shared::FutureId;

/// Wrapper around [`Sender`] that sends updates for [`Traced`] futures to the host.
#[derive(Debug)]
pub struct Tracer<C> {
    sender: Sender<FutureUpdate, C>,
}

impl<C: Encode<FutureUpdate> + Clone> Clone for Tracer<C> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<C: Encode<FutureUpdate>> Tracer<C> {
    fn trace(&mut self, update: FutureUpdate) {
        self.sender
            .feed(update)
            .now_or_never()
            .expect("tracing channel not ready")
            .ok(); // we don't have a recourse if the tracing channel is gone
    }
}

impl<C> TakeHandle<Wasm> for Tracer<C>
where
    C: Encode<FutureUpdate> + Default,
{
    type Id = str;
    type Handle = Self;

    fn take_handle(env: &mut Wasm, id: &str) -> Result<Self::Handle, AccessError> {
        Sender::take_handle(env, id).map(|sender| Self { sender })
    }
}

impl<C> ValidateInterface for Tracer<C>
where
    C: Encode<FutureUpdate> + Default,
{
    type Id = str;

    fn validate_interface(interface: &Interface<()>, id: &str) -> Result<(), AccessError> {
        if let Some(spec) = interface.outbound_channel(id) {
            if spec.capacity != None {
                let err = format!(
                    "unexpected channel capacity: {:?}, expected infinite capacity (`None`)",
                    spec.capacity
                );
                return Err(AccessErrorKind::custom(err).with_location(OutboundChannel(id)));
            }
            Ok(())
        } else {
            let err = AccessErrorKind::Unknown.with_location(OutboundChannel(id));
            Err(err)
        }
    }
}

impl ExtendEnv for TracedFutures {
    fn id(&self) -> String {
        format!("traces::{}", self.channel_name())
    }
}

impl<C> TakeHandle<EnvExtensions> for Tracer<C> {
    type Id = str;
    type Handle = TracedFutures;

    fn take_handle(env: &mut EnvExtensions, id: &Self::Id) -> Result<Self::Handle, AccessError> {
        Ok(env
            .take::<TracedFutures>(&format!("traces::{}", id))?
            .unwrap_or_else(|| TracedFutures::new(id)))
    }
}

/// Wrapper around a [`Future`] that traces its progress.
#[derive(Debug)]
pub struct Traced<F, C: Encode<FutureUpdate>> {
    id: FutureId,
    tracer: Tracer<C>,
    inner: F,
}

impl<F, C: Encode<FutureUpdate>> Drop for Traced<F, C> {
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

impl<F: Future, C: Encode<FutureUpdate>> Traced<F, C> {
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
    // field projections manually.
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

impl<F: Future, C: Encode<FutureUpdate>> Future for Traced<F, C> {
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
