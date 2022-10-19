//! Tracing.

use futures::{FutureExt, SinkExt};
use serde::{Deserialize, Serialize};

use std::{
    collections::HashMap,
    error, fmt,
    future::Future,
    ops,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
    thread,
};

use crate::{
    channel::Sender,
    interface::{AccessError, InterfaceBuilder, OutboundChannelSpec},
    types::FutureId,
    workflow::{TakeHandle, Wasm},
    Encode,
};

/// Payload of the traced future update.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum FutureUpdateKind {
    /// Future has been created.
    Created {
        /// Human-readable future name that can be used for debugging purposes.
        name: String,
    },
    /// Future is being polled.
    Polling,
    /// Future was polled.
    Polled {
        /// `true` if polling resulted in [`Poll::Ready`](std::task::Poll::Ready).
        is_ready: bool,
    },
    /// Future was dropped.
    Dropped,
}

/// Update for a traced future.
#[derive(Debug, Serialize, Deserialize)]
pub struct FutureUpdate {
    /// ID of the future being updated.
    pub id: FutureId,
    /// Update payload.
    pub kind: FutureUpdateKind,
}

/// Errors that can occur when updating state of a traced future.
#[derive(Debug)]
#[non_exhaustive]
pub enum FutureUpdateError {
    /// Future with the specified ID is undefined.
    UndefinedFuture {
        /// ID of the future.
        id: FutureId,
    },
    /// Future with the specified ID is defined multiple times.
    RedefinedFuture {
        /// ID of the future.
        id: FutureId,
    },
    /// [`FutureUpdate`] is invalid w.r.t. the current state of the traced future.
    InvalidUpdate {
        /// Current future state.
        state: FutureState,
        /// Invalid update.
        update: FutureUpdateKind,
    },
}

impl fmt::Display for FutureUpdateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UndefinedFuture { id } => write!(formatter, "undefined future with ID {}", id),
            Self::RedefinedFuture { id } => {
                write!(formatter, "attempt to redefine future with ID {}", id)
            }
            Self::InvalidUpdate { state, update } => write!(
                formatter,
                "unexpected future transition from state {:?} with update {:?}",
                state, update
            ),
        }
    }
}

impl error::Error for FutureUpdateError {}

/// State of a traced [`Future`](std::future::Future).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FutureState {
    /// Future is created, but was not yet polled.
    Created,
    /// Future was polled at least once, but did not complete.
    Polling,
    /// Future has been polled to completion.
    Completed,
    /// Future was dropped in WASM.
    Dropped,
    /// Future is dropped before completion.
    Abandoned,
}

impl FutureState {
    /// Updates the state.
    ///
    /// # Errors
    ///
    /// Returns an error if the update is inconsistent with the state.
    pub fn update(&mut self, update: &FutureUpdateKind) -> Result<(), FutureUpdateError> {
        use self::FutureState::{Abandoned, Completed, Created, Dropped, Polling};

        *self = match (&*self, update) {
            (Created | Polling, FutureUpdateKind::Polling)
            | (Polling, FutureUpdateKind::Polled { is_ready: false }) => Polling,

            (Polling, FutureUpdateKind::Polled { is_ready: true }) => Completed,
            (Created | Polling, FutureUpdateKind::Dropped) => Abandoned,
            (Completed, FutureUpdateKind::Dropped) => Dropped,

            _ => {
                return Err(FutureUpdateError::InvalidUpdate {
                    state: *self,
                    update: update.clone(),
                })
            }
        };
        Ok(())
    }
}

/// Information about a traced [`Future`](std::future::Future).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracedFuture {
    name: String,
    state: FutureState,
}

impl TracedFuture {
    /// Returns human-readable future name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the current state of the future.
    pub fn state(&self) -> FutureState {
        self.state
    }
}

/// Container for traced futures.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TracedFutures {
    futures: HashMap<FutureId, TracedFuture>,
}

impl TracedFutures {
    /// Returns the number of traced futures.
    pub fn len(&self) -> usize {
        self.futures.len()
    }

    /// Checks whether this container is empty (contains no traced futures).
    pub fn is_empty(&self) -> bool {
        self.futures.is_empty()
    }

    /// Obtains the traced future with the specified ID.
    pub fn get(&self, id: FutureId) -> Option<&TracedFuture> {
        self.futures.get(&id)
    }

    /// Iterates over the contained futures.
    pub fn iter(&self) -> impl Iterator<Item = (FutureId, &TracedFuture)> + '_ {
        self.futures.iter().map(|(id, state)| (*id, state))
    }

    /// Updates traced futures, adding a new future to the `map` if necessary.
    ///
    /// # Errors
    ///
    /// Returns an error if the update is inconsistent with the current state of an existing
    /// traced future.
    pub fn update(&mut self, update: FutureUpdate) -> Result<(), FutureUpdateError> {
        if let FutureUpdateKind::Created { name } = update.kind {
            let prev_state = self.futures.insert(
                update.id,
                TracedFuture {
                    name,
                    state: FutureState::Created,
                },
            );
            if prev_state.is_some() {
                return Err(FutureUpdateError::RedefinedFuture { id: update.id });
            }
        } else {
            let future = self
                .futures
                .get_mut(&update.id)
                .ok_or(FutureUpdateError::UndefinedFuture { id: update.id })?;
            future.state.update(&update.kind)?;
        }
        Ok(())
    }
}

impl ops::Index<FutureId> for TracedFutures {
    type Output = TracedFuture;

    fn index(&self, id: FutureId) -> &Self::Output {
        self.get(id)
            .unwrap_or_else(|| panic!("future with ID {} is not traced", id))
    }
}

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

impl<C: Encode<FutureUpdate>> TakeHandle<InterfaceBuilder> for Tracer<C> {
    type Id = str;
    type Handle = ();

    fn take_handle(env: &mut InterfaceBuilder, id: &Self::Id) -> Result<(), AccessError> {
        let mut spec = OutboundChannelSpec::default();
        spec.capacity = None;
        env.insert_outbound_channel(id, spec);
        Ok(())
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
