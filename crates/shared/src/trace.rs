//! Future tracing functionality.

use serde::{Deserialize, Serialize};

use std::{collections::HashMap, error, fmt, ops};

use crate::FutureId;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone, Default)]
pub struct TracedFutures {
    inner: HashMap<FutureId, TracedFuture>,
}

impl TracedFutures {
    /// Returns the number of traced futures.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Checks whether this container is empty (contains no traced futures).
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Obtains the traced future with the specified ID.
    pub fn get(&self, id: FutureId) -> Option<&TracedFuture> {
        self.inner.get(&id)
    }

    /// Iterates over the contained futures.
    pub fn iter(&self) -> impl Iterator<Item = (FutureId, &TracedFuture)> + '_ {
        self.inner.iter().map(|(id, state)| (*id, state))
    }

    /// Updates traced futures, adding a new future to the `map` if necessary.
    ///
    /// # Errors
    ///
    /// Returns an error if the update is inconsistent with the current state of an existing
    /// traced future.
    pub fn update(&mut self, update: FutureUpdate) -> Result<(), FutureUpdateError> {
        if let FutureUpdateKind::Created { name } = update.kind {
            let prev_state = self.inner.insert(
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
                .inner
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
