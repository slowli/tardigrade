//! Tracing functionality.

use serde::{Deserialize, Serialize};

use std::{collections::HashMap, error, fmt};

use crate::FutureId;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FutureUpdateKind {
    Created { name: String },
    Polling,
    Polled { is_ready: bool },
    Dropped,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FutureUpdate {
    pub id: FutureId,
    pub kind: FutureUpdateKind,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum FutureUpdateError {
    UndefinedFuture {
        id: FutureId,
    },
    RedefinedFuture {
        id: FutureId,
    },
    InvalidUpdate {
        state: FutureState,
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
    pub fn update(&mut self, update: &FutureUpdateKind) -> Result<(), FutureUpdateError> {
        use self::FutureState::*;

        *self = match (&*self, update) {
            (Created | Polling, FutureUpdateKind::Polling) => Polling,
            (Polling, FutureUpdateKind::Polled { is_ready: false }) => Polling,
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

#[derive(Debug, Clone)]
pub struct TracedFuture {
    name: String,
    state: FutureState,
}

impl TracedFuture {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn state(&self) -> FutureState {
        self.state
    }
}

pub type TracedFutures = HashMap<FutureId, TracedFuture>;

impl TracedFuture {
    pub fn update(map: &mut TracedFutures, update: FutureUpdate) -> Result<(), FutureUpdateError> {
        if let FutureUpdateKind::Created { name } = update.kind {
            let prev_state = map.insert(
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
            let future = map
                .get_mut(&update.id)
                .ok_or(FutureUpdateError::UndefinedFuture { id: update.id })?;
            future.state.update(&update.kind)?;
        }
        Ok(())
    }
}
