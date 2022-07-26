//! Helper traits for `WorkflowManager`.

use std::sync::Arc;

use super::{Clock, WorkflowManager};
use crate::{engine::WorkflowEngine, storage::Storage};

/// Trait encapsulating all type params of a [`WorkflowManager`].
pub trait AsManager: Send + Sync {
    /// Engine used by the manager.
    type Engine: WorkflowEngine;
    /// Storage used by the manager.
    type Storage: Storage;
    /// Clock used by the manager.
    type Clock: Clock;

    #[doc(hidden)] // implementation detail
    fn as_manager(&self) -> &WorkflowManager<Self::Engine, Self::Clock, Self::Storage>;
}

impl<E: WorkflowEngine, C: Clock, S: Storage> AsManager for WorkflowManager<E, C, S> {
    type Engine = E;
    type Storage = S;
    type Clock = C;

    #[inline]
    fn as_manager(&self) -> &WorkflowManager<Self::Engine, Self::Clock, Self::Storage> {
        self
    }
}

impl<M: AsManager> AsManager for &M {
    type Engine = M::Engine;
    type Storage = M::Storage;
    type Clock = M::Clock;

    #[inline]
    fn as_manager(&self) -> &WorkflowManager<Self::Engine, Self::Clock, Self::Storage> {
        (**self).as_manager()
    }
}

impl<M: AsManager> AsManager for Arc<M> {
    type Engine = M::Engine;
    type Storage = M::Storage;
    type Clock = M::Clock;

    #[inline]
    fn as_manager(&self) -> &WorkflowManager<Self::Engine, Self::Clock, Self::Storage> {
        (**self).as_manager()
    }
}
