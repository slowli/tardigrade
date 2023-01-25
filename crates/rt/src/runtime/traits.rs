//! Helper traits for `Runtime`.

use super::{Clock, Runtime};
use crate::{engine::WorkflowEngine, storage::Storage};

/// Trait encapsulating all type params of a [`Runtime`].
pub trait AsRuntime: Send + Sync {
    /// Engine used by the runtime.
    type Engine: WorkflowEngine;
    /// Storage used by the runtime.
    type Storage: Storage;
    /// Clock used by the runtime.
    type Clock: Clock;

    #[doc(hidden)] // implementation detail
    fn as_runtime(&self) -> &Runtime<Self::Engine, Self::Clock, Self::Storage>;

    #[doc(hidden)] // implementation detail
    fn as_runtime_mut(&mut self) -> &mut Runtime<Self::Engine, Self::Clock, Self::Storage>;
}

impl<E: WorkflowEngine, C: Clock, S: Storage> AsRuntime for Runtime<E, C, S> {
    type Engine = E;
    type Storage = S;
    type Clock = C;

    #[inline]
    fn as_runtime(&self) -> &Runtime<Self::Engine, Self::Clock, Self::Storage> {
        self
    }

    #[inline]
    fn as_runtime_mut(&mut self) -> &mut Runtime<Self::Engine, Self::Clock, Self::Storage> {
        self
    }
}

pub(super) trait IntoRuntime: AsRuntime {
    fn into_runtime(self) -> Runtime<Self::Engine, Self::Clock, Self::Storage>;
}

impl<E: WorkflowEngine, C: Clock, S: Storage> IntoRuntime for Runtime<E, C, S> {
    fn into_runtime(self) -> Runtime<Self::Engine, Self::Clock, Self::Storage> {
        self
    }
}
