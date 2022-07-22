//! `Future` extensions.

use std::future::Future;

use crate::{
    trace::{FutureUpdate, Traced, Tracer},
    Encode,
};

/// Workflow-specific [`Future`] extensions.
pub trait FutureExt: Sized {
    /// Wraps a future into a [`Traced`] wrapper so its progress can be traced on host.
    fn trace<C>(self, tracer: &Tracer<C>, description: impl Into<String>) -> Traced<Self, C>
    where
        C: Encode<FutureUpdate> + Clone;
}

impl<T: Future> FutureExt for T {
    fn trace<C>(self, tracer: &Tracer<C>, description: impl Into<String>) -> Traced<Self, C>
    where
        C: Encode<FutureUpdate> + Clone,
    {
        Traced::new(self, tracer.clone(), description.into())
    }
}
