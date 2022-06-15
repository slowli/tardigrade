//! `Future` extensions.

use std::future::Future;

mod trace;

pub use self::trace::{Traced, Tracer};

use crate::Encoder;
use tardigrade_shared::TracedFutureUpdate;

pub trait FutureExt: Sized {
    fn trace<C>(self, tracer: &Tracer<C>, description: impl Into<String>) -> Traced<Self, C>
    where
        C: Encoder<TracedFutureUpdate> + Clone;
}

impl<T: Future> FutureExt for T {
    fn trace<C>(self, tracer: &Tracer<C>, description: impl Into<String>) -> Traced<Self, C>
    where
        C: Encoder<TracedFutureUpdate> + Clone,
    {
        Traced::new(self, tracer.clone(), description.into())
    }
}
