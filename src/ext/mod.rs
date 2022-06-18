//! `Future` extensions.

use std::future::Future;

use crate::{
    trace::{FutureUpdate, Traced, Tracer},
    Encoder,
};

pub trait FutureExt: Sized {
    fn trace<C>(self, tracer: &Tracer<C>, description: impl Into<String>) -> Traced<Self, C>
    where
        C: Encoder<FutureUpdate> + Clone;
}

impl<T: Future> FutureExt for T {
    fn trace<C>(self, tracer: &Tracer<C>, description: impl Into<String>) -> Traced<Self, C>
    where
        C: Encoder<FutureUpdate> + Clone,
    {
        Traced::new(self, tracer.clone(), description.into())
    }
}
