//! `WorkflowModule`, `WorkflowSpawner` and closely related types.

use crate::{data::WorkflowData, engine::CreateWorkflow};
use tardigrade::interface::Interface;

mod services;

//#[cfg(test)]
//mod tests;

pub use self::services::{Clock, Schedule, TimerFuture};
pub(crate) use self::services::{Services, StashWorkflow};

/// Spawner of workflows of a specific type.
///
/// Can be created using [`WorkflowModule::for_workflow`] or
/// [`WorkflowModule::for_untyped_workflow`].
#[derive(Debug)]
pub struct WorkflowSpawner<S> {
    interface: Interface,
    inner: S,
}

impl<S: CreateWorkflow> WorkflowSpawner<S> {
    /// Creates a new spawner from the provided parts.
    pub fn new(interface: Interface, inner: S) -> Self {
        Self { interface, inner }
    }

    /// Returns the interface of the workflow spawned by this spawner.
    pub fn interface(&self) -> &Interface {
        &self.interface
    }

    pub(crate) fn create_workflow(&self, data: WorkflowData) -> anyhow::Result<S::Spawned> {
        self.inner.create_workflow(data)
    }
}
