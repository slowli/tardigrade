//! Helper traits for `WorkflowManager`.

use async_trait::async_trait;

use std::fmt;

use super::WorkflowManager;
use crate::{
    module::Clock,
    storage::{ModuleRecord, Storage},
    WorkflowEngine, WorkflowModule,
};

/// Trait encapsulating all type params of a [`WorkflowManager`].
pub trait AsManager {
    /// Storage used by the manager.
    type Storage: for<'a> Storage<'a>;
    /// Clock used by the manager.
    type Clock: Clock;

    #[doc(hidden)] // implementation detail
    fn as_manager(&self) -> &WorkflowManager<Self::Clock, Self::Storage>;
}

impl<C: Clock, S: for<'a> Storage<'a>> AsManager for WorkflowManager<C, S> {
    type Storage = S;
    type Clock = C;

    #[inline]
    fn as_manager(&self) -> &WorkflowManager<Self::Clock, Self::Storage> {
        self
    }
}

/// Customizable [`WorkflowModule`] instantiation logic.
#[async_trait]
pub trait CreateModule {
    /// Restores module from a [`ModuleRecord`].
    ///
    /// # Errors
    ///
    /// Returns an error if instantiation fails for whatever reason. This error will bubble up
    /// in [`WorkflowManagerBuilder::build()`].
    ///
    /// [`WorkflowManagerBuilder::build()`]: crate::manager::WorkflowManagerBuilder::build()
    async fn create_module<'a>(
        &self,
        module: &'a ModuleRecord<'_>,
    ) -> anyhow::Result<WorkflowModule<'a>>;
}

impl fmt::Debug for dyn CreateModule + '_ {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateModule")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CreateModule for WorkflowEngine {
    async fn create_module<'a>(
        &self,
        module: &'a ModuleRecord<'_>,
    ) -> anyhow::Result<WorkflowModule<'a>> {
        WorkflowModule::new(self, module.bytes.as_ref())
    }
}
