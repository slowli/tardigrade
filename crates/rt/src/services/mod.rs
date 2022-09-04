//! Services available to workflows. For now, a single service is supported - a wall clock.

use chrono::{DateTime, Utc};

use std::{borrow::Cow, fmt, sync::Arc};

mod manager;

pub use self::manager::{
    ChannelInfo, PersistedWorkflows, WorkflowBuilderExt, WorkflowManager, WorkflowManagerBuilder,
};

use crate::{workflow::ChannelIds, WorkflowId};
use tardigrade::{
    interface::Interface,
    spawn::{ChannelHandles, ManageWorkflows, SpawnError},
};

/// Wall clock.
pub trait Clock: Send + Sync + 'static {
    /// Returns the current timestamp. This is used in [`Workflow`]s when creating new timers.
    ///
    /// [`Workflow`]: crate::Workflow
    fn now(&self) -> DateTime<Utc>;
}

impl<F> Clock for F
where
    F: Fn() -> DateTime<Utc> + Send + Sync + 'static,
{
    fn now(&self) -> DateTime<Utc> {
        self()
    }
}

impl fmt::Debug for dyn Clock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Clock").finish_non_exhaustive()
    }
}

/// FIXME
#[derive(Debug)]
pub struct WorkflowAndChannelIds {
    pub(crate) workflow_id: WorkflowId,
    pub(crate) channel_ids: ChannelIds,
}

/// Workflow manager that does not hold any workflow definitions and correspondingly
/// cannot spawn workflows.
#[derive(Debug)]
pub struct NoOpWorkflowManager;

impl ManageWorkflows for NoOpWorkflowManager {
    type Handle = WorkflowAndChannelIds;

    fn interface(&self, _definition_id: &str) -> Option<Cow<'_, Interface<()>>> {
        None
    }

    fn create_workflow(
        &self,
        _definition_id: &str,
        _args: Vec<u8>,
        _handles: &ChannelHandles,
    ) -> Result<Self::Handle, SpawnError> {
        unreachable!("No definitions, thus `create_workflow` should never be called")
    }
}

/// Dynamically dispatched services available to workflows.
#[derive(Clone)]
pub(crate) struct Services {
    pub clock: Arc<dyn Clock>,
    pub workflows: Arc<dyn ManageWorkflows<Handle = WorkflowAndChannelIds>>,
}

impl fmt::Debug for Services {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Services").finish_non_exhaustive()
    }
}

#[cfg(test)]
impl Default for Services {
    fn default() -> Self {
        Self {
            clock: Arc::new(crate::test::MockScheduler::default()),
            workflows: Arc::new(NoOpWorkflowManager),
        }
    }
}
