//! Services available to workflows. For now, a single service is supported - a wall clock.

use chrono::{DateTime, Utc};

use std::{borrow::Cow, fmt};

use crate::{workflow::ChannelIds, WorkflowId};
use tardigrade::{
    interface::Interface,
    spawn::{ChannelHandles, ManageInterfaces, ManageWorkflows, SpawnError},
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

#[derive(Debug)]
pub(crate) struct WorkflowAndChannelIds {
    pub workflow_id: WorkflowId,
    pub channel_ids: ChannelIds,
}

/// Workflow manager that does not hold any workflow definitions and correspondingly
/// cannot spawn workflows.
#[derive(Debug)]
pub(crate) struct NoOpWorkflowManager;

impl ManageInterfaces for NoOpWorkflowManager {
    fn interface(&self, _definition_id: &str) -> Option<Cow<'_, Interface<()>>> {
        None
    }
}

impl ManageWorkflows<'_, ()> for NoOpWorkflowManager {
    type Handle = WorkflowAndChannelIds;

    fn create_workflow(
        &self,
        _definition_id: &str,
        _args: Vec<u8>,
        _handles: &ChannelHandles,
    ) -> Result<Self::Handle, SpawnError> {
        unreachable!("No definitions, thus `create_workflow` should never be called")
    }
}

type DynManager = dyn for<'a> ManageWorkflows<'a, (), Handle = WorkflowAndChannelIds>;

/// Dynamically dispatched services available to workflows.
#[derive(Clone, Copy)]
pub(crate) struct Services<'a> {
    pub clock: &'a dyn Clock,
    pub workflows: &'a DynManager,
}

impl fmt::Debug for Services<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Services").finish_non_exhaustive()
    }
}