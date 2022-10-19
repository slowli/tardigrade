//! Services available to workflows. For now, a single service is supported - a wall clock.

use chrono::{DateTime, Utc};

use std::{borrow::Cow, fmt};

use crate::workflow::ChannelIds;
use tardigrade::{
    interface::Interface,
    spawn::{ChannelsConfig, ManageInterfaces, ManageWorkflows, SpecifyWorkflowChannels},
    ChannelId, WorkflowId,
};

/// Wall clock.
///
/// This trait can be used in [`WorkflowManagerBuilder`] to specify which clock the manager
/// should expose to workflow instances.
///
/// [`WorkflowManagerBuilder`]: crate::manager::WorkflowManagerBuilder
pub trait Clock: Send + Sync + 'static {
    /// Returns the current timestamp. This is used in workflows when creating new timers.
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

#[derive(Debug, Clone)]
pub(crate) struct WorkflowAndChannelIds {
    pub workflow_id: WorkflowId,
    pub channel_ids: ChannelIds,
}

/// Workflow manager that does not hold any workflow definitions and correspondingly
/// cannot spawn workflows.
#[derive(Debug)]
pub(crate) struct NoOpWorkflowManager;

impl ManageInterfaces for NoOpWorkflowManager {
    fn interface(&self, _definition_id: &str) -> Option<Cow<'_, Interface>> {
        None
    }
}

impl ManageWorkflows<'_, ()> for NoOpWorkflowManager {
    type Handle = WorkflowAndChannelIds;
    type Error = anyhow::Error;

    fn create_workflow(
        &self,
        _definition_id: &str,
        _args: Vec<u8>,
        _channels: ChannelsConfig<ChannelId>,
    ) -> Result<Self::Handle, Self::Error> {
        unreachable!("No definitions, thus `create_workflow` should never be called")
    }
}

impl SpecifyWorkflowChannels for NoOpWorkflowManager {
    type Inbound = ChannelId;
    type Outbound = ChannelId;
}

type DynManager = dyn for<'a> ManageWorkflows<
    'a,
    (),
    Inbound = ChannelId,
    Outbound = ChannelId,
    Handle = WorkflowAndChannelIds,
    Error = anyhow::Error,
>;

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
