//! Services available to workflows. For now, a single service is supported - a wall clock.

use chrono::{DateTime, Utc};
use tracing_tunnel::TracingEventReceiver;

use std::fmt;

use tardigrade::{
    spawn::{ChannelsConfig, ManageInterfaces},
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

/// Similar to [`tardigrade::ManageWorkflows`], but mutable and synchronous.
/// The returned handle is stored in a `Workflow` and, before it's persisted, exchanged for
/// a `WorkflowId`.
pub(crate) trait StashWorkflows: Send + Sync + ManageInterfaces {
    fn stash_workflow(
        &mut self,
        stub_id: WorkflowId,
        id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<ChannelId>,
    );
}

/// Dynamically dispatched services available to workflows.
pub(crate) struct Services<'a> {
    pub clock: &'a dyn Clock,
    pub workflows: Option<&'a mut dyn StashWorkflows>,
    pub tracer: Option<TracingEventReceiver<'a>>,
}

impl fmt::Debug for Services<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Services").finish_non_exhaustive()
    }
}
