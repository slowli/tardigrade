//! Services available to workflows. For now, a single service is supported - a wall clock.

use chrono::{DateTime, Utc};
use tracing_tunnel::TracingEventReceiver;

use std::{fmt, future::Future, pin::Pin};

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

impl fmt::Debug for dyn Clock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Clock").finish_non_exhaustive()
    }
}

impl Clock for () {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Scheduler that allows creating futures completing at the specified timestamp.
pub trait Schedule: Clock {
    /// Creates a timer with the specified expiration timestamp.
    fn create_timer(&self, expires_at: DateTime<Utc>) -> TimerFuture;
}

impl fmt::Debug for dyn Schedule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Schedule").finish_non_exhaustive()
    }
}

/// Future for [`Schedule::create_timer()`].
pub type TimerFuture = Pin<Box<dyn Future<Output = DateTime<Utc>> + Send>>;

/// Similar to [`tardigrade::ManageWorkflows`], but mutable and synchronous.
/// The returned handle is stored in a `Workflow` and, before it's persisted, exchanged for
/// a `WorkflowId`.
pub(crate) trait StashWorkflow: Send + Sync + ManageInterfaces {
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
    pub workflows: Option<&'a mut dyn StashWorkflow>,
    pub tracer: Option<&'a mut TracingEventReceiver>,
}

impl fmt::Debug for Services<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Services").finish_non_exhaustive()
    }
}
