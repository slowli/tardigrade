//! Services available to workflows. For now, a single service is supported - a wall clock.

use chrono::{DateTime, Utc};
use tracing_tunnel::TracingEventReceiver;

use std::{
    any::{Any, TypeId},
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
};

use crate::workflow::ChannelIds;
use tardigrade::{ChannelId, WorkflowId};

/// Wall clock.
///
/// This trait can be used in [`RuntimeBuilder`] to specify which clock the runtime
/// should expose to workflow instances.
///
/// [`RuntimeBuilder`]: crate::runtime::RuntimeBuilder
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
pub(crate) trait StashStub: Any + Send + Sync {
    /// Stashes a request to retrieve the specified workflow definition.
    fn stash_definition(&mut self, stub_id: u64, definition_id: &str);

    /// Stashes a workflow stub with the specified params.
    fn stash_workflow(
        &mut self,
        stub_id: WorkflowId,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelIds,
    );

    fn stash_channel(&mut self, stub_id: ChannelId);
}

impl dyn StashStub {
    pub(crate) fn downcast<T: StashStub>(self: Box<Self>) -> T {
        assert_eq!(self.as_ref().type_id(), TypeId::of::<T>());
        *unsafe {
            // SAFETY: This duplicates downcasting logic from `Box::<dyn Any>::downcast()`.
            let leaked = (Box::leak(self) as *mut Self).cast::<T>();
            Box::<T>::from_raw(leaked)
        }
    }
}

/// Dynamically dispatched services available to workflows.
pub(crate) struct Services {
    pub clock: Arc<dyn Clock>,
    pub stubs: Option<Box<dyn StashStub>>,
    pub tracer: Option<TracingEventReceiver>,
}

impl fmt::Debug for Services {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Services").finish_non_exhaustive()
    }
}
