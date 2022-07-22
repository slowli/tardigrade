//! Services available to workflows. For now, a single service is supported - a wall clock.

use chrono::{DateTime, Utc};

use std::{fmt, sync::Arc};

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

/// Dynamically dispatched services available to workflows.
#[derive(Debug, Clone)]
pub(crate) struct Services {
    pub clock: Arc<dyn Clock>,
}

impl Default for Services {
    fn default() -> Self {
        Self {
            clock: Arc::new(Utc::now),
        }
    }
}
