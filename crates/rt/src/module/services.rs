//! Services available to workflows. For now, a single service is supported - a wall clock.

use chrono::{DateTime, Utc};

use std::{
    fmt,
    sync::{Arc, Mutex},
};

use crate::ChannelId;

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

/// Manager of channels.
pub trait ManageChannels: Send + Sync + 'static {
    /// Creates a new channel.
    fn create_channel(&self) -> ChannelId;
}

impl fmt::Debug for dyn ManageChannels {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManageChannels")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
pub struct InMemoryChannelManager {
    inner: Mutex<InMemoryChannelManagerInner>,
}

#[derive(Debug, Default)]
struct InMemoryChannelManagerInner {
    // FIXME: add channel states
    next_channel_id: ChannelId,
}

impl ManageChannels for InMemoryChannelManager {
    fn create_channel(&self) -> ChannelId {
        let mut lock = self.inner.lock().unwrap();
        let channel_id = lock.next_channel_id;
        lock.next_channel_id += 1;
        channel_id
    }
}

/// Dynamically dispatched services available to workflows.
#[derive(Debug, Clone)]
pub(crate) struct Services {
    pub clock: Arc<dyn Clock>,
    pub channels: Arc<dyn ManageChannels>,
}

impl Default for Services {
    fn default() -> Self {
        Self {
            clock: Arc::new(Utc::now),
            channels: Arc::new(InMemoryChannelManager::default()),
        }
    }
}
