//! Services available to workflows. For now, a single service is supported - a wall clock.

use chrono::{DateTime, Utc};

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use tardigrade::interface::Interface;
use tardigrade_shared::{ChannelId, SpawnError, WorkflowId};

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

impl dyn ManageChannels {
    /// ID of a predefined closed channel.
    pub const CLOSED_CHANNEL: ChannelId = 0;
}

impl fmt::Debug for dyn ManageChannels {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManageChannels")
            .finish_non_exhaustive()
    }
}

/// In-memory implementation of a [channel manager](ManageChannels).
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

/// Channel handles to use when creating a new workflow instance.
#[derive(Debug, Default)]
pub struct ChannelHandles<'a> {
    /// IDs for inbound channels.
    pub inbound: HashMap<&'a str, ChannelId>,
    /// IDs for outbound channels.
    pub outbound: HashMap<&'a str, ChannelId>,
}

/// Manager of workflows.
pub trait ManageWorkflows: Send + Sync + 'static {
    /// Returns the interface spec of a workflow with the specified ID.
    fn interface(&self, id: &str) -> Option<Interface<()>>;

    /// Creates a new workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be spawned, e.g., if the provided `args`
    /// are incorrect.
    fn create_workflow(
        &self,
        id: &str,
        args: &[u8],
        handles: &ChannelHandles<'_>,
    ) -> Result<WorkflowId, SpawnError>;
}

impl fmt::Debug for dyn ManageWorkflows {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManageWorkflows")
            .finish_non_exhaustive()
    }
}

/// Simple implementation of a workflow manager.
#[derive(Debug, Default)]
pub struct SimpleWorkflowManager {}

impl ManageWorkflows for SimpleWorkflowManager {
    fn interface(&self, _id: &str) -> Option<Interface<()>> {
        None
    }

    fn create_workflow(
        &self,
        _id: &str,
        _args: &[u8],
        _handles: &ChannelHandles<'_>,
    ) -> Result<WorkflowId, SpawnError> {
        todo!()
    }
}

/// Dynamically dispatched services available to workflows.
#[derive(Debug, Clone)]
pub(crate) struct Services {
    pub clock: Arc<dyn Clock>,
    pub channels: Arc<dyn ManageChannels>,
    pub workflows: Arc<dyn ManageWorkflows>,
}

impl Default for Services {
    fn default() -> Self {
        Self {
            clock: Arc::new(Utc::now),
            channels: Arc::new(InMemoryChannelManager::default()),
            workflows: Arc::new(SimpleWorkflowManager::default()),
        }
    }
}
