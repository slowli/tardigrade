//! Services available to workflows. For now, a single service is supported - a wall clock.

use chrono::{DateTime, Utc};

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, Weak},
};

use crate::WorkflowSpawner;
use tardigrade::{interface::Interface, workflow::WorkflowFn};
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
    fn interface(&self, id: &str) -> Option<&Interface<()>>;

    /// Creates a new workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be spawned, e.g., if the provided `args`
    /// are incorrect.
    fn create_workflow(
        &self,
        id: &str,
        args: Vec<u8>,
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
pub struct WorkflowManager {
    arc: Weak<Self>,
    spawners: HashMap<String, WorkflowSpawner<()>>,
    // FIXME: connect to supervisor to store the workflow
}

impl WorkflowManager {
    /// Creates a manager builder.
    pub fn builder() -> WorkflowManagerBuilder {
        WorkflowManagerBuilder {
            manager: Self::default(),
        }
    }
}

impl ManageWorkflows for WorkflowManager {
    fn interface(&self, id: &str) -> Option<&Interface<()>> {
        Some(self.spawners.get(id)?.interface())
    }

    fn create_workflow(
        &self,
        id: &str,
        args: Vec<u8>,
        handles: &ChannelHandles<'_>,
    ) -> Result<WorkflowId, SpawnError> {
        let spawner = self
            .spawners
            .get(id)
            .unwrap_or_else(|| panic!("workflow with ID `{}` is not defined", id));

        let mut patched_services = spawner.services.clone();
        patched_services.workflows = self.arc.upgrade().unwrap();
        let workflow = spawner
            .do_spawn(args, handles, patched_services)
            .map_err(|err| SpawnError::new(err.to_string()))?;
        let _workflow = workflow
            .init()
            .map_err(|err| SpawnError::new(err.to_string()))?
            .into_inner();
        // FIXME: persist workflow
        Ok(0)
    }
}

/// Builder for a [`WorkflowManager`].
#[derive(Debug)]
pub struct WorkflowManagerBuilder {
    manager: WorkflowManager,
}

impl WorkflowManagerBuilder {
    /// Inserts a workflow spawner into the manager.
    #[must_use]
    pub fn with_spawner<W: WorkflowFn>(mut self, id: &str, spawner: WorkflowSpawner<W>) -> Self {
        self.manager.spawners.insert(id.to_owned(), spawner.erase());
        self
    }

    /// Finishes building the manager.
    pub fn build(self) -> Arc<WorkflowManager> {
        let mut manager = self.manager;
        Arc::new_cyclic(|arc| {
            manager.arc = Weak::clone(arc);
            manager
        })
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
            workflows: Arc::new(WorkflowManager::default()),
        }
    }
}
