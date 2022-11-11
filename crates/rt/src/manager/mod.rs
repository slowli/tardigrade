//! [`WorkflowManager`] and tightly related types.
//!
//! See `WorkflowManager` docs for an overview and examples of usage.

use chrono::{DateTime, Utc};
use futures::{lock::Mutex, StreamExt};
use tracing_tunnel::{LocalSpans, PersistedMetadata};

use std::{borrow::Cow, collections::HashMap};

pub mod driver;
mod handle;
mod new_workflows;
mod persistence;
mod tick;
mod traits;

#[cfg(test)]
mod tests;

pub use self::{
    handle::{MessageReceiver, MessageSender, ReceivedMessage, WorkflowHandle},
    tick::{Actions, TickResult, WouldBlock},
    traits::{AsManager, CreateModule},
};

use self::persistence::StorageHelper;
use crate::{
    module::Clock,
    storage::{
        ChannelRecord, ModuleRecord, ReadChannels, ReadModules, ReadWorkflows, Storage,
        StorageTransaction, WriteChannels, WriteModules, WriteWorkflows,
    },
    workflow::ChannelIds,
    WorkflowEngine, WorkflowModule, WorkflowSpawner,
};
use tardigrade::{channel::SendError, ChannelId, WorkflowId};

#[derive(Debug)]
struct WorkflowAndChannelIds {
    workflow_id: WorkflowId,
    channel_ids: ChannelIds,
}

#[derive(Debug, Default)]
struct WorkflowSpawners {
    inner: HashMap<String, HashMap<String, WorkflowSpawner<()>>>,
}

impl WorkflowSpawners {
    fn get(&self, module_id: &str, name_in_module: &str) -> &WorkflowSpawner<()> {
        &self.inner[module_id][name_in_module]
    }

    fn split_full_id(full_id: &str) -> Option<(&str, &str)> {
        full_id.split_once("::")
    }

    fn for_full_id(&self, full_id: &str) -> Option<&WorkflowSpawner<()>> {
        let (module_id, name_in_module) = Self::split_full_id(full_id)?;
        self.inner.get(module_id)?.get(name_in_module)
    }

    fn insert(&mut self, module_id: String, name_in_module: String, spawner: WorkflowSpawner<()>) {
        let module_entry = self.inner.entry(module_id).or_default();
        module_entry.insert(name_in_module, spawner);
    }
}

/// Part of the manager used during workflow instantiation.
#[derive(Debug, Clone, Copy)]
struct Shared<'a> {
    clock: &'a dyn Clock,
    spawners: &'a WorkflowSpawners,
}

#[derive(Debug, Clone, Copy)]
enum ChannelSide {
    HostSender,
    WorkflowSender(WorkflowId),
    Receiver,
}

/// Simple in-memory implementation of a workflow manager.
///
/// A workflow manager is responsible for managing state and interfacing with workflows
/// and channels connected to the workflows. In particular, a manager supports the following
/// operations:
///
/// - Spawning new workflows (including from the workflow code)
/// - Writing messages to channels and reading messages from channels
/// - Driving the contained workflows to completion (either [directly](Self::tick()) or
///   using [future-based API][`AsyncEnv`])
///
/// This is the simplest manager implementation that stores the entire state in RAM.
/// It is not a good choice for high-load, but is sufficiently flexible to support
/// most basic use cases (e.g., persisting / resuming workflows).
///
/// [`AsyncEnv`]: crate::manager::future::AsyncEnv
///
/// # Examples
///
/// ```
/// # use tardigrade_rt::{manager::{WorkflowHandle, WorkflowManager}, WorkflowModule};
/// # fn test_wrapper(module: WorkflowModule) -> anyhow::Result<()> {
/// // A manager is instantiated using the builder pattern:
/// let module: WorkflowModule = // ...
/// #   module;
/// let spawner = module.for_untyped_workflow("TestWorkflow").unwrap();
/// let mut manager = WorkflowManager::builder()
///     .with_spawner("test", spawner)
///     .build();
///
/// // After that, new workflows can be spawned using `ManageWorkflowsExt`
/// // trait from the `tardigrade` crate:
/// use tardigrade::spawn::ManageWorkflowsExt;
/// let args = b"test_args".to_vec();
/// let workflow =
///     manager.new_workflow::<()>("test", args)?.build()?;
/// // Do something with `workflow`, e.g., write something to its channels...
///
/// // Initialize the workflow:
/// let receipt = manager.tick()?.into_inner()?;
/// println!("{:?}", receipt);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct WorkflowManager<C, S> {
    clock: C,
    spawners: WorkflowSpawners,
    storage: S,
    local_spans: Mutex<HashMap<WorkflowId, LocalSpans>>,
}

#[allow(clippy::mismatching_type_param_order)] // false positive
impl<S: for<'a> Storage<'a>> WorkflowManager<(), S> {
    /// Creates a builder that will use the specified storage.
    pub fn builder(storage: S) -> WorkflowManagerBuilder<'static, (), S> {
        WorkflowManagerBuilder {
            module_creator: Box::new(WorkflowEngine::default()),
            clock: (),
            storage,
        }
    }
}

impl<C: Clock, S: for<'a> Storage<'a>> WorkflowManager<C, S> {
    async fn new(clock: C, storage: S, module_creator: &dyn CreateModule) -> anyhow::Result<Self> {
        let spawners = {
            let transaction = storage.readonly_transaction().await;
            let mut spawners = WorkflowSpawners::default();
            let mut module_records = transaction.modules();
            while let Some(record) = module_records.next().await {
                let module = module_creator.create_module(&record).await?;
                for (name, spawner) in module.into_spawners() {
                    spawners.insert(record.id.clone(), name, spawner);
                }
            }
            spawners
        };

        Ok(Self {
            clock,
            spawners,
            storage,
            local_spans: Mutex::default(),
        })
    }

    /// Inserts the specified module into the manager.
    pub async fn insert_module(&mut self, id: &str, module: WorkflowModule<'_>) {
        let mut transaction = self.storage.transaction().await;
        let module_record = ModuleRecord {
            id: id.to_owned(),
            bytes: Cow::Borrowed(module.bytes),
            tracing_metadata: PersistedMetadata::default(),
        };
        transaction.insert_module(module_record).await;

        for (name, spawner) in module.into_spawners() {
            self.spawners.insert(id.to_owned(), name, spawner);
        }
        transaction.commit().await;
    }

    /// Returns current information about the channel with the specified ID, or `None` if a channel
    /// with this ID does not exist.
    pub async fn channel(&self, channel_id: ChannelId) -> Option<ChannelRecord> {
        let transaction = self.storage.readonly_transaction().await;
        let record = transaction.channel(channel_id).await?;
        Some(record)
    }

    /// Returns a handle to a workflow with the specified ID.
    pub async fn workflow(&self, workflow_id: WorkflowId) -> Option<WorkflowHandle<'_, (), Self>> {
        let transaction = self.storage.readonly_transaction().await;
        let record = transaction.workflow(workflow_id).await?;
        let interface = self
            .spawners
            .get(&record.module_id, &record.name_in_module)
            .interface();
        let handle = WorkflowHandle::new(self, &transaction, interface, record).await;
        Some(handle)
    }

    /// Returns the number of non-completed workflows.
    pub async fn workflow_count(&self) -> usize {
        let transaction = self.storage.readonly_transaction().await;
        transaction.count_workflows().await
    }

    /// Returns the encapsulated storage.
    pub fn into_storage(self) -> S {
        self.storage
    }
}

impl<'a, C: Clock, S: Storage<'a>> WorkflowManager<C, S> {
    fn shared(&'a self) -> Shared<'a> {
        Shared {
            clock: &self.clock,
            spawners: &self.spawners,
        }
    }

    pub(crate) async fn send_message(
        &'a self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), SendError> {
        let mut transaction = self.storage.transaction().await;
        let result = transaction.push_messages(channel_id, vec![message]).await;
        transaction.commit().await;
        result
    }

    pub(crate) async fn close_host_sender(&'a self, channel_id: ChannelId) {
        let mut transaction = self.storage.transaction().await;
        StorageHelper::new(&mut transaction)
            .close_channel_side(channel_id, ChannelSide::HostSender)
            .await;
        transaction.commit().await;
    }

    pub(crate) async fn close_host_receiver(&'a self, channel_id: ChannelId) {
        let mut transaction = self.storage.transaction().await;
        if cfg!(debug_assertions) {
            let channel = transaction.channel(channel_id).await.unwrap();
            debug_assert!(
                channel.receiver_workflow_id.is_none(),
                "Attempted to close channel {} for which the host doesn't hold receiver",
                channel_id
            );
        }
        StorageHelper::new(&mut transaction)
            .close_channel_side(channel_id, ChannelSide::Receiver)
            .await;
        transaction.commit().await;
    }

    /// Sets the current time for this manager. This may expire timers in some of the contained
    /// workflows.
    #[tracing::instrument(skip(self))]
    pub async fn set_current_time(&'a self, time: DateTime<Utc>) {
        let mut transaction = self.storage.transaction().await;
        StorageHelper::new(&mut transaction)
            .set_current_time(time)
            .await;
        transaction.commit().await;
    }

    /// Aborts the workflow with the specified ID. The parent workflow, if any, will be notified,
    /// and all channel handles owned by the workflow will be properly disposed.
    ///
    /// # Panics
    ///
    /// Panics if the manager does not contain a workflow with the specified ID.
    #[tracing::instrument(skip(self))]
    pub async fn abort_workflow(&'a self, workflow_id: WorkflowId) {
        let mut transaction = self.storage.transaction().await;
        let record = transaction
            .manipulate_workflow(workflow_id, |persisted| {
                persisted.abort();
            })
            .await;
        if let Some(record) = record {
            StorageHelper::new(&mut transaction)
                .handle_workflow_update(workflow_id, record.parent_id, &record.persisted)
                .await;
        }
        transaction.commit().await;
    }
}

/// Builder for a [`WorkflowManager`].
#[derive(Debug)]
pub struct WorkflowManagerBuilder<'r, C, S> {
    clock: C,
    module_creator: Box<dyn CreateModule + 'r>,
    storage: S,
}

#[allow(clippy::mismatching_type_param_order)] // false positive
impl<'r, S: for<'a> Storage<'a>> WorkflowManagerBuilder<'r, (), S> {
    /// Sets the wall clock to be used in the manager.
    #[must_use]
    pub fn with_clock<C: Clock>(self, clock: C) -> WorkflowManagerBuilder<'r, C, S> {
        WorkflowManagerBuilder {
            clock,
            module_creator: self.module_creator,
            storage: self.storage,
        }
    }
}

impl<'r, C: Clock, S: for<'a> Storage<'a>> WorkflowManagerBuilder<'r, C, S> {
    /// Specifies the [module creator](CreateModule) to use.
    #[must_use]
    pub fn with_module_creator(mut self, creator: impl CreateModule + 'r) -> Self {
        self.module_creator = Box::new(creator);
        self
    }

    /// Finishes building the manager.
    ///
    /// # Errors
    ///
    /// Returns an error if [module instantiation](CreateModule) fails.
    pub async fn build(self) -> anyhow::Result<WorkflowManager<C, S>> {
        WorkflowManager::new(self.clock, self.storage, self.module_creator.as_ref()).await
    }
}
