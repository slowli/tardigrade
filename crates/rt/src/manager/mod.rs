//! [`WorkflowManager`] and tightly related types.
//!
//! See `WorkflowManager` docs for an overview and examples of usage.

use chrono::{DateTime, Utc};
use futures::{lock::Mutex, StreamExt};
use tracing_tunnel::{LocalSpans, PersistedMetadata, PersistedSpans};

use std::{collections::HashMap, sync::Arc};

mod handle;
mod new_workflows;
mod persistence;
mod tick;
mod traits;

#[cfg(test)]
pub(crate) mod tests;

pub use self::{
    handle::{
        AnyWorkflowHandle, CompletedWorkflowHandle, ConcurrencyError, ErroneousMessage,
        ErroredWorkflowHandle, MessageReceiver, MessageSender, ReceivedMessage, WorkflowHandle,
    },
    tick::{TickResult, WouldBlock},
    traits::{AsManager, CreateModule},
};

use self::persistence::StorageHelper;
use crate::{
    module::Clock,
    storage::{
        ChannelRecord, ModuleRecord, ReadChannels, ReadModules, ReadWorkflows, Storage,
        StorageTransaction, WorkflowState, WriteChannels, WriteModules, WriteWorkflows,
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

/// Manager for workflow modules, workflows and channels.
///
/// A workflow manager is responsible for managing the state and interfacing with workflows
/// and channels connected to the workflows. In particular, a manager supports the following
/// operations:
///
/// - Spawning new workflows (including from the workflow code)
/// - Writing messages to channels and reading messages from channels
/// - Driving the contained workflows to completion, either [manually](Self::tick()) or
///   using a [`Driver`]
///
/// [`Driver`]: crate::driver::Driver
///
/// # Workflow lifecycle
///
/// A workflow can be in one of the following states:
///
/// - [**Active.**](WorkflowHandle) This is the initial state, provided that the workflow
///   successfully initialized. In this state, the workflow can execute,
///   receive and send messages etc.
/// - [**Completed.**](CompletedWorkflowHandle) This is the terminal state after the workflow
///   completes as a result of its execution or is aborted.
/// - [**Errored.**](ErroredWorkflowHandle) A workflow reaches this state after it panics.
///   The panicking execution is reverted, but this is usually not enough to fix the error;
///   since workflows are largely deterministic, if nothing changes, a repeated execution
///   would most probably lead to the same panic. An errored workflow cannot execute
///   until it is *repaired*.
///
/// The only way to repair a workflow so far is to make the workflow skip the message(s)
/// leading to the error.
///
/// # Examples
///
/// ```
/// # use tardigrade_rt::{
/// #     manager::{WorkflowHandle, WorkflowManager},
/// #     storage::LocalStorage, AsyncIoScheduler, WorkflowModule,
/// # };
/// # async fn test_wrapper(module: WorkflowModule) -> anyhow::Result<()> {
/// // A manager is instantiated using the builder pattern:
/// let storage = LocalStorage::default();
/// let mut manager = WorkflowManager::builder(storage)
///     .with_clock(AsyncIoScheduler)
///     .build()
///     .await?;
/// // After this, modules may be added:
/// let module: WorkflowModule = // ...
/// #   module;
/// manager.insert_module("test", module).await;
///
/// // After that, new workflows can be spawned using `ManageWorkflowsExt`
/// // trait from the `tardigrade` crate:
/// use tardigrade::spawn::ManageWorkflowsExt;
/// let definition_id = "test::Workflow";
/// // ^ The definition ID is the ID of the module and the name of a workflow
/// //   within the module separated by `::`.
/// let args = b"test_args".to_vec();
/// let mut workflow = manager
///     .new_workflow::<()>(definition_id, args)?
///     .build()
///     .await?;
/// // Do something with `workflow`, e.g., write something to its channels...
///
/// // Initialize the workflow:
/// let receipt = manager.tick().await?.drop_handle().into_inner()?;
/// println!("{receipt:?}");
/// // Check the workflow state:
/// workflow.update().await?;
/// assert!(workflow.persisted().is_initialized());
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct WorkflowManager<C, S> {
    pub(crate) clock: C,
    pub(crate) storage: S,
    spawners: WorkflowSpawners,
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
    pub async fn insert_module(&mut self, id: &str, module: WorkflowModule) {
        let mut transaction = self.storage.transaction().await;
        let module_record = ModuleRecord {
            id: id.to_owned(),
            bytes: Arc::clone(&module.bytes),
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

    /// Returns a handle to an active workflow with the specified ID. If the workflow is
    /// not active or does not exist, returns `None`.
    pub async fn workflow(&self, workflow_id: WorkflowId) -> Option<WorkflowHandle<'_, (), Self>> {
        let handle = self.any_workflow(workflow_id).await?;
        match handle {
            AnyWorkflowHandle::Active(handle) => Some(*handle),
            _ => None,
        }
    }

    /// Returns a handle to a workflow with the specified ID. The workflow may have any state.
    /// If the workflow does not exist, returns `None`.
    pub async fn any_workflow(
        &self,
        workflow_id: WorkflowId,
    ) -> Option<AnyWorkflowHandle<'_, Self>> {
        let transaction = self.storage.readonly_transaction().await;
        let record = transaction.workflow(workflow_id).await?;
        match record.state {
            WorkflowState::Active(state) => {
                let interface = self
                    .spawners
                    .get(&record.module_id, &record.name_in_module)
                    .interface();
                let handle =
                    WorkflowHandle::new(self, &transaction, workflow_id, interface, *state).await;
                Some(handle.into())
            }

            WorkflowState::Errored(state) => {
                let handle = ErroredWorkflowHandle::new(
                    self,
                    workflow_id,
                    state.error,
                    state.erroneous_messages,
                );
                Some(handle.into())
            }

            WorkflowState::Completed(state) => {
                let handle = CompletedWorkflowHandle::new(workflow_id, state);
                Some(handle.into())
            }
        }
    }

    /// Returns the number of active workflows.
    pub async fn workflow_count(&self) -> usize {
        let transaction = self.storage.readonly_transaction().await;
        transaction.count_active_workflows().await
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
        let channel = transaction.channel(channel_id).await.unwrap();
        if channel.is_closed {
            return Err(SendError::Closed);
        }

        transaction.push_messages(channel_id, vec![message]).await;
        transaction.commit().await;
        Ok(())
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
    #[tracing::instrument(skip(self))]
    async fn abort_workflow(&'a self, workflow_id: WorkflowId) -> Result<(), ConcurrencyError> {
        let mut transaction = self.storage.transaction().await;
        let record = transaction
            .workflow_for_update(workflow_id)
            .await
            .ok_or_else(ConcurrencyError::new)?;
        let (parent_id, mut persisted) = match record.state {
            WorkflowState::Active(state) => (record.parent_id, state.persisted),
            WorkflowState::Errored(state) => (record.parent_id, state.persisted),
            WorkflowState::Completed(_) => return Err(ConcurrencyError::new()),
        };

        persisted.abort();
        let spans = PersistedSpans::default();
        StorageHelper::new(&mut transaction)
            .persist_workflow(workflow_id, parent_id, persisted, spans)
            .await;
        transaction.commit().await;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn repair_workflow(&'a self, workflow_id: WorkflowId) -> Result<(), ConcurrencyError> {
        let mut transaction = self.storage.transaction().await;
        let record = transaction
            .workflow_for_update(workflow_id)
            .await
            .ok_or_else(ConcurrencyError::new)?;
        let record = record.into_errored().ok_or_else(ConcurrencyError::new)?;
        let repaired_state = record.state.repair();
        transaction
            .update_workflow(workflow_id, repaired_state.into())
            .await;
        transaction.commit().await;
        Ok(())
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
