//! [`WorkflowManager`] and tightly related types.
//!
//! See `WorkflowManager` docs for an overview and examples of usage.

use chrono::{DateTime, Utc};
use futures::{lock::Mutex, StreamExt};
use tracing_tunnel::{LocalSpans, PersistedMetadata, PersistedSpans};

use std::{collections::HashMap, convert::Infallible, sync::Arc};

mod handle;
mod new_workflows;
mod persistence;
mod services;
mod tick;
mod traits;

#[cfg(test)]
pub(crate) mod tests;

pub(crate) use self::services::{Services, StashWorkflow};
pub use self::{
    handle::{
        AnyWorkflowHandle, CompletedWorkflowHandle, ConcurrencyError, ErroneousMessage,
        ErroredWorkflowHandle, MessageReceiver, MessageSender, ReceivedMessage, WorkflowHandle,
    },
    services::{Clock, Schedule, TimerFuture},
    tick::{TickResult, WouldBlock},
    traits::AsManager,
};

use self::persistence::StorageHelper;
use crate::{
    engine::{DefineWorkflow, WorkflowEngine, WorkflowModule},
    storage::{
        ChannelRecord, ModuleRecord, ReadChannels, ReadModules, ReadWorkflows, Storage,
        StorageTransaction, WorkflowState, WriteChannels, WriteModules, WriteWorkflows,
    },
};
use tardigrade::{
    channel::SendError,
    workflow::{HandleFormat, IntoRaw, TryFromRaw},
    ChannelId, Codec, WorkflowId,
};

#[derive(Debug)]
struct WorkflowDefinitions<D> {
    inner: HashMap<String, HashMap<String, D>>,
}

impl<D> Default for WorkflowDefinitions<D> {
    fn default() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
}

impl<D> WorkflowDefinitions<D> {
    fn get(&self, module_id: &str, name_in_module: &str) -> &D {
        &self.inner[module_id][name_in_module]
    }

    fn split_full_id(full_id: &str) -> Option<(&str, &str)> {
        full_id.split_once("::")
    }

    fn for_full_id(&self, full_id: &str) -> Option<&D> {
        let (module_id, name_in_module) = Self::split_full_id(full_id)?;
        self.inner.get(module_id)?.get(name_in_module)
    }

    fn insert(&mut self, module_id: String, name_in_module: String, definition: D) {
        let module_entry = self.inner.entry(module_id).or_default();
        module_entry.insert(name_in_module, definition);
    }
}

/// Part of the manager used during workflow instantiation.
#[derive(Debug, Clone)]
struct Shared<D> {
    clock: Arc<dyn Clock>,
    definitions: Arc<WorkflowDefinitions<D>>,
}

#[derive(Debug, Clone, Copy)]
enum ChannelSide {
    HostSender,
    WorkflowSender(WorkflowId),
    HostReceiver,
    Receiver(WorkflowId),
}

/// Host [format](HandleFormat) for channel handles.
#[derive(Debug)]
pub struct Host(());

impl HandleFormat for Host {
    type RawReceiver = HostChannelId;
    type Receiver<T, C: Codec<T>> = HostChannelId;
    type RawSender = HostChannelId;
    type Sender<T, C: Codec<T>> = HostChannelId;
}

// FIXME: use sender / receiver handles?
/// Host channel ID.
#[derive(Debug)]
pub struct HostChannelId(ChannelId);

impl HostChannelId {
    /// Channel ID for a closed channel.
    pub const fn closed() -> Self {
        Self(0)
    }

    pub(crate) fn unchecked(id: ChannelId) -> Self {
        Self(id)
    }
}

impl From<HostChannelId> for ChannelId {
    fn from(channel_id: HostChannelId) -> Self {
        channel_id.0
    }
}

impl TryFromRaw<Self> for HostChannelId {
    type Error = Infallible;

    fn try_from_raw(raw: Self) -> Result<Self, Self::Error> {
        Ok(raw)
    }
}

impl IntoRaw<Self> for HostChannelId {
    fn into_raw(self) -> Self {
        self
    }
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
/// #     engine::{Wasmtime, WasmtimeModule},
/// #     manager::{WorkflowHandle, WorkflowManager},
/// #     storage::LocalStorage, AsyncIoScheduler,
/// # };
/// #
/// # async fn test_wrapper(module: WasmtimeModule) -> anyhow::Result<()> {
/// // A manager is instantiated using the builder pattern:
/// let storage = LocalStorage::default();
/// let mut manager = WorkflowManager::builder(Wasmtime::default(), storage)
///     .with_clock(AsyncIoScheduler)
///     .build()
///     .await?;
/// // After this, modules may be added:
/// let module: WasmtimeModule = // ...
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
pub struct WorkflowManager<E: WorkflowEngine, C, S> {
    pub(crate) clock: Arc<C>,
    pub(crate) storage: S,
    definitions: Arc<WorkflowDefinitions<E::Definition>>,
    local_spans: Mutex<HashMap<WorkflowId, LocalSpans>>,
}

#[allow(clippy::mismatching_type_param_order)] // false positive
impl<E: WorkflowEngine, S: Storage> WorkflowManager<E, (), S> {
    /// Creates a builder that will use the specified storage.
    pub fn builder(engine: E, storage: S) -> WorkflowManagerBuilder<E, (), S> {
        WorkflowManagerBuilder {
            engine,
            clock: (),
            storage,
        }
    }
}

impl<E: WorkflowEngine, C: Clock, S: Storage> WorkflowManager<E, C, S> {
    async fn new(engine: E, clock: C, storage: S) -> anyhow::Result<Self> {
        let definitions = {
            let transaction = storage.readonly_transaction().await;
            let mut definitions = WorkflowDefinitions::default();
            let mut module_records = transaction.modules();
            while let Some(record) = module_records.next().await {
                let module = engine.create_module(&record).await?;
                for (name, def) in module {
                    definitions.insert(record.id.clone(), name, def);
                }
            }
            definitions
        };

        Ok(Self {
            clock: Arc::new(clock),
            definitions: Arc::new(definitions),
            storage,
            local_spans: Mutex::default(),
        })
    }

    /// Inserts the specified module into the manager.
    pub async fn insert_module(&mut self, id: &str, module: E::Module) {
        let mut transaction = self.storage.transaction().await;
        let module_record = ModuleRecord {
            id: id.to_owned(),
            bytes: module.bytes(),
            tracing_metadata: PersistedMetadata::default(),
        };
        transaction.insert_module(module_record).await;

        for (name, def) in module {
            let definitions = Arc::get_mut(&mut self.definitions).expect("leaked definitions");
            definitions.insert(id.to_owned(), name, def);
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
                    .definitions
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

    fn shared(&self) -> Shared<E::Definition> {
        Shared {
            clock: Arc::clone(&self.clock) as Arc<dyn Clock>,
            definitions: Arc::clone(&self.definitions),
        }
    }

    pub(crate) async fn send_message(
        &self,
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

    pub(crate) async fn close_host_sender(&self, channel_id: ChannelId) {
        let mut transaction = self.storage.transaction().await;
        StorageHelper::new(&mut transaction)
            .close_channel_side(channel_id, ChannelSide::HostSender)
            .await;
        transaction.commit().await;
    }

    pub(crate) async fn close_host_receiver(&self, channel_id: ChannelId) {
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
            .close_channel_side(channel_id, ChannelSide::HostReceiver)
            .await;
        transaction.commit().await;
    }

    /// Sets the current time for this manager. This may expire timers in some of the contained
    /// workflows.
    #[tracing::instrument(skip(self))]
    pub async fn set_current_time(&self, time: DateTime<Utc>) {
        let mut transaction = self.storage.transaction().await;
        StorageHelper::new(&mut transaction)
            .set_current_time(time)
            .await;
        transaction.commit().await;
    }

    /// Aborts the workflow with the specified ID. The parent workflow, if any, will be notified,
    /// and all channel handles owned by the workflow will be properly disposed.
    #[tracing::instrument(skip(self))]
    async fn abort_workflow(&self, workflow_id: WorkflowId) -> Result<(), ConcurrencyError> {
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
    async fn repair_workflow(&self, workflow_id: WorkflowId) -> Result<(), ConcurrencyError> {
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
pub struct WorkflowManagerBuilder<E, C, S> {
    engine: E,
    clock: C,
    storage: S,
}

#[allow(clippy::mismatching_type_param_order)] // false positive
impl<E: WorkflowEngine, S: Storage> WorkflowManagerBuilder<E, (), S> {
    /// Sets the wall clock to be used in the manager.
    #[must_use]
    pub fn with_clock<C: Clock>(self, clock: C) -> WorkflowManagerBuilder<E, C, S> {
        WorkflowManagerBuilder {
            engine: self.engine,
            clock,
            storage: self.storage,
        }
    }
}

impl<E: WorkflowEngine, C: Clock, S: Storage> WorkflowManagerBuilder<E, C, S> {
    /// Finishes building the manager.
    ///
    /// # Errors
    ///
    /// Returns an error if [module instantiation](WorkflowEngine::create_module()) fails.
    pub async fn build(self) -> anyhow::Result<WorkflowManager<E, C, S>> {
        WorkflowManager::new(self.engine, self.clock, self.storage).await
    }
}
