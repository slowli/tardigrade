//! [`WorkflowManager`] and tightly related types.
//!
//! See `WorkflowManager` docs for an overview and examples of usage.

use chrono::{DateTime, Utc};
use futures::{lock::Mutex, StreamExt};
use lru::LruCache;
use tracing_tunnel::{LocalSpans, PersistedMetadata};

use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

mod driver;
mod services;
mod stubs;
mod tick;
mod traits;

#[cfg(test)]
pub(crate) mod tests;

pub(crate) use self::services::{Services, StashStub};
pub use self::{
    driver::{DriveConfig, Termination},
    services::{Clock, Schedule, TimerFuture},
    stubs::ManagerSpawner,
    tick::{TickResult, WouldBlock},
    traits::AsManager,
};

use crate::{
    engine::{DefineWorkflow, WorkflowEngine, WorkflowModule},
    handle::StorageRef,
    storage::{
        helper::StorageHelper, CommitStream, DefinitionRecord, ModuleRecord, ReadModules, Storage,
        StorageTransaction, Streaming, WriteModules,
    },
    workflow::Workflow,
};
use tardigrade::WorkflowId;

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

#[derive(Debug)]
struct CachedWorkflow<I> {
    inner: Workflow<I>,
    execution_count: usize,
}

/// In-memory LRU cache for recently executed `Workflow`s.
///
/// # Assumptions
///
/// - Workflows are never modified outside of `WorkflowManager` methods (e.g., no manual storage
///   edits). This is used when checking cache staleness.
/// - A specific workflow is never executed concurrently. This should be enforced by the storage
///   implementation.
#[derive(Debug)]
struct CachedWorkflows<I> {
    inner: LruCache<WorkflowId, CachedWorkflow<I>>,
}

impl<I> CachedWorkflows<I> {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            inner: LruCache::new(capacity),
        }
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, workflow),
        fields(self.len = self.inner.len())
    )]
    fn insert(&mut self, id: WorkflowId, workflow: Workflow<I>, execution_count: usize) {
        self.inner.push(
            id,
            CachedWorkflow {
                inner: workflow,
                execution_count,
            },
        );
    }

    #[tracing::instrument(level = "debug", skip(self), fields(self.len = self.inner.len()))]
    fn take(&mut self, id: WorkflowId, execution_count: usize) -> Option<Workflow<I>> {
        let cached = self.inner.pop(&id);
        tracing::debug!(is_cached = cached.is_some(), "accessing workflow cache");
        let cached = cached?;
        if cached.execution_count == execution_count {
            Some(cached.inner)
        } else {
            tracing::info!(
                id,
                cached.execution_count,
                execution_count,
                "cached workflow is stale"
            );
            None
        }
    }
}

/// Manager for workflow modules, workflows and channels.
///
/// A workflow manager is responsible for managing the state and interfacing with workflows
/// and channels connected to the workflows. In particular, a manager supports the following
/// operations:
///
/// - Spawning new workflows (including from the workflow code) using a [`ManagerSpawner`] handle
///   obtained via [`Self::spawner()`]
/// - Manipulating channels (writing / reading messages) and workflows using a [`StorageRef`] handle
///   obtained via [`Self::storage()`]
/// - Driving the contained workflows to completion, either [manually](Self::tick()) or
///   using a [driver](Self::drive()) if the underlying storage
///   [supports event streaming](Streaming)
///
/// # Workflow lifecycle
///
/// A workflow can be in one of the following states:
///
/// - [**Active.**](crate::handle::WorkflowHandle) This is the initial state,
///   provided that the workflow successfully initialized. In this state, the workflow can execute,
///   receive and send messages etc.
/// - [**Completed.**](crate::handle::CompletedWorkflowHandle) This is the terminal state
///   after the workflow completes as a result of its execution or is aborted.
/// - [**Errored.**](crate::handle::ErroredWorkflowHandle) A workflow reaches this state
///   after it panics.
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
/// #     handle::WorkflowHandle, manager::WorkflowManager, storage::LocalStorage, AsyncIoScheduler,
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
/// use tardigrade::spawn::CreateWorkflow;
///
/// let spawner = manager.spawner();
/// let definition_id = "test::Workflow";
/// // ^ The definition ID is the ID of the module and the name of a workflow
/// //   within the module separated by `::`.
/// let args = b"test_args".to_vec();
/// let builder = spawner.new_workflow::<()>(definition_id)?;
/// let (handles, self_handles) = builder.handles(|_| {}).await;
/// let mut workflow = builder.build(args, handles).await?;
/// // Do something with `workflow`, e.g., write something to its channels
/// // using `self_handles`...
///
/// // Initialize the workflow:
/// let receipt = manager.tick().await?.into_inner()?;
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
    cached_workflows: Mutex<CachedWorkflows<E::Instance>>,
    local_spans: Mutex<HashMap<WorkflowId, LocalSpans>>,
}

#[allow(clippy::mismatching_type_param_order, clippy::missing_panics_doc)] // false positive
impl<E: WorkflowEngine, S: Storage> WorkflowManager<E, (), S> {
    /// Creates a builder that will use the specified storage.
    pub fn builder(engine: E, storage: S) -> WorkflowManagerBuilder<E, (), S> {
        WorkflowManagerBuilder {
            engine,
            clock: (),
            storage,
            workflow_cache_capacity: NonZeroUsize::new(16).unwrap(),
        }
    }
}

impl<E: WorkflowEngine, C: Clock, S: Storage> WorkflowManager<E, C, S> {
    async fn new(
        engine: E,
        clock: C,
        storage: S,
        workflow_cache_capacity: NonZeroUsize,
    ) -> anyhow::Result<Self> {
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
            cached_workflows: Mutex::new(CachedWorkflows::new(workflow_cache_capacity)),
            local_spans: Mutex::default(),
        })
    }

    /// Inserts the specified module into the manager.
    pub async fn insert_module(&mut self, id: &str, module: E::Module) {
        let bytes = module.bytes();
        let mut definitions = HashMap::new();
        for (name, def) in module {
            definitions.insert(
                name.clone(),
                DefinitionRecord {
                    interface: def.interface().clone(),
                },
            );
            let self_definitions = Arc::get_mut(&mut self.definitions).expect("leaked definitions");
            self_definitions.insert(id.to_owned(), name, def);
        }

        let module_record = ModuleRecord {
            id: id.to_owned(),
            bytes,
            definitions,
            tracing_metadata: PersistedMetadata::default(),
        };
        let mut transaction = self.storage.transaction().await;
        transaction.insert_module(module_record).await;
        transaction.commit().await;
    }

    /// Returns a reference to the underlying storage.
    pub fn storage(&self) -> StorageRef<'_, S> {
        StorageRef::from(&self.storage)
    }

    /// Returns a spawner handle that can be used to create new workflows.
    pub fn spawner(&self) -> ManagerSpawner<'_, Self> {
        ManagerSpawner::new(self)
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

    /// Sets the current time for this manager. This may expire timers in some of the contained
    /// workflows.
    #[tracing::instrument(skip(self))]
    pub async fn set_current_time(&self, time: DateTime<Utc>) {
        self.do_set_current_time(&self.storage, time).await;
    }

    pub(super) async fn do_set_current_time(&self, storage: &S, time: DateTime<Utc>) {
        let mut transaction = storage.transaction().await;
        StorageHelper::new(&mut transaction)
            .set_current_time(time)
            .await;
        transaction.commit().await;
    }
}

impl<E, C, S> WorkflowManager<E, C, Streaming<S>>
where
    E: WorkflowEngine,
    C: Schedule,
    S: Storage + Clone,
{
    /// Drives this manager using the provided config.
    pub async fn drive(&self, commits_rx: &mut CommitStream, config: DriveConfig) -> Termination {
        config.run(self, commits_rx).await
    }
}

/// Builder for a [`WorkflowManager`].
#[derive(Debug)]
pub struct WorkflowManagerBuilder<E, C, S> {
    engine: E,
    clock: C,
    storage: S,
    workflow_cache_capacity: NonZeroUsize,
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
            workflow_cache_capacity: self.workflow_cache_capacity,
        }
    }
}

impl<E: WorkflowEngine, C: Clock, S: Storage> WorkflowManagerBuilder<E, C, S> {
    /// Sets the capacity of the LRU cache of the recently executed workflows. Depending
    /// on the workflow engine, caching workflows can significantly speed up workflow execution.
    ///
    /// # Panics
    ///
    /// Panics if the provided capacity is 0.
    #[must_use]
    pub fn cache_workflows(mut self, capacity: usize) -> Self {
        self.workflow_cache_capacity =
            NonZeroUsize::new(capacity).expect("cannot set workflow cache capacity to 0");
        self
    }

    /// Finishes building the manager.
    ///
    /// # Errors
    ///
    /// Returns an error if [module instantiation](WorkflowEngine::create_module()) fails.
    pub async fn build(self) -> anyhow::Result<WorkflowManager<E, C, S>> {
        let manager_future = WorkflowManager::new(
            self.engine,
            self.clock,
            self.storage,
            self.workflow_cache_capacity,
        );
        manager_future.await
    }
}
