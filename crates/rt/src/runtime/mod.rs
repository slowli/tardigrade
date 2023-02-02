//! [`Runtime`] and tightly related types.
//!
//! See `Runtime` docs for an overview and examples of usage.

use chrono::{DateTime, Utc};
use futures::lock::{Mutex, MutexGuard};
use lru::LruCache;
use tracing_tunnel::{LocalSpans, PersistedMetadata};

use std::{collections::HashMap, fmt, num::NonZeroUsize, sync::Arc};

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
    stubs::{MapFormat, RuntimeSpawner},
    tick::{TickResult, WorkflowTickError, WouldBlock},
    traits::AsRuntime,
};

use crate::{
    engine::{DefineWorkflow, WorkflowEngine, WorkflowModule},
    handle::StorageRef,
    storage::{
        helper::StorageHelper, CommitStream, DefinitionRecord, ModuleRecord, ReadModules, Storage,
        StorageTransaction, Streaming, TransactionAsStorage, WriteModules,
    },
    workflow::Workflow,
};
use tardigrade::{Raw, WorkflowId};

#[derive(Debug)]
struct CachedDefinitions<D> {
    // We wrap each definition in `Arc` to be able to easily clone it. This is fine since
    // definitions are immutable.
    inner: LruCache<String, Arc<D>>,
}

impl CachedDefinitions<()> {
    fn full_id(module_id: &str, name_in_module: &str) -> String {
        format!("{module_id}::{name_in_module}")
    }

    fn split_full_id(full_id: &str) -> Option<(&str, &str)> {
        full_id.split_once("::")
    }
}

impl<D> CachedDefinitions<D> {
    fn get(&mut self, definition_id: &str) -> Option<&Arc<D>> {
        self.inner.get(definition_id)
    }

    fn insert(&mut self, definition_id: String, definition: Arc<D>) {
        self.inner.push(definition_id, definition);
    }
}

#[derive(Debug)]
struct Definitions<'a, E: WorkflowEngine> {
    cached: MutexGuard<'a, CachedDefinitions<E::Definition>>,
    engine: &'a E,
}

impl<E: WorkflowEngine> Definitions<'_, E> {
    #[tracing::instrument(
        level = "debug",
        skip(self, transaction),
        fields(self.len = self.cached.inner.len())
    )]
    async fn get(
        &mut self,
        definition_id: &str,
        transaction: &impl ReadModules,
    ) -> Option<Arc<E::Definition>> {
        if let Some(def) = self.cached.get(definition_id) {
            tracing::debug!(is_cached = true, "accessing definition cache");
            return Some(Arc::clone(def));
        }
        tracing::debug!(is_cached = false, "accessing definition cache");
        let (module_id, name_in_module) = CachedDefinitions::split_full_id(definition_id)?;
        let span = tracing::debug_span!("restore_module", module_id);
        let entered = span.enter();

        let module = transaction.module(module_id).await?;
        module.definitions.get(name_in_module)?;
        // ^ Do not waste time on restoring the module if the `name_in_module` is bogus.

        let module = self.engine.restore_module(&module).await;
        let module = match module {
            Ok(module) => module,
            Err(err) => {
                tracing::warn!(%err, module_id, "cannot restore persisted module");
                return None;
            }
        };
        drop(entered);

        let definition = module
            .into_iter()
            .find_map(|(name, def)| (name == name_in_module).then_some(def))?;
        let definition = Arc::new(definition);
        self.cached
            .insert(definition_id.to_owned(), Arc::clone(&definition));
        Some(definition)
    }
}

#[derive(Debug)]
struct CachedWorkflow<I> {
    inner: Workflow<I>,
    execution_count: u64,
}

/// In-memory LRU cache for recently executed `Workflow`s.
///
/// # Assumptions
///
/// - Workflows are never modified outside of `Runtime` methods (e.g., no manual storage
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
    fn insert(&mut self, id: WorkflowId, workflow: Workflow<I>, execution_count: u64) {
        self.inner.push(
            id,
            CachedWorkflow {
                inner: workflow,
                execution_count,
            },
        );
    }

    #[tracing::instrument(level = "debug", skip(self), fields(self.len = self.inner.len()))]
    fn take(&mut self, id: WorkflowId, execution_count: u64) -> Option<Workflow<I>> {
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

/// Part of the `Runtime` not tied to the storage.
///
/// We need this as a separate type to make storage easily swappable (e.g., to implement
/// driving the runtime or transactional operations).
#[derive(Debug)]
struct RuntimeInner<E: WorkflowEngine, C> {
    engine: E,
    clock: Arc<C>,
    definitions: Mutex<CachedDefinitions<E::Definition>>,
    cached_workflows: Mutex<CachedWorkflows<E::Instance>>,
    local_spans: Mutex<HashMap<WorkflowId, LocalSpans>>,
}

impl<E: WorkflowEngine, C: Clock> RuntimeInner<E, C> {
    fn new(engine: E, clock: C, workflow_cache_capacity: NonZeroUsize) -> Self {
        let definitions = CachedDefinitions {
            inner: LruCache::unbounded(), // TODO: support bounded cache
        };

        Self {
            engine,
            clock: Arc::new(clock),
            definitions: Mutex::new(definitions),
            cached_workflows: Mutex::new(CachedWorkflows::new(workflow_cache_capacity)),
            local_spans: Mutex::default(),
        }
    }

    async fn definitions(&self) -> Definitions<'_, E> {
        Definitions {
            cached: self.definitions.lock().await,
            engine: &self.engine,
        }
    }
}

/// Runtime for workflow modules, workflows and channels.
///
/// A workflow runtime is responsible for managing the state and interfacing with workflows
/// and channels connected to the workflows. In particular, a runtime supports the following
/// operations:
///
/// - Spawning new workflows (including from the workflow code) using a [`RuntimeSpawner`] handle
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
/// #     handle::WorkflowHandle, runtime::Runtime, storage::LocalStorage, AsyncIoScheduler,
/// # };
/// #
/// # async fn test_wrapper(module: WasmtimeModule) -> anyhow::Result<()> {
/// // A runtime is instantiated using the builder pattern:
/// let storage = LocalStorage::default();
/// let runtime = Runtime::builder(Wasmtime::default(), storage)
///     .with_clock(AsyncIoScheduler)
///     .build();
/// // After this, modules may be added:
/// let module: WasmtimeModule = // ...
/// #   module;
/// runtime.insert_module("test", module).await;
///
/// // After that, new workflows can be spawned using `ManageWorkflowsExt`
/// // trait from the `tardigrade` crate:
/// use tardigrade::spawn::CreateWorkflow;
///
/// let spawner = runtime.spawner();
/// let definition_id = "test::Workflow";
/// // ^ The definition ID is the ID of the module and the name of a workflow
/// //   within the module separated by `::`.
/// let args = b"test_args".to_vec();
/// let builder = spawner.new_workflow::<()>(definition_id).await?;
/// let (handles, self_handles) = builder.handles(|_| {}).await;
/// let mut workflow = builder.build(args, handles).await?;
/// // Do something with `workflow`, e.g., write something to its channels
/// // using `self_handles`...
///
/// // Initialize the workflow:
/// let receipt = runtime.tick().await?.into_inner()?;
/// println!("{receipt:?}");
/// // Check the workflow state:
/// workflow.update().await?;
/// assert!(workflow.persisted().is_initialized());
/// # Ok(())
/// # }
/// ```
pub struct Runtime<E: WorkflowEngine, C, S> {
    inner: Arc<RuntimeInner<E, C>>,
    storage: S,
}

impl<E, C, S> fmt::Debug for Runtime<E, C, S>
where
    E: WorkflowEngine + fmt::Debug,
    E::Instance: fmt::Debug,
    C: fmt::Debug,
    S: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Runtime")
            .field("inner", &self.inner)
            .field("storage", &self.storage)
            .finish()
    }
}

impl<E: WorkflowEngine, C: Clock, S: Storage + Clone> Clone for Runtime<E, C, S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            storage: self.storage.clone(),
        }
    }
}

#[allow(clippy::mismatching_type_param_order, clippy::missing_panics_doc)] // false positive
impl<E: WorkflowEngine, S: Storage> Runtime<E, (), S> {
    /// Creates a builder that will use the specified storage.
    pub fn builder(engine: E, storage: S) -> RuntimeBuilder<E, (), S> {
        RuntimeBuilder {
            engine,
            clock: (),
            storage,
            workflow_cache_capacity: NonZeroUsize::new(16).unwrap(),
        }
    }
}

impl<E: WorkflowEngine, C: Clock, S: Storage> Runtime<E, C, S> {
    fn new(engine: E, clock: C, storage: S, workflow_cache_capacity: NonZeroUsize) -> Self {
        Self {
            inner: Arc::new(RuntimeInner::new(engine, clock, workflow_cache_capacity)),
            storage,
        }
    }

    /// Inserts the specified module into the runtime.
    ///
    /// # Panics
    ///
    /// Panics if the module `id` contains ineligible chars, such as `:`.
    #[tracing::instrument(skip(self, module), ret)]
    pub async fn insert_module(&self, id: &str, module: E::Module) -> ModuleRecord {
        assert!(!id.contains(':'), "module ID contains ineligible char `:`");

        let bytes = module.bytes();
        let mut definitions = HashMap::new();
        let mut cached_definitions = self.inner.definitions.lock().await;
        for (name, def) in module {
            definitions.insert(
                name.clone(),
                DefinitionRecord {
                    interface: def.interface().clone(),
                },
            );
            let definition_id = CachedDefinitions::full_id(id, &name);
            cached_definitions.insert(definition_id, Arc::new(def));
        }
        drop(cached_definitions);

        let module_record = ModuleRecord {
            id: id.to_owned(),
            bytes,
            definitions,
            tracing_metadata: PersistedMetadata::default(),
        };
        let mut transaction = self.storage.transaction().await;
        transaction.insert_module(module_record.clone()).await;
        transaction.commit().await;
        module_record
    }

    /// Returns a reference to the execution engine.
    pub fn engine(&self) -> &E {
        &self.inner.engine
    }

    /// Returns a reference to the underlying storage.
    pub fn storage(&self) -> StorageRef<'_, S> {
        StorageRef::from(&self.storage)
    }

    #[doc(hidden)] // not mature for external usage yet
    pub fn clock(&self) -> &C {
        self.inner.clock.as_ref()
    }

    #[doc(hidden)] // not mature for external usage yet
    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    /// Returns the encapsulated storage.
    pub fn into_storage(self) -> S {
        self.storage
    }

    /// Creates a storage transaction and uses it for the further operations on the runtime
    /// (e.g., creating new workflows / channels). Beware of [`TransactionAsStorage`] drawbacks
    /// documented in the type docs.
    ///
    /// The original transaction can be extracted back using [`Self::into_storage()`] and
    /// then [`TransactionAsStorage::into_inner()`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use tardigrade::spawn::CreateWorkflow;
    /// # use tardigrade_rt::{
    /// #     engine::Wasmtime, runtime::Runtime,
    /// #     storage::{LocalStorage, StorageTransaction},
    /// # };
    /// #
    /// # async fn test_wrapper() -> anyhow::Result<()> {
    /// let storage = LocalStorage::default();
    /// let mut runtime = Runtime::builder(Wasmtime::default(), storage)
    ///     .build();
    /// // Obtain a runtime operating in a transaction
    /// let tx_runtime = runtime.in_transaction().await;
    /// // Create a workflow and channels for it in a transaction
    /// let spawner = tx_runtime.spawner();
    /// let builder = spawner.new_workflow::<()>("test::Workflow").await?;
    /// let (handles, _) = builder.handles(|_| {}).await;
    /// builder.build(vec![], handles).await?;
    ///
    /// // Unwrap the transaction and commit it.
    /// if let Some(tx) = tx_runtime.into_storage().into_inner() {
    ///     tx.commit().await;
    /// }
    /// // If the transaction is not committed, the workflow and all channels created
    /// // for it are gone.
    /// # Ok(())
    /// # }
    /// ```
    pub async fn in_transaction(
        &mut self,
    ) -> Runtime<E, C, TransactionAsStorage<S::Transaction<'_>>> {
        let transaction = self.storage.transaction().await;
        Runtime {
            inner: Arc::clone(&self.inner),
            storage: transaction.into(),
        }
    }

    /// Returns a spawner handle that can be used to create new workflows.
    pub fn spawner(&self) -> RuntimeSpawner<'_, Self> {
        RuntimeSpawner::new(self)
    }

    /// Returns a *raw* spawner handle that can be used to create new workflows.
    /// In contrast to [`Self::spawner()`], the returned spawner uses channel IDs
    /// rather than channel handles to create workflows, and returns the ID of a created workflow
    /// rather than its handle.
    pub fn raw_spawner(&self) -> RuntimeSpawner<'_, Self, Raw> {
        RuntimeSpawner::new(self)
    }

    /// Sets the current time for this runtime. This may expire timers in some of the contained
    /// workflows.
    #[tracing::instrument(skip(self))]
    pub async fn set_current_time(&self, time: DateTime<Utc>) {
        let mut transaction = self.storage.transaction().await;
        StorageHelper::new(&mut transaction)
            .set_current_time(time)
            .await;
        transaction.commit().await;
    }
}

impl<E, C, S> Runtime<E, C, Streaming<S>>
where
    E: WorkflowEngine,
    C: Schedule,
    S: Storage + Clone,
{
    /// Drives this runtime using the provided config.
    pub async fn drive(self, commits_rx: &mut CommitStream, config: DriveConfig) -> Termination {
        config.run(self, commits_rx).await
    }
}

/// Builder for a [`Runtime`].
#[derive(Debug)]
pub struct RuntimeBuilder<E, C, S> {
    engine: E,
    clock: C,
    storage: S,
    workflow_cache_capacity: NonZeroUsize,
}

#[allow(clippy::mismatching_type_param_order)] // false positive
impl<E: WorkflowEngine, S: Storage> RuntimeBuilder<E, (), S> {
    /// Sets the wall clock to be used in the runtime.
    #[must_use]
    pub fn with_clock<C: Clock>(self, clock: C) -> RuntimeBuilder<E, C, S> {
        RuntimeBuilder {
            engine: self.engine,
            clock,
            storage: self.storage,
            workflow_cache_capacity: self.workflow_cache_capacity,
        }
    }
}

impl<E: WorkflowEngine, C: Clock, S: Storage> RuntimeBuilder<E, C, S> {
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

    /// Finishes building the runtime.
    pub fn build(self) -> Runtime<E, C, S> {
        Runtime::new(
            self.engine,
            self.clock,
            self.storage,
            self.workflow_cache_capacity,
        )
    }
}
