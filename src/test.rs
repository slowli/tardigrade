//! Utils for workflow testing.
//!
//! This emulates imports / exports provided to WASM workflow modules by the Tardigrade runtime,
//! allowing to test workflow logic without launching a runtime (or even including
//! the corresponding crate as a dev dependency).
//!
//! # Examples
//!
//! ```
//! # use std::time::Duration;
//! # use assert_matches::assert_matches;
//! # use futures::{FutureExt, SinkExt, StreamExt};
//! # use serde::{Deserialize, Serialize};
//! # use tardigrade::{
//! #     channel::Sender,
//! #     workflow::{Handle, GetInterface, SpawnWorkflow, TaskHandle, Wasm, WorkflowFn},
//! #     Json,
//! # };
//! // Assume we want to test a workflow.
//! #[derive(Debug, GetInterface)]
//! # #[tardigrade(interface = r#"{
//! #     "v": 0,
//! #     "out": { "events": {} }
//! # }"#)]
//! pub struct MyWorkflow(());
//!
//! /// Workflow handle.
//! #[tardigrade::handle(for = "MyWorkflow")]
//! #[derive(Debug)]
//! pub struct MyHandle<Env> {
//!     pub events: Handle<Sender<Event, Json>, Env>,
//! }
//!
//! /// Arguments provided to the workflow on creation.
//! #[derive(Debug, Serialize, Deserialize)]
//! pub struct Args {
//!     pub counter: u32,
//! }
//!
//! /// Events emitted by the workflow.
//! #[derive(Debug, Serialize, Deserialize)]
//! pub enum Event {
//!     Count(u32),
//! }
//!
//! impl WorkflowFn for MyWorkflow {
//!     type Args = Args;
//!     type Codec = Json;
//! }
//!
//! // Workflow logic
//! impl SpawnWorkflow for MyWorkflow {
//!     fn spawn(args: Args, handle: MyHandle<Wasm>) -> TaskHandle {
//!         let counter = args.counter;
//!         let mut events = handle.events;
//!         TaskHandle::new(async move {
//!             for i in 0..counter {
//!                 tardigrade::sleep(Duration::from_millis(100)).await;
//!                 events.send(Event::Count(i)).await.ok();
//!             }
//!         })
//!     }
//! }
//!
//! // We can test the workflow as follows:
//! use tardigrade::{spawn::RemoteWorkflow, test::{Runtime, Timers}};
//!
//! async fn test_workflow(handle: MyHandle<RemoteWorkflow>) {
//!     // The workflow should be waiting for a timer to emit an event.
//!     let mut events = handle.events.unwrap();
//!     assert!(events.next().now_or_never().is_none());
//!
//!     let now = Timers::now();
//!     let new_now = Timers::next_timer_expiration().unwrap();
//!     assert_eq!((new_now - now).num_milliseconds(), 100);
//!     Timers::set_now(new_now);
//!     let event = events.next().await.unwrap();
//!     assert_matches!(event, Event::Count(0));
//! }
//! Runtime::default().test::<MyWorkflow, _, _>(
//!     Args { counter: 1 },
//!     test_workflow,
//! );
//! ```

use chrono::{DateTime, TimeZone, Utc};
use futures::{
    channel::oneshot,
    executor::{LocalPool, LocalSpawner},
    future::RemoteHandle,
    task::LocalSpawnExt,
};

use std::{
    cell::RefCell,
    cmp,
    collections::{BinaryHeap, HashMap},
    fmt,
    future::Future,
};

use crate::workflow::UntypedHandle;
use crate::{
    interface::Interface,
    spawn::{ManageWorkflowsExt, RemoteWorkflow, Spawner, WorkflowBuilder, Workflows},
    workflow::{Handle, SpawnWorkflow, TakeHandle, TaskHandle, Wasm},
};

#[derive(Debug)]
struct TimerEntry {
    expires_at: DateTime<Utc>,
    notifier: oneshot::Sender<DateTime<Utc>>,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.expires_at == other.expires_at
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.expires_at.cmp(&other.expires_at).reverse()
    }
}

/// Mock workflow scheduler.
#[derive(Debug)]
pub struct MockScheduler {
    now: DateTime<Utc>,
    timers: BinaryHeap<TimerEntry>,
}

impl Default for MockScheduler {
    fn default() -> Self {
        Self::new(Utc.timestamp(0, 0))
    }
}

impl MockScheduler {
    /// Creates a scheduler with the specified current timestamp.
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Self::floor_timestamp(now),
            timers: BinaryHeap::new(),
        }
    }

    /// Approximates `timestamp` to be presentable as an integer number of milliseconds since
    /// Unix epoch. This emulates WASM interface which uses millisecond precision.
    fn floor_timestamp(ts: DateTime<Utc>) -> DateTime<Utc> {
        Utc.timestamp_millis(ts.timestamp_millis())
    }

    /// Returns the expiration for the nearest timer, or `None` if there are no active timers.
    pub fn next_timer_expiration(&self) -> Option<DateTime<Utc>> {
        self.timers.peek().map(|timer| timer.expires_at)
    }

    /// Inserts a timer into this scheduler.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn insert_timer(&mut self, expires_at: DateTime<Utc>) -> oneshot::Receiver<DateTime<Utc>> {
        let expires_at = Self::floor_timestamp(expires_at);
        let (sx, rx) = oneshot::channel();
        if expires_at > self.now {
            self.timers.push(TimerEntry {
                expires_at,
                notifier: sx,
            });
        } else {
            // Immediately complete the timer; `unwrap()` is safe since `rx` is alive.
            sx.send(self.now).unwrap();
        }
        rx
    }

    /// Returns the current timestamp.
    pub fn now(&self) -> DateTime<Utc> {
        self.now
    }

    /// Sets the current timestamp for the scheduler.
    pub fn set_now(&mut self, now: DateTime<Utc>) {
        let now = Self::floor_timestamp(now);
        self.now = now;
        while let Some(timer) = self.timers.pop() {
            if timer.expires_at <= now {
                timer.notifier.send(now).ok();
            } else {
                self.timers.push(timer);
                break;
            }
        }
    }
}

/// Handle allowing to manipulate time in the test environment.
#[derive(Debug)]
pub struct Timers(());

#[allow(clippy::unused_self)] // `self` included for future compatibility
impl Timers {
    /// Returns current time.
    pub fn now() -> DateTime<Utc> {
        Runtime::with(|rt| rt.scheduler.now)
    }

    /// Returns the expiration timestamp of the nearest timer.
    pub fn next_timer_expiration() -> Option<DateTime<Utc>> {
        Runtime::with(|rt| rt.scheduler.next_timer_expiration())
    }

    /// Advances time to the specified point, triggering the expired timers.
    pub fn set_now(now: DateTime<Utc>) {
        Runtime::with_mut(|rt| rt.scheduler.set_now(now));
    }
}

struct ErasedWorkflow {
    interface: Interface<()>,
    spawn_fn: Box<dyn Fn(Vec<u8>, Wasm) -> TaskHandle>,
}

impl fmt::Debug for ErasedWorkflow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ErasedWorkflow")
            .field("interface", &self.interface)
            .finish_non_exhaustive()
    }
}

/// Mock registry for workflows.
#[derive(Debug, Default)]
pub struct WorkflowRegistry {
    workflows: HashMap<String, ErasedWorkflow>,
}

impl WorkflowRegistry {
    /// Inserts a new workflow into this registry.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn insert<W: SpawnWorkflow>(&mut self, id: impl Into<String>) {
        let interface = W::interface().erase();
        let spawn_fn = |args, wasm| TaskHandle::from_workflow::<W>(args, wasm).unwrap();
        self.workflows.insert(
            id.into(),
            ErasedWorkflow {
                interface,
                spawn_fn: Box::new(spawn_fn),
            },
        );
    }

    pub(crate) fn interface(&self, definition_id: &str) -> Option<&Interface<()>> {
        self.workflows
            .get(definition_id)
            .map(|workflow| &workflow.interface)
    }

    pub(crate) fn create_workflow(
        &self,
        definition_id: &str,
        args: Vec<u8>,
        remote_handles: UntypedHandle<Wasm>,
    ) -> TaskHandle {
        let workflow = self.workflows.get(definition_id).unwrap_or_else(|| {
            panic!("workflow `{}` is not defined", definition_id);
        });
        (workflow.spawn_fn)(args, Wasm::new(remote_handles))
    }
}

/// Mock runtime allowing to execute workflows.
#[derive(Debug)]
pub struct Runtime {
    local_pool: Option<LocalPool>,
    spawner: LocalSpawner,
    scheduler: MockScheduler,
    workflow_registry: WorkflowRegistry,
}

impl Default for Runtime {
    fn default() -> Self {
        let local_pool = LocalPool::new();
        Self {
            spawner: local_pool.spawner(),
            local_pool: Some(local_pool),
            workflow_registry: WorkflowRegistry::default(),
            scheduler: MockScheduler::default(),
        }
    }
}

thread_local! {
    static RT: RefCell<Option<Runtime>> = RefCell::default();
}

impl Runtime {
    fn setup(mut self) -> (RuntimeGuard, LocalPool) {
        let local_pool = self.local_pool.take().unwrap();
        RT.with(|cell| {
            let mut borrow = cell.borrow_mut();
            debug_assert!(
                borrow.is_none(),
                "reinitializing runtime; this should never happen"
            );
            *borrow = Some(self);
        });
        (RuntimeGuard, local_pool)
    }

    pub(crate) fn with<T>(act: impl FnOnce(&Self) -> T) -> T {
        RT.with(|cell| {
            let borrow = cell.borrow();
            let rt = borrow
                .as_ref()
                .expect("runtime accessed outside event loop");
            act(rt)
        })
    }

    pub(crate) fn with_mut<T>(act: impl FnOnce(&mut Self) -> T) -> T {
        RT.with(|cell| {
            let mut borrow = cell.borrow_mut();
            let rt = borrow
                .as_mut()
                .expect("runtime accessed outside event loop");
            act(rt)
        })
    }

    pub(crate) fn scheduler(&mut self) -> &mut MockScheduler {
        &mut self.scheduler
    }

    pub(crate) fn workflow_registry(&self) -> &WorkflowRegistry {
        &self.workflow_registry
    }

    /// Returns a mutable reference to the workflow registry.
    pub fn workflow_registry_mut(&mut self) -> &mut WorkflowRegistry {
        &mut self.workflow_registry
    }

    pub(crate) fn spawn_task<T>(&self, task: impl Future<Output = T> + 'static) -> RemoteHandle<T> {
        self.spawner
            .spawn_local_with_handle(task)
            .expect("failed spawning task")
    }

    /// Executes the provided future in the context of this runtime. Thus, the future will
    /// have access to globals provided by a runtime such as [`Workflows`], or test-specific
    /// globals such as [`Timers`].
    ///
    /// This method can be used to handle more complex tests than [`Self::test()`], e.g.,
    /// ones that do not want to spin a specific workflow.
    ///
    /// [`Workflows`]: crate::spawn::Workflows
    pub fn run<T, Fut>(self, test_future: Fut) -> T
    where
        Fut: Future<Output = T>,
    {
        let (_guard, mut local_pool) = self.setup();
        local_pool.run_until(test_future)
    }

    /// Executes the provided test code for a specific workflow definition in the context
    /// of this runtime.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn test<W, F, Fut>(mut self, args: W::Args, test_fn: F)
    where
        W: SpawnWorkflow + TakeHandle<Spawner, Id = ()> + TakeHandle<RemoteWorkflow, Id = ()>,
        F: FnOnce(Handle<W, RemoteWorkflow>) -> Fut,
        Fut: Future<Output = ()>,
    {
        const DEFINITION_ID: &str = "__tested_workflow";

        self.workflow_registry.insert::<W>(DEFINITION_ID);
        self.run(async {
            let builder: WorkflowBuilder<_, W> =
                Workflows.new_workflow(DEFINITION_ID, args).unwrap();
            let workflow = builder.build().unwrap();
            crate::yield_now().await; // allow the workflow to initialize
            test_fn(workflow.api).await;
        });
    }
}

/// Guard that frees up runtime TLS on drop.
#[derive(Debug)]
struct RuntimeGuard;

impl Drop for RuntimeGuard {
    fn drop(&mut self) {
        RT.with(|cell| {
            *cell.borrow_mut() = None;
        });
    }
}
