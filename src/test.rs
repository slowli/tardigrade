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
//! # use futures::{FutureExt, StreamExt};
//! # use serde::{Deserialize, Serialize};
//! # use tardigrade::{
//! #     channel::Sender,
//! #     workflow::{Handle, GetInterface, SpawnWorkflow, TaskHandle, Wasm},
//! #     Data, Json,
//! # };
//! // Assume we want to test a workflow.
//! #[derive(Debug, GetInterface)]
//! # #[tardigrade(interface = r#"{
//! #     "v": 0,
//! #     "out": { "events": {} },
//! #     "data": { "input": {} }
//! # }"#)]
//! pub struct MyWorkflow(());
//!
//! /// Workflow handle.
//! #[tardigrade::handle(for = "MyWorkflow")]
//! #[derive(Debug)]
//! pub struct MyHandle<Env> {
//!     pub input: Handle<Data<Input, Json>, Env>,
//!     pub events: Handle<Sender<Event, Json>, Env>,
//! }
//!
//! /// Input provided to the workflow.
//! #[tardigrade::init(for = "MyWorkflow", codec = "Json")]
//! #[derive(Debug, Serialize, Deserialize)]
//! pub struct Input {
//!     pub counter: u32,
//! }
//!
//! /// Events emitted by the workflow.
//! #[derive(Debug, Serialize, Deserialize)]
//! pub enum Event {
//!     Count(u32),
//! }
//!
//! // Workflow logic
//! impl SpawnWorkflow for MyWorkflow {
//!     fn spawn(handle: MyHandle<Wasm>) -> TaskHandle {
//!         let counter = handle.input.into_inner().counter;
//!         let mut events = handle.events;
//!         TaskHandle::new(async move {
//!             for i in 0..counter {
//!                 tardigrade::sleep(Duration::from_millis(100)).await;
//!                 events.send(Event::Count(i)).await;
//!             }
//!         })
//!     }
//! }
//!
//! // We can test the workflow as follows:
//! use tardigrade::test::{TestHandle, TestWorkflow};
//!
//! async fn test_workflow(mut handle: TestHandle<MyWorkflow>) {
//!     // The workflow should be waiting for a timer to emit an event.
//!     assert!(handle.api.events.next().now_or_never().is_none());
//!
//!     let now = handle.timers.now();
//!     let new_now = handle.timers.next_timer_expiration().unwrap();
//!     assert_eq!((new_now - now).num_milliseconds(), 100);
//!     handle.timers.set_now(new_now);
//!     let event = handle.api.events.next().await.unwrap();
//!     assert_matches!(event, Event::Count(0));
//! }
//! MyWorkflow::test(Input { counter: 1 }, test_workflow);
//! ```

use chrono::{DateTime, TimeZone, Utc};
use futures::{
    channel::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    executor::{LocalPool, LocalSpawner},
    future::RemoteHandle,
    stream::{Fuse, FusedStream},
    task::LocalSpawnExt,
    FutureExt, StreamExt,
};

use std::{
    cell::RefCell,
    cmp,
    collections::{BinaryHeap, HashMap},
    fmt,
    future::Future,
    marker::PhantomData,
    thread_local,
};

use crate::{
    channel::{imp as channel_imp, Receiver, Sender},
    interface::{AccessError, AccessErrorKind, InboundChannel, Interface, OutboundChannel},
    trace::Tracer,
    workflow::{Inputs, SpawnWorkflow, TakeHandle, Wasm},
    Data, Decoder, Encoder,
};
use tardigrade_shared::trace::{FutureUpdate, TracedFutures};

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
        Self::new(Utc::now())
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
    ///
    /// # Panics
    ///
    /// Panics if `expires_at` is in the past according to [`Self::now()`].
    pub fn insert_timer(&mut self, expires_at: DateTime<Utc>) -> oneshot::Receiver<DateTime<Utc>> {
        let expires_at = Self::floor_timestamp(expires_at);
        assert!(expires_at > self.now);

        let (sx, rx) = oneshot::channel();
        self.timers.push(TimerEntry {
            expires_at,
            notifier: sx,
        });
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
pub struct TimersHandle {
    // Since the handle accesses TLS, it doesn't make sense to send it across threads.
    inner: PhantomData<*mut ()>,
}

#[allow(clippy::unused_self)] // `self` included for future compatibility
impl TimersHandle {
    /// Returns current time.
    pub fn now(&self) -> DateTime<Utc> {
        Runtime::with_mut(|rt| rt.scheduler.now)
    }

    /// Returns the expiration timestamp of the nearest timer.
    pub fn next_timer_expiration(&self) -> Option<DateTime<Utc>> {
        Runtime::with_mut(|rt| rt.scheduler.next_timer_expiration())
    }

    /// Advances time to the specified point, triggering the expired timers.
    pub fn set_now(&self, now: DateTime<Utc>) {
        Runtime::with_mut(|rt| rt.scheduler.set_now(now));
    }
}

#[derive(Debug)]
struct ChannelPair {
    sx: Option<UnboundedSender<Vec<u8>>>,
    rx: Option<UnboundedReceiver<Vec<u8>>>,
}

impl Default for ChannelPair {
    fn default() -> Self {
        let (sx, rx) = mpsc::unbounded();
        Self {
            sx: Some(sx),
            rx: Some(rx),
        }
    }
}

impl ChannelPair {
    fn clone_sx(&self) -> UnboundedSender<Vec<u8>> {
        self.sx.clone().unwrap()
        // ^ `unwrap()` is safe: `take_sx()` is never called for outbound channels
    }

    fn take_sx(&mut self) -> Option<UnboundedSender<Vec<u8>>> {
        self.sx.take()
    }

    fn take_rx(&mut self) -> Option<UnboundedReceiver<Vec<u8>>> {
        self.rx.take()
    }
}

#[derive(Debug)]
pub(crate) struct Runtime {
    spawner: LocalSpawner,
    data_inputs: HashMap<String, Vec<u8>>,
    inbound_channels: HashMap<String, ChannelPair>,
    outbound_channels: HashMap<String, ChannelPair>,
    scheduler: MockScheduler,
}

thread_local! {
    static RT: RefCell<Option<Runtime>> = RefCell::default();
}

impl Runtime {
    fn initialize<W>(
        interface: &Interface<W>,
        data_inputs: HashMap<String, Vec<u8>>,
    ) -> (RuntimeGuard, LocalPool) {
        let local_pool = LocalPool::new();

        let inbound = interface
            .inbound_channels()
            .map(|(name, _)| (name.to_owned(), ChannelPair::default()));
        let outbound = interface
            .outbound_channels()
            .map(|(name, _)| (name.to_owned(), ChannelPair::default()));

        let rt = Self {
            spawner: local_pool.spawner(),
            data_inputs,
            inbound_channels: inbound.collect(),
            outbound_channels: outbound.collect(),
            scheduler: MockScheduler::default(),
        };

        RT.with(|cell| {
            let mut borrow = cell.borrow_mut();
            debug_assert!(
                borrow.is_none(),
                "reinitializing runtime; this should never happen"
            );
            *borrow = Some(rt);
        });
        (RuntimeGuard, local_pool)
    }

    pub fn with_mut<T>(act: impl FnOnce(&mut Self) -> T) -> T {
        RT.with(|cell| {
            let mut borrow = cell.borrow_mut();
            let rt = borrow
                .as_mut()
                .expect("runtime accessed outside event loop");
            act(rt)
        })
    }

    pub fn data_input(&self, name: &str) -> Option<Vec<u8>> {
        self.data_inputs.get(name).cloned()
    }

    pub fn take_inbound_channel(
        &mut self,
        name: &str,
    ) -> Result<UnboundedReceiver<Vec<u8>>, AccessError> {
        let channel = self
            .inbound_channels
            .get_mut(name)
            .ok_or_else(|| AccessErrorKind::Unknown.with_location(InboundChannel(name)))?;
        channel
            .take_rx()
            .ok_or_else(|| AccessErrorKind::AlreadyAcquired.with_location(InboundChannel(name)))
    }

    pub fn take_sender_for_inbound_channel(
        &mut self,
        name: &str,
    ) -> Result<UnboundedSender<Vec<u8>>, AccessError> {
        let channel = self
            .inbound_channels
            .get_mut(name)
            .ok_or_else(|| AccessErrorKind::Unknown.with_location(InboundChannel(name)))?;
        channel
            .take_sx()
            .ok_or_else(|| AccessErrorKind::AlreadyAcquired.with_location(InboundChannel(name)))
    }

    pub fn outbound_channel(&self, name: &str) -> Result<UnboundedSender<Vec<u8>>, AccessError> {
        self.outbound_channels
            .get(name)
            .map(ChannelPair::clone_sx)
            .ok_or_else(|| AccessErrorKind::Unknown.with_location(OutboundChannel(name)))
    }

    pub fn take_receiver_for_outbound_channel(
        &mut self,
        name: &str,
    ) -> Result<UnboundedReceiver<Vec<u8>>, AccessError> {
        let channel = self
            .outbound_channels
            .get_mut(name)
            .ok_or_else(|| AccessErrorKind::Unknown.with_location(OutboundChannel(name)))?;
        channel
            .take_rx()
            .ok_or_else(|| AccessErrorKind::AlreadyAcquired.with_location(OutboundChannel(name)))
    }

    pub fn scheduler(&mut self) -> &mut MockScheduler {
        &mut self.scheduler
    }

    pub fn spawn_task<T>(&self, task: impl Future<Output = T> + 'static) -> RemoteHandle<T> {
        self.spawner
            .spawn_local_with_handle(task)
            .expect("failed spawning task")
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

/// Workflow handle for the [test environment](TestHost). Instance of such a handle is provided
/// to a closure in [`TestWorkflow::test()`].
#[non_exhaustive]
pub struct TestHandle<W: TestWorkflow> {
    /// Handle to the workflow interface (channels, data inputs etc.).
    pub api: <W as TakeHandle<TestHost>>::Handle,
    /// Handle to manipulate time in the test environment.
    pub timers: TimersHandle,
}

impl<W: TestWorkflow> fmt::Debug for TestHandle<W>
where
    <W as TakeHandle<TestHost>>::Handle: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TestHandle")
            .field("api", &self.api)
            .field("timers", &self.timers)
            .finish()
    }
}

impl<W: TestWorkflow> TestHandle<W> {
    fn new() -> Self {
        let mut host = TestHost::new();
        let api = W::take_handle(&mut host, &()).unwrap();
        Self {
            api,
            timers: TimersHandle { inner: PhantomData },
        }
    }
}

fn test<W, F, Fut, E>(inputs: W::Init, test_fn: F) -> Result<(), E>
where
    W: TestWorkflow,
    F: FnOnce(TestHandle<W>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    let interface = W::interface();
    let workflow_inputs = Inputs::for_interface(&interface, inputs);

    let (guard, mut local_pool) = Runtime::initialize(&interface, workflow_inputs.into_inner());
    let mut wasm = Wasm::default();
    let workflow = W::take_handle(&mut wasm, &()).unwrap();
    let test_handle = TestHandle::new();

    local_pool
        .spawner()
        .spawn_local(W::spawn(workflow).into_inner().map(|()| drop(guard)))
        .expect("cannot spawn workflow");
    local_pool.run_until_stalled();
    local_pool.run_until(test_fn(test_handle))
}

/// Extension trait for testing workflows.
pub trait TestWorkflow: Sized + SpawnWorkflow + TakeHandle<TestHost, Id = ()> {
    /// Runs the specified test function for a workflow instantiated from the provided `inputs`.
    fn test<F>(inputs: Self::Init, test_fn: impl FnOnce(TestHandle<Self>) -> F)
    where
        F: Future<Output = ()> + 'static,
    {
        test::<Self, _, _, _>(inputs, |handle| async {
            test_fn(handle).await;
            Ok::<_, ()>(())
        })
        .unwrap();
    }
}

impl<W> TestWorkflow for W where W: SpawnWorkflow + TakeHandle<TestHost, Id = ()> {}

/// Host part of the test environment.
///
/// This type is used as a type param for the [`TakeHandle`] trait. The returned handles
/// allow manipulating the workflow from the (emulated) host; for example, a handle
/// for a [`Sender`] is a complementary [`Receiver`] (to receive emitted messages), and vice versa.
#[derive(Debug)]
pub struct TestHost(());

impl TestHost {
    fn new() -> Self {
        Self(())
    }
}

impl<T, C: Encoder<T> + Default> TakeHandle<TestHost> for Receiver<T, C> {
    type Id = str;
    type Handle = Sender<T, C>;

    fn take_handle(env: &mut TestHost, id: &str) -> Result<Self::Handle, AccessError> {
        channel_imp::MpscReceiver::take_handle(env, id).map(|raw| Sender::new(raw, C::default()))
    }
}

impl<T, C: Decoder<T> + Default> TakeHandle<TestHost> for Sender<T, C> {
    type Id = str;
    type Handle = Receiver<T, C>;

    fn take_handle(env: &mut TestHost, id: &str) -> Result<Self::Handle, AccessError> {
        channel_imp::MpscSender::take_handle(env, id).map(|raw| Receiver::new(raw, C::default()))
    }
}

impl<T, C: Decoder<T> + Default> TakeHandle<TestHost> for Data<T, C> {
    type Id = str;
    type Handle = Self;

    fn take_handle(_env: &mut TestHost, id: &str) -> Result<Self, AccessError> {
        Self::from_env(id)
    }
}

/// Handle for traced futures in the [test environment](TestHost).
#[derive(Debug)]
pub struct TracerHandle<C> {
    receiver: Fuse<Receiver<FutureUpdate, C>>,
    futures: TracedFutures,
}

impl<C> TracerHandle<C>
where
    C: Decoder<FutureUpdate> + Default,
{
    /// Returns a reference to the traced futures.
    pub fn futures(&self) -> &TracedFutures {
        &self.futures
    }

    /// Applies all accumulated updates for the traced futures.
    #[allow(clippy::missing_panics_doc)]
    pub fn update(&mut self) {
        if self.receiver.is_terminated() {
            return;
        }
        while let Some(Some(update)) = self.receiver.next().now_or_never() {
            self.futures.update(update).unwrap();
            // `unwrap()` is intentional: it's to catch bugs in library code
        }
    }
}

impl<C> TakeHandle<TestHost> for Tracer<C>
where
    C: Decoder<FutureUpdate> + Default,
{
    type Id = str;
    type Handle = TracerHandle<C>;

    fn take_handle(env: &mut TestHost, id: &str) -> Result<Self::Handle, AccessError> {
        Ok(TracerHandle {
            receiver: Sender::take_handle(env, id)?.fuse(),
            futures: TracedFutures::new(id),
        })
    }
}
