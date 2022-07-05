//! Mock utils.

use chrono::{DateTime, Utc};
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
    time::Duration,
};

use crate::{
    channel::{imp as channel_imp, Receiver, Sender},
    trace::Tracer,
    Data, Decoder, Encoder, SpawnWorkflow, Wasm,
};
use tardigrade_shared::{
    trace::{FutureUpdate, TracedFuture, TracedFutures},
    workflow::{
        HandleError, HandleErrorKind, InboundChannel, Interface, OutboundChannel, TakeHandle,
    },
    FutureId,
};

#[derive(Debug)]
struct TimerEntry {
    expires_at: DateTime<Utc>,
    notifier: oneshot::Sender<()>,
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

#[derive(Debug)]
struct RuntimeClock {
    now: DateTime<Utc>,
    timers: BinaryHeap<TimerEntry>,
}

impl Default for RuntimeClock {
    fn default() -> Self {
        Self {
            now: Utc::now(),
            timers: BinaryHeap::new(),
        }
    }
}

impl RuntimeClock {
    fn insert_timer(&mut self, duration: Duration) -> oneshot::Receiver<()> {
        let duration = chrono::Duration::from_std(duration).expect("duration is too large");
        let (sx, rx) = oneshot::channel();
        self.timers.push(TimerEntry {
            expires_at: self.now + duration,
            notifier: sx,
        });
        rx
    }

    fn advance_time_to_next_event(&mut self) -> DateTime<Utc> {
        if let Some(entry) = self.timers.peek() {
            let expires_at = entry.expires_at;
            self.set_now(expires_at);
        }
        self.now
    }

    fn set_now(&mut self, now: DateTime<Utc>) {
        self.now = now;
        while let Some(timer) = self.timers.pop() {
            if timer.expires_at <= now {
                timer.notifier.send(()).ok();
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

#[allow(clippy::unused_self)] // included for future compatibility
impl TimersHandle {
    /// Returns current time.
    pub fn now(&self) -> DateTime<Utc> {
        Runtime::with_mut(|rt| rt.clock.now)
    }

    /// Advances time to the next timer event.
    pub fn advance_time_to_next_event(&self) -> DateTime<Utc> {
        Runtime::with_mut(|rt| rt.clock.advance_time_to_next_event())
    }
}

#[derive(Debug)]
struct ChannelPair {
    sx: UnboundedSender<Vec<u8>>,
    rx: Option<UnboundedReceiver<Vec<u8>>>,
}

impl Default for ChannelPair {
    fn default() -> Self {
        let (sx, rx) = mpsc::unbounded();
        Self { sx, rx: Some(rx) }
    }
}

impl ChannelPair {
    fn clone_sx(&self) -> UnboundedSender<Vec<u8>> {
        self.sx.clone()
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
    clock: RuntimeClock,
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
            clock: RuntimeClock::default(),
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
    ) -> Result<UnboundedReceiver<Vec<u8>>, HandleError> {
        let channel_place = self
            .inbound_channels
            .get_mut(name)
            .ok_or_else(|| HandleErrorKind::Unknown.for_handle(InboundChannel(name)))?;
        channel_place
            .take_rx()
            .ok_or_else(|| HandleErrorKind::AlreadyAcquired.for_handle(InboundChannel(name)))
    }

    pub fn sender_for_inbound_channel(
        &self,
        name: &str,
    ) -> Result<UnboundedSender<Vec<u8>>, HandleError> {
        self.inbound_channels
            .get(name)
            .map(ChannelPair::clone_sx)
            .ok_or_else(|| HandleErrorKind::Unknown.for_handle(InboundChannel(name)))
    }

    pub fn outbound_channel(&self, name: &str) -> Result<UnboundedSender<Vec<u8>>, HandleError> {
        self.outbound_channels
            .get(name)
            .map(ChannelPair::clone_sx)
            .ok_or_else(|| HandleErrorKind::Unknown.for_handle(OutboundChannel(name)))
    }

    pub fn take_receiver_for_outbound_channel(
        &mut self,
        name: &str,
    ) -> Result<UnboundedReceiver<Vec<u8>>, HandleError> {
        let channel_place = self
            .outbound_channels
            .get_mut(name)
            .ok_or_else(|| HandleErrorKind::Unknown.for_handle(OutboundChannel(name)))?;
        channel_place
            .take_rx()
            .ok_or_else(|| HandleErrorKind::AlreadyAcquired.for_handle(OutboundChannel(name)))
    }

    pub fn insert_timer(&mut self, duration: Duration) -> oneshot::Receiver<()> {
        self.clock.insert_timer(duration)
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

/// Workflow handle for the [test environment](TestEnv). Instance of such a handle is provided
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
    let workflow_inputs = interface.create_inputs(inputs);

    // Order is important: we need futures in `local_pool` to be dropped before `_guard`
    // (which will drop `Runtime`, including the clock)
    let (_guard, mut local_pool) = Runtime::initialize(&interface, workflow_inputs.into_inner());
    let mut wasm = Wasm::default();
    let workflow = W::take_handle(&mut wasm, &()).unwrap();
    let test_handle = TestHandle::new();

    local_pool
        .spawner()
        .spawn_local(W::spawn(workflow).into_inner())
        .expect("cannot spawn workflow");
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

/// Host part of the test env.
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

    fn take_handle(env: &mut TestHost, id: &str) -> Result<Self::Handle, HandleError> {
        channel_imp::MpscReceiver::take_handle(env, id).map(|raw| Sender::new(raw, C::default()))
    }
}

impl<T, C: Decoder<T> + Default> TakeHandle<TestHost> for Sender<T, C> {
    type Id = str;
    type Handle = Receiver<T, C>;

    fn take_handle(env: &mut TestHost, id: &str) -> Result<Self::Handle, HandleError> {
        channel_imp::MpscSender::take_handle(env, id).map(|raw| Receiver::new(raw, C::default()))
    }
}

impl<T, C: Decoder<T> + Default> TakeHandle<TestHost> for Data<T, C> {
    type Id = str;
    type Handle = Self;

    fn take_handle(_env: &mut TestHost, id: &str) -> Result<Self, HandleError> {
        Self::from_env(id)
    }
}

/// Handle for traced futures in the [test environment](TestEnv).
#[derive(Debug)]
pub struct TracerHandle<C> {
    receiver: Fuse<Receiver<FutureUpdate, C>>,
    futures: TracedFutures,
}

impl<C> TracerHandle<C>
where
    C: Decoder<FutureUpdate> + Default,
{
    /// Applies all accumulated updates for the traced futures.
    #[allow(clippy::missing_panics_doc)]
    pub fn update(&mut self) {
        if self.receiver.is_terminated() {
            return;
        }
        while let Some(Some(update)) = self.receiver.next().now_or_never() {
            TracedFuture::update(&mut self.futures, update).unwrap();
            // `unwrap()` is intentional: it's to catch bugs in library code
        }
    }

    /// Returns the current state of a future with the specified ID.
    pub fn future(&self, id: FutureId) -> Option<&TracedFuture> {
        self.futures.get(&id)
    }

    /// Lists all futures together their current state.
    pub fn futures(&self) -> impl Iterator<Item = (FutureId, &TracedFuture)> + '_ {
        self.futures.iter().map(|(id, state)| (*id, state))
    }
}

impl<C> TakeHandle<TestHost> for Tracer<C>
where
    C: Decoder<FutureUpdate> + Default,
{
    type Id = str;
    type Handle = TracerHandle<C>;

    fn take_handle(env: &mut TestHost, id: &str) -> Result<Self::Handle, HandleError> {
        Ok(TracerHandle {
            receiver: Sender::take_handle(env, id)?.fuse(),
            futures: TracedFutures::default(),
        })
    }
}
