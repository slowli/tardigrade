//! Mock utils.

use chrono::{DateTime, Utc};
use futures::{
    channel::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    executor::{LocalPool, LocalSpawner},
    future::RemoteHandle,
    task::LocalSpawnExt,
};

use std::{
    cell::RefCell,
    cmp,
    collections::{BinaryHeap, HashMap},
    future::Future,
    marker::PhantomData,
    thread_local,
    time::Duration,
};

use crate::{
    channel::{imp as channel_imp, RawReceiver, RawSender, Receiver, Sender},
    Data, Decoder, Encoder, SpawnWorkflow, Tracer, Wasm,
};
use tardigrade_shared::{
    workflow::{Interface, TakeHandle},
    ChannelError, ChannelErrorKind, ChannelKind, TracedFutureUpdate,
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

#[derive(Debug)]
pub struct TimersHandle {
    // Since the handle accesses TLS, it doesn't make sense to send it across threads.
    inner: PhantomData<*mut ()>,
}

impl TimersHandle {
    pub fn now(&self) -> DateTime<Utc> {
        Runtime::with_mut(|rt| rt.clock.now)
    }

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

    pub fn data_input(&self, name: &'static str) -> Option<Vec<u8>> {
        self.data_inputs.get(name).cloned()
    }

    pub fn take_inbound_channel(
        &mut self,
        name: &'static str,
    ) -> Result<UnboundedReceiver<Vec<u8>>, ChannelError> {
        let channel_place = self
            .inbound_channels
            .get_mut(name)
            .ok_or_else(|| ChannelErrorKind::Unknown.for_channel(ChannelKind::Inbound, name))?;
        channel_place.take_rx().ok_or_else(|| {
            ChannelErrorKind::AlreadyAcquired.for_channel(ChannelKind::Inbound, name)
        })
    }

    pub fn sender_for_inbound_channel(
        &self,
        name: &'static str,
    ) -> Result<UnboundedSender<Vec<u8>>, ChannelError> {
        self.inbound_channels
            .get(name)
            .map(ChannelPair::clone_sx)
            .ok_or_else(|| ChannelErrorKind::Unknown.for_channel(ChannelKind::Inbound, name))
    }

    pub fn outbound_channel(
        &self,
        name: &'static str,
    ) -> Result<UnboundedSender<Vec<u8>>, ChannelError> {
        self.outbound_channels
            .get(name)
            .map(ChannelPair::clone_sx)
            .ok_or_else(|| ChannelErrorKind::Unknown.for_channel(ChannelKind::Outbound, name))
    }

    pub fn take_receiver_for_outbound_channel(
        &mut self,
        name: &'static str,
    ) -> Result<UnboundedReceiver<Vec<u8>>, ChannelError> {
        let channel_place = self
            .outbound_channels
            .get_mut(name)
            .ok_or_else(|| ChannelErrorKind::Unknown.for_channel(ChannelKind::Outbound, name))?;
        channel_place.take_rx().ok_or_else(|| {
            ChannelErrorKind::AlreadyAcquired.for_channel(ChannelKind::Outbound, name)
        })
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
        })
    }
}

#[non_exhaustive]
pub struct TestHandle<W: TestWorkflow> {
    pub interface: <W as TakeHandle<TestHost, ()>>::Handle,
    pub timers: TimersHandle,
}

impl<W: TestWorkflow> TestHandle<W> {
    fn new() -> Self {
        let mut host = TestHost::new();
        let interface = W::take_handle(&mut host, ());
        Self {
            interface,
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
    let workflow = W::take_handle(&mut wasm, ());
    let test_handle = TestHandle::new();

    local_pool
        .spawner()
        .spawn_local(W::spawn(workflow).into_inner())
        .expect("cannot spawn workflow");
    local_pool.run_until(test_fn(test_handle))
}

/// Extension trait for testing.
pub trait TestWorkflow: Sized + SpawnWorkflow + TakeHandle<TestHost, ()> {
    fn test<F>(inputs: Self::Init, test_fn: impl FnOnce(TestHandle<Self>) -> F)
    where
        F: Future<Output = ()> + 'static,
    {
        test::<Self, _, _, _>(inputs, |handle| async {
            test_fn(handle).await;
            Ok::<_, ()>(())
        })
        .unwrap()
    }
}

impl<W> TestWorkflow for W where W: SpawnWorkflow + TakeHandle<TestHost, ()> {}

/// Host part of the test env.
#[derive(Debug)]
pub struct TestHost(());

impl TestHost {
    fn new() -> Self {
        Self(())
    }
}

impl TakeHandle<TestHost, &'static str> for RawReceiver {
    type Handle = RawSender;

    fn take_handle(env: &mut TestHost, id: &'static str) -> Self::Handle {
        RawSender::new(channel_imp::MpscReceiver::take_handle(env, id))
    }
}

impl<T, C: Encoder<T> + Default> TakeHandle<TestHost, &'static str> for Receiver<T, C> {
    type Handle = Sender<T, C>;

    fn take_handle(env: &mut TestHost, id: &'static str) -> Self::Handle {
        Sender::new(RawReceiver::take_handle(env, id), C::default())
    }
}

impl TakeHandle<TestHost, &'static str> for RawSender {
    type Handle = RawReceiver;

    fn take_handle(env: &mut TestHost, id: &'static str) -> Self::Handle {
        RawReceiver::new(channel_imp::MpscSender::take_handle(env, id))
    }
}

impl<T, C: Decoder<T> + Default> TakeHandle<TestHost, &'static str> for Sender<T, C> {
    type Handle = Receiver<T, C>;

    fn take_handle(env: &mut TestHost, id: &'static str) -> Self::Handle {
        Receiver::new(RawSender::take_handle(env, id), C::default())
    }
}

impl<T, C: Decoder<T> + Default> TakeHandle<TestHost, &'static str> for Data<T, C> {
    type Handle = Self;

    fn take_handle(_env: &mut TestHost, id: &'static str) -> Self {
        Self::from_env(id)
    }
}
