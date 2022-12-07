//! Configuring child spawning.

use futures::future;

use std::{cell::RefCell, convert::Infallible, fmt, marker::PhantomData, mem, rc::Rc};

use crate::{
    handle::{
        AccessError, AccessErrorKind, Handle, HandleMap, HandleMapKey, HandlePath, HandlePathBuf,
        ReceiverAt, SenderAt,
    },
    interface::Interface,
    spawn::ManageChannels,
    workflow::{
        HandleFormat, InEnv, IntoRaw, Inverse, TakeHandles, TryFromRaw, UntypedHandles, Wasm,
        WithHandle,
    },
    Codec, Raw,
};

/// Configuration for a single workflow channel during workflow instantiation.
#[derive(Debug)]
enum ChannelSpawnConfig<T> {
    /// Create a new channel.
    New,
    /// Close the channel immediately on workflow creation.
    Closed,
    /// Copy an existing channel sender or move an existing receiver.
    Existing(T),
}

impl<T> Default for ChannelSpawnConfig<T> {
    fn default() -> Self {
        Self::New
    }
}

type ChannelsConfig<Fmt> = HandleMap<
    ChannelSpawnConfig<<Fmt as HandleFormat>::RawReceiver>,
    ChannelSpawnConfig<<Fmt as HandleFormat>::RawSender>,
>;

struct SpawnerInner<Fmt: HandleFormat> {
    channels: RefCell<ChannelsConfig<Fmt>>,
}

impl<Fmt> fmt::Debug for SpawnerInner<Fmt>
where
    Fmt: HandleFormat,
    Fmt::RawReceiver: fmt::Debug,
    Fmt::RawSender: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Spawner")
            .field("channels", &self.channels)
            .finish()
    }
}

impl<Fmt: HandleFormat> SpawnerInner<Fmt> {
    fn new(interface: &Interface) -> Self {
        let config = interface.handles().map(|(path, spec)| {
            let config = spec
                .as_ref()
                .map_receiver(|_| ChannelSpawnConfig::default())
                .map_sender(|_| ChannelSpawnConfig::default());
            (path.to_owned(), config)
        });

        Self {
            channels: RefCell::new(config.collect()),
        }
    }

    fn close_channel_half(&self, path: HandlePath<'_>) {
        let mut borrow = self.channels.borrow_mut();
        match borrow.get_mut(&path).unwrap() {
            Handle::Receiver(rx) => {
                *rx = ChannelSpawnConfig::Closed;
            }
            Handle::Sender(sx) => {
                *sx = ChannelSpawnConfig::Closed;
            }
        }
    }

    fn copy_sender(&self, path: HandlePath<'_>, sender: Fmt::RawSender) {
        let mut borrow = self.channels.borrow_mut();
        if let Handle::Sender(sx) = borrow.get_mut(&path).unwrap() {
            *sx = ChannelSpawnConfig::Existing(sender);
        }
    }
}

/// Spawn [environment](HandleFormat) that can be used to configure channels before spawning
/// a workflow.
pub struct Spawner<Fmt: HandleFormat = Wasm> {
    inner: Rc<SpawnerInner<Fmt>>,
}

impl<Fmt: HandleFormat> fmt::Debug for Spawner<Fmt>
where
    Fmt::RawReceiver: fmt::Debug,
    Fmt::RawSender: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, formatter)
    }
}

impl<Fmt: HandleFormat> HandleFormat for Spawner<Fmt> {
    type RawReceiver = ReceiverConfig<Fmt, Vec<u8>, Raw>;
    type Receiver<T, C: Codec<T>> = ReceiverConfig<Fmt, T, C>;
    type RawSender = SenderConfig<Fmt, Vec<u8>, Raw>;
    type Sender<T, C: Codec<T>> = SenderConfig<Fmt, T, C>;
}

/// Configurator of a workflow channel [`Receiver`].
pub struct ReceiverConfig<Fmt: HandleFormat, T, C> {
    spawner: Rc<SpawnerInner<Fmt>>,
    path: HandlePathBuf,
    _ty: PhantomData<fn(C) -> T>,
}

impl<Fmt, T, C> fmt::Debug for ReceiverConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    Fmt::RawReceiver: fmt::Debug,
    Fmt::RawSender: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReceiverConfig")
            .field("spawner", &self.spawner)
            .field("channel_name", &self.path)
            .finish()
    }
}

impl<Fmt, T, C> ReceiverConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    /// Closes the channel immediately on workflow instantiation.
    pub fn close(&self) {
        self.spawner.close_channel_half(self.path.as_ref());
    }
}

impl<Fmt, T, C> TryFromRaw<ReceiverConfig<Fmt, Vec<u8>, Raw>> for ReceiverConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    type Error = Infallible;

    #[inline]
    fn try_from_raw(raw: ReceiverConfig<Fmt, Vec<u8>, Raw>) -> Result<Self, Self::Error> {
        Ok(Self {
            spawner: raw.spawner,
            path: raw.path,
            _ty: PhantomData,
        })
    }
}

impl<Fmt, T, C> IntoRaw<ReceiverConfig<Fmt, Vec<u8>, Raw>> for ReceiverConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    #[inline]
    fn into_raw(self) -> ReceiverConfig<Fmt, Vec<u8>, Raw> {
        ReceiverConfig {
            spawner: self.spawner,
            path: self.path,
            _ty: PhantomData,
        }
    }
}

/// Configurator of a workflow channel [`Sender`].
pub struct SenderConfig<Fmt: HandleFormat, T, C> {
    spawner: Rc<SpawnerInner<Fmt>>,
    path: HandlePathBuf,
    _ty: PhantomData<fn(C) -> T>,
}

impl<Fmt, T, C> fmt::Debug for SenderConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    Fmt::RawReceiver: fmt::Debug,
    Fmt::RawSender: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SenderConfig")
            .field("spawner", &self.spawner)
            .field("channel_name", &self.path)
            .finish()
    }
}

impl<Fmt, T, C> SenderConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    /// Closes the channel immediately on workflow instantiation.
    pub fn close(&self) {
        self.spawner.close_channel_half(self.path.as_ref());
    }
}

impl<Fmt, T, C> SenderConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    /// Copies the channel from the provided `sender`. Thus, the created workflow will send
    /// messages over the same channel as `sender`.
    pub fn copy_from(&self, sender: Fmt::Sender<T, C>) {
        self.spawner
            .copy_sender(self.path.as_ref(), sender.into_raw());
    }
}

impl<Fmt, T, C> TryFromRaw<SenderConfig<Fmt, Vec<u8>, Raw>> for SenderConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    type Error = Infallible;

    #[inline]
    fn try_from_raw(raw: SenderConfig<Fmt, Vec<u8>, Raw>) -> Result<Self, Self::Error> {
        Ok(Self {
            spawner: raw.spawner,
            path: raw.path,
            _ty: PhantomData,
        })
    }
}

impl<Fmt, T, C> IntoRaw<SenderConfig<Fmt, Vec<u8>, Raw>> for SenderConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    #[inline]
    fn into_raw(self) -> SenderConfig<Fmt, Vec<u8>, Raw> {
        SenderConfig {
            spawner: self.spawner,
            path: self.path,
            _ty: PhantomData,
        }
    }
}

/// Builder of handle pairs: ones to be retained in the parent workflow and ones to be provided
/// to the child workflow.
pub(super) struct HandlesBuilder<W: WithHandle, Fmt: HandleFormat = Wasm> {
    spawner: Spawner<Fmt>,
    config: InEnv<W, Spawner<Fmt>>,
}

impl<W: WithHandle, Fmt: HandleFormat> fmt::Debug for HandlesBuilder<W, Fmt>
where
    Fmt::RawReceiver: fmt::Debug,
    Fmt::RawSender: fmt::Debug,
    InEnv<W, Spawner<Fmt>>: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HandlesBuilder")
            .field("spawner", &self.spawner)
            .field("config", &self.config)
            .finish()
    }
}

impl<W, Fmt> HandlesBuilder<W, Fmt>
where
    Fmt: HandleFormat,
    W: WithHandle,
{
    /// Creates a new builder based on the interface of `W`.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn new(interface: &Interface) -> Self {
        let spawner = Spawner {
            inner: Rc::new(SpawnerInner::new(interface)),
        };
        let untyped = interface.handles().map(|(path, spec)| {
            let config = spec
                .as_ref()
                .map_receiver(|_| ReceiverConfig {
                    spawner: Rc::clone(&spawner.inner),
                    path: path.to_owned(),
                    _ty: PhantomData,
                })
                .map_sender(|_| SenderConfig {
                    spawner: Rc::clone(&spawner.inner),
                    path: path.to_owned(),
                    _ty: PhantomData,
                });
            (path.to_owned(), config)
        });
        let untyped = untyped.collect();

        let config = W::try_from_untyped(untyped).unwrap();
        Self { spawner, config }
    }

    /// Returns a [handle](TakeHandle) that can be used to configure created workflow channels.
    pub fn config(&self) -> &InEnv<W, Spawner<Fmt>> {
        &self.config
    }
}

impl<W: WithHandle, Fmt: HandleFormat> HandlesBuilder<W, Fmt> {
    pub async fn build<M>(self, manager: &M) -> (InEnv<W, M::Fmt>, InEnv<W, Inverse<M::Fmt>>)
    where
        M: ManageChannels<Fmt = Fmt>,
    {
        drop(self.config);
        let spawner = Rc::try_unwrap(self.spawner.inner).map_err(drop).unwrap();
        let channels = spawner.channels.into_inner();

        let pairs = channels.into_iter().map(|(path, config)| async {
            let pair = match config {
                Handle::Receiver(rx_config) => {
                    Handle::Receiver(ChannelPair::for_remote_receiver(rx_config, manager).await)
                }
                Handle::Sender(sx_config) => {
                    Handle::Sender(ChannelPair::for_remote_sender(sx_config, manager).await)
                }
            };
            (path, pair)
        });
        let pairs = future::join_all(pairs).await;

        let mut remote = ForSelf {
            inner: pairs.into_iter().collect(),
        };
        let remote_handles = W::take_from_untyped(&mut remote, HandlePath::EMPTY).unwrap();
        let mut local = ForChild {
            inner: remote.inner,
        };
        let local_handles = W::take_from_untyped(&mut local, HandlePath::EMPTY).unwrap();
        (local_handles, remote_handles)
    }
}

#[derive(Debug)]
struct ChannelPair<Fmt: HandleFormat> {
    sender: Option<Fmt::RawSender>,
    receiver: Option<Fmt::RawReceiver>,
}

impl<Fmt: HandleFormat> ChannelPair<Fmt> {
    fn new(sender: Fmt::RawSender, receiver: Fmt::RawReceiver) -> Self {
        Self {
            sender: Some(sender),
            receiver: Some(receiver),
        }
    }

    async fn for_remote_receiver(
        config: ChannelSpawnConfig<Fmt::RawReceiver>,
        manager: &impl ManageChannels<Fmt = Fmt>,
    ) -> Self {
        let (sender, receiver) = match config {
            ChannelSpawnConfig::New => manager.create_channel().await,
            ChannelSpawnConfig::Closed => (manager.closed_sender(), manager.closed_receiver()),
            ChannelSpawnConfig::Existing(rx) => (manager.closed_sender(), rx),
        };
        Self::new(sender, receiver)
    }

    async fn for_remote_sender(
        config: ChannelSpawnConfig<Fmt::RawSender>,
        manager: &impl ManageChannels<Fmt = Fmt>,
    ) -> Self {
        let (sender, receiver) = match config {
            ChannelSpawnConfig::New => manager.create_channel().await,
            ChannelSpawnConfig::Closed => (manager.closed_sender(), manager.closed_receiver()),
            ChannelSpawnConfig::Existing(sx) => (sx, manager.closed_receiver()),
        };
        Self::new(sender, receiver)
    }
}

/// Handles format for handles retained for the parent workflow of a spawned child.
struct ForSelf<Fmt: HandleFormat> {
    inner: HandleMap<ChannelPair<Fmt>>,
}

impl<Fmt: HandleFormat> TakeHandles<Inverse<Fmt>> for ForSelf<Fmt> {
    fn take_receiver(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawSender, AccessError> {
        let pair = ReceiverAt(path).get_mut(&mut self.inner)?;
        pair.sender.take().ok_or_else(|| {
            AccessErrorKind::custom("attempted to take the same handle twice")
                .with_location(ReceiverAt(path))
        })
    }

    fn take_sender(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawReceiver, AccessError> {
        let pair = SenderAt(path).get_mut(&mut self.inner)?;
        pair.receiver.take().ok_or_else(|| {
            AccessErrorKind::custom("attempted to take the same handle twice")
                .with_location(ReceiverAt(path))
        })
    }

    fn drain(&mut self) -> UntypedHandles<Inverse<Fmt>> {
        let handles = self.inner.iter_mut().filter_map(|(path, handle)| {
            let handle = match handle {
                Handle::Receiver(pair) => Handle::Receiver(pair.sender.take()?),
                Handle::Sender(pair) => Handle::Sender(pair.receiver.take()?),
            };
            Some((path.clone(), handle))
        });
        handles.collect()
    }
}

struct ForChild<Fmt: HandleFormat> {
    inner: HandleMap<ChannelPair<Fmt>>,
}

impl<Fmt: HandleFormat> TakeHandles<Fmt> for ForChild<Fmt> {
    fn take_receiver(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawReceiver, AccessError> {
        let pair = ReceiverAt(path).get_mut(&mut self.inner)?;
        pair.receiver.take().ok_or_else(|| {
            AccessErrorKind::custom("attempted to take the same handle twice")
                .with_location(ReceiverAt(path))
        })
    }

    fn take_sender(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawSender, AccessError> {
        let pair = SenderAt(path).get_mut(&mut self.inner)?;
        pair.sender.take().ok_or_else(|| {
            AccessErrorKind::custom("attempted to take the same handle twice")
                .with_location(ReceiverAt(path))
        })
    }

    fn drain(&mut self) -> UntypedHandles<Fmt> {
        let handles = mem::take(&mut self.inner).into_iter();
        let handles = handles.filter_map(|(path, handle)| {
            let handle = match handle {
                Handle::Receiver(pair) => Handle::Receiver(pair.receiver?),
                Handle::Sender(pair) => Handle::Sender(pair.sender?),
            };
            Some((path, handle))
        });
        handles.collect()
    }
}
