//! Configuring child spawning.

use futures::future;

use std::{cell::Cell, convert::Infallible, fmt, marker::PhantomData, mem, rc::Rc};

use crate::{
    handle::{
        AccessError, AccessErrorKind, Handle, HandleMap, HandleMapKey, HandlePath, ReceiverAt,
        SenderAt,
    },
    interface::Interface,
    spawn::CreateChannel,
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

#[cfg(feature = "tracing")]
impl<T> ChannelSpawnConfig<T> {
    fn drop_payload(&self) -> ChannelSpawnConfig<()> {
        match self {
            Self::New => ChannelSpawnConfig::New,
            Self::Closed => ChannelSpawnConfig::Closed,
            Self::Existing(_) => ChannelSpawnConfig::Existing(()),
        }
    }
}

type SharedSpawnConfig<T> = Rc<Cell<ChannelSpawnConfig<T>>>;

type ChannelsConfig<Fmt> = HandleMap<
    SharedSpawnConfig<<Fmt as HandleFormat>::RawReceiver>,
    SharedSpawnConfig<<Fmt as HandleFormat>::RawSender>,
>;

/// Spawn [environment](HandleFormat) that can be used to configure channels before spawning
/// a workflow.
pub struct Spawner<Fmt: HandleFormat = Wasm> {
    channels: ChannelsConfig<Fmt>,
}

impl<Fmt: HandleFormat> fmt::Debug for Spawner<Fmt> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Spawner").finish_non_exhaustive()
    }
}

impl<Fmt: HandleFormat> Spawner<Fmt> {
    fn new(interface: &Interface) -> Self {
        let config = interface.handles().map(|(path, spec)| {
            let config = spec
                .as_ref()
                .map_receiver(|_| SharedSpawnConfig::default())
                .map_sender(|spec| {
                    if spec.worker.is_some() {
                        SharedSpawnConfig::new(Cell::new(ChannelSpawnConfig::Closed))
                    } else {
                        SharedSpawnConfig::default()
                    }
                });
            (path.to_owned(), config)
        });

        Self {
            channels: config.collect(),
        }
    }
}

impl<Fmt: HandleFormat> HandleFormat for Spawner<Fmt> {
    type RawReceiver = ReceiverConfig<Fmt, Vec<u8>, Raw>;
    type Receiver<T, C: Codec<T>> = ReceiverConfig<Fmt, T, C>;
    type RawSender = SenderConfig<Fmt, Vec<u8>, Raw>;
    type Sender<T, C: Codec<T>> = SenderConfig<Fmt, T, C>;
}

/// Configurator of a workflow channel [`Receiver`](crate::channel::Receiver).
pub struct ReceiverConfig<Fmt: HandleFormat, T, C> {
    inner: SharedSpawnConfig<Fmt::RawReceiver>,
    _ty: PhantomData<fn(C) -> T>,
}

impl<Fmt, T, C> fmt::Debug for ReceiverConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    Fmt::RawReceiver: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.take();
        let result = fmt::Debug::fmt(&inner, formatter);
        self.inner.set(inner);
        result
    }
}

impl<Fmt, T, C> ReceiverConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    /// Closes the channel immediately on workflow instantiation.
    pub fn close(&self) {
        self.inner.set(ChannelSpawnConfig::Closed);
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
            inner: raw.inner,
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
            inner: self.inner,
            _ty: PhantomData,
        }
    }
}

/// Configurator of a workflow channel [`Sender`](crate::channel::Sender).
pub struct SenderConfig<Fmt: HandleFormat, T, C> {
    inner: SharedSpawnConfig<Fmt::RawSender>,
    _ty: PhantomData<fn(C) -> T>,
}

impl<Fmt, T, C> fmt::Debug for SenderConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    Fmt::RawSender: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.take();
        let result = fmt::Debug::fmt(&inner, formatter);
        self.inner.set(inner);
        result
    }
}

impl<Fmt, T, C> SenderConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    /// Closes the channel immediately on workflow instantiation.
    pub fn close(&self) {
        self.inner.set(ChannelSpawnConfig::Closed);
    }

    /// Copies the channel from the provided `sender`. Thus, the created workflow will send
    /// messages over the same channel as `sender`.
    pub fn copy_from(&self, sender: Fmt::Sender<T, C>) {
        self.inner
            .set(ChannelSpawnConfig::Existing(sender.into_raw()));
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
            inner: raw.inner,
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
            inner: self.inner,
            _ty: PhantomData,
        }
    }
}

/// Builder of handle pairs: ones to be retained in the parent workflow and ones to be provided
/// to the child workflow.
pub(super) struct HandlesBuilder<Fmt: HandleFormat = Wasm> {
    spawner: Spawner<Fmt>,
}

impl<Fmt: HandleFormat> fmt::Debug for HandlesBuilder<Fmt>
where
    Fmt::RawReceiver: fmt::Debug,
    Fmt::RawSender: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HandlesBuilder")
            .field("spawner", &self.spawner)
            .finish()
    }
}

impl<Fmt: HandleFormat> HandlesBuilder<Fmt> {
    /// Creates a new builder based on the interface of `W`.
    ///
    /// **Important.** The returned config (second param) must be dropped before calling
    /// `Self::build()`.
    pub fn new<W: WithHandle>(interface: &Interface) -> (Self, InEnv<W, Spawner<Fmt>>) {
        let (this, untyped) = Self::new_untyped(interface);
        let config = W::try_from_untyped(untyped).unwrap();
        (this, config)
    }

    fn new_untyped(interface: &Interface) -> (Self, UntypedHandles<Spawner<Fmt>>) {
        let spawner = Spawner::new(interface);
        let untyped = spawner.channels.iter().map(|(path, spec)| {
            let config = spec
                .as_ref()
                .map_receiver(|cell| ReceiverConfig {
                    inner: Rc::clone(cell),
                    _ty: PhantomData,
                })
                .map_sender(|cell| SenderConfig {
                    inner: Rc::clone(cell),
                    _ty: PhantomData,
                });
            (path.clone(), config)
        });
        let untyped = untyped.collect();
        (Self { spawner }, untyped)
    }
}

impl<Fmt: HandleFormat> HandlesBuilder<Fmt> {
    pub async fn build<M>(self, manager: &M) -> ForSelf<Fmt>
    where
        M: CreateChannel<Fmt = Fmt>,
    {
        let channels = self.spawner.channels;

        let pairs = channels.into_iter().map(|(path, config)| async {
            let pair = match config {
                Handle::Receiver(config) => {
                    let config = config.take();
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        %path, remote = "receiver", config = ?config.drop_payload(),
                        "creating channel"
                    );

                    Handle::Receiver(ChannelPair::for_remote_receiver(config, manager).await)
                }
                Handle::Sender(config) => {
                    let config = config.take();
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        %path, remote = "sender", config = ?config.drop_payload(),
                        "creating channel"
                    );

                    Handle::Sender(ChannelPair::for_remote_sender(config, manager).await)
                }
            };
            (path, pair)
        });
        let pairs = future::join_all(pairs).await;

        ForSelf {
            inner: pairs.into_iter().collect(),
        }
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
        manager: &impl CreateChannel<Fmt = Fmt>,
    ) -> Self {
        let (sender, receiver) = match config {
            ChannelSpawnConfig::New => manager.new_channel().await,
            ChannelSpawnConfig::Closed => (manager.closed_sender(), manager.closed_receiver()),
            ChannelSpawnConfig::Existing(rx) => (manager.closed_sender(), rx),
        };
        Self::new(sender, receiver)
    }

    async fn for_remote_sender(
        config: ChannelSpawnConfig<Fmt::RawSender>,
        manager: &impl CreateChannel<Fmt = Fmt>,
    ) -> Self {
        let (sender, receiver) = match config {
            ChannelSpawnConfig::New => manager.new_channel().await,
            ChannelSpawnConfig::Closed => (manager.closed_sender(), manager.closed_receiver()),
            ChannelSpawnConfig::Existing(sx) => (sx, manager.closed_receiver()),
        };
        Self::new(sender, receiver)
    }
}

/// Handles format for handles retained for the parent workflow of a spawned child.
pub(super) struct ForSelf<Fmt: HandleFormat> {
    inner: HandleMap<ChannelPair<Fmt>>,
}

impl<Fmt: HandleFormat> ForSelf<Fmt> {
    pub fn for_child(self) -> ForChild<Fmt> {
        ForChild { inner: self.inner }
    }
}

impl<Fmt: HandleFormat> TakeHandles<Inverse<Fmt>> for ForSelf<Fmt> {
    fn take_receiver(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawSender, AccessError> {
        let pair = ReceiverAt(path).get_mut_from(&mut self.inner)?;
        pair.sender.take().ok_or_else(|| {
            AccessErrorKind::custom("attempted to take the same handle twice")
                .with_location(ReceiverAt(path))
        })
    }

    fn take_sender(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawReceiver, AccessError> {
        let pair = SenderAt(path).get_mut_from(&mut self.inner)?;
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

pub(super) struct ForChild<Fmt: HandleFormat> {
    inner: HandleMap<ChannelPair<Fmt>>,
}

impl<Fmt: HandleFormat> TakeHandles<Fmt> for ForChild<Fmt> {
    fn take_receiver(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawReceiver, AccessError> {
        let pair = ReceiverAt(path).get_mut_from(&mut self.inner)?;
        pair.receiver.take().ok_or_else(|| {
            AccessErrorKind::custom("attempted to take the same handle twice")
                .with_location(ReceiverAt(path))
        })
    }

    fn take_sender(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawSender, AccessError> {
        let pair = SenderAt(path).get_mut_from(&mut self.inner)?;
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
