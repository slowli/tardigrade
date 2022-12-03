//! Spawning and managing child workflows.
//!
//! # Examples
//!
//! ```
//! # use async_trait::async_trait;
//! # use futures::{SinkExt, StreamExt};
//! #
//! # use std::error;
//! #
//! # use tardigrade::{
//! #     channel::{Sender, Receiver},
//! #     task::TaskResult,
//! #     workflow::{GetInterface, InEnv, SpawnWorkflow, TakeHandle, Wasm, WorkflowEnv, WorkflowFn},
//! #     Json,
//! # };
//! // Assume we want to spawn a child workflow defined as follows:
//! #[derive(Debug, GetInterface, TakeHandle)]
//! #[tardigrade(handle = "ChildHandle", auto_interface)]
//! pub struct ChildWorkflow(());
//!
//! #[derive(TakeHandle)]
//! #[tardigrade(derive(Debug))]
//! pub struct ChildHandle<Env: WorkflowEnv> {
//!     pub commands: InEnv<Receiver<String, Json>, Env>,
//!     pub events: InEnv<Sender<String, Json>, Env>,
//! }
//!
//! impl WorkflowFn for ChildWorkflow {
//!     type Args = ();
//!     type Codec = Json;
//! }
//! # #[async_trait(?Send)]
//! # impl SpawnWorkflow for ChildWorkflow {
//! #     async fn spawn(_args: (), handle: ChildHandle<Wasm>) -> TaskResult {
//! #         handle.commands.map(Ok).forward(handle.events).await?;
//! #         Ok(())
//! #     }
//! # }
//!
//! // To spawn a workflow, we should use the following code
//! // in the parent workflow:
//! use tardigrade::spawn::{ManageWorkflowsExt, Workflows, WorkflowHandle};
//! # use tardigrade::test::Runtime;
//!
//! # let mut runtime = Runtime::default();
//! # runtime.workflow_registry_mut().insert::<ChildWorkflow>("child");
//! # runtime.run(async {
//! let builder = Workflows.new_workflow::<ChildWorkflow>("child", ())?;
//! // It is possible to customize child workflow initialization via
//! // `builder.handle()`, but zero config is fine as well.
//! let mut child = builder.build().await?;
//! // `child` contains handles to the created channels.
//! child.api.commands.send("ping".to_owned()).await?;
//! # drop(child.api.commands);
//! let event: String = child.api.events.next().await.unwrap();
//! # assert_eq!(event, "ping");
//! child.workflow.await?;
//! # Ok::<_, Box<dyn error::Error>>(())
//! # }).unwrap();
//! ```

use async_trait::async_trait;
use futures::future;
use pin_project_lite::pin_project;

use std::{
    borrow::Cow,
    cell::RefCell,
    convert::Infallible,
    fmt,
    future::Future,
    marker::PhantomData,
    mem,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

pub use crate::error::HostError;

use crate::channel::{channel, RawReceiver, RawSender};
use crate::workflow::TakeHandles;
use crate::{
    channel::{Receiver, Sender},
    interface::{
        AccessError, AccessErrorKind, Handle, HandleMap, HandleMapKey, HandlePath, HandlePathBuf,
        Interface, ReceiverAt, SenderAt,
    },
    task::JoinError,
    workflow::{
        GetInterface, HandleFormat, InEnv, IntoRaw, TryFromRaw, UntypedHandles, Wasm, WithHandle,
        WorkflowFn,
    },
    Codec,
};

#[cfg(target_arch = "wasm32")]
#[path = "imp_wasm32.rs"]
pub(crate) mod imp;
#[cfg(not(target_arch = "wasm32"))]
#[path = "imp_mock.rs"]
mod imp;

/// Manager of [`Interface`]s that allows obtaining an interface by a string identifier.
pub trait ManageInterfaces {
    /// Returns the interface spec of a workflow with the specified ID.
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>>;
}

/// Manager of workflows that allows spawning workflows of a certain type.
///
/// This trait is low-level; use [`ManageWorkflowsExt`] for a high-level alternative.
#[async_trait]
pub trait ManageWorkflows: ManageInterfaces {
    /// Handle to an instantiated workflow.
    type Spawned<'s, W: WorkflowFn + GetInterface>
    where
        Self: 's;
    /// Error spawning a workflow.
    type Error: 'static + Send + Sync;
    /// Format of handles that this manager operates in.
    type Fmt: HandleFormat;

    #[doc(hidden)] // implementation detail; should only be used via `WorkflowBuilder`
    async fn new_workflow_raw(
        &self,
        definition_id: &str,
        args: Vec<u8>,
        handles: UntypedHandles<Self::Fmt>,
    ) -> Result<Self::Spawned<'_, ()>, Self::Error>;

    #[doc(hidden)]
    fn downcast<W: WorkflowFn + GetInterface>(
        &self,
        spawned: Self::Spawned<'_, ()>,
    ) -> Self::Spawned<'_, W>;

    /// Initiates creating a new workflow and returns the corresponding builder.
    ///
    /// # Errors
    ///
    /// Returns an error on interface mismatch between `W` and the workflow definition
    /// contained in this manager under `definition_id`.
    fn new_workflow<'a, W: WorkflowFn + GetInterface>(
        &'a self,
        definition_id: &'a str,
    ) -> Result<WorkflowBuilder<'a, Self, W>, AccessError>
    where
        Self: Sized,
    {
        let provided_interface = self
            .interface(definition_id)
            .ok_or(AccessErrorKind::Unknown)?;
        W::interface().check_compatibility(&provided_interface)?;

        Ok(WorkflowBuilder {
            manager: self,
            definition_id,
            _ty: PhantomData,
        })
    }
}

/// Builder of child workflows.
pub struct WorkflowBuilder<'a, M: ManageWorkflows, W> {
    manager: &'a M,
    definition_id: &'a str,
    _ty: PhantomData<W>,
}

impl<M: ManageWorkflows, W> fmt::Debug for WorkflowBuilder<'_, M, W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowBuilder")
            .field("definition_id", &self.definition_id)
            .finish()
    }
}

impl<'a, M, W> WorkflowBuilder<'a, M, W>
where
    M: ManageWorkflows,
    W: WorkflowFn + GetInterface,
{
    /// Builds the handles pair allowing to configure handles in the process.
    pub async fn handles<F>(&self, config_fn: F) -> (InEnv<W, Wasm>, InEnv<W, ForSelf>)
    where
        F: FnOnce(&InEnv<W, Spawner>),
    {
        let handles_builder = HandlesBuilder::<W>::new();
        config_fn(handles_builder.config());
        handles_builder.build().await
    }

    /// Creates a new workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be spawned, e.g., if the provided `args`
    /// are incorrect.
    pub async fn build(
        self,
        args: W::Args,
        handles: W::Handle<M::Fmt>,
    ) -> Result<M::Spawned<'a, W>, M::Error> {
        let raw_args = <W::Codec>::encode_value(args);
        let raw_handles = W::into_untyped(handles);
        let spawned = self
            .manager
            .new_workflow_raw(self.definition_id, raw_args, raw_handles)
            .await?;
        Ok(self.manager.downcast(spawned))
    }
}

/// Client-side connection to a [workflow manager][`ManageWorkflows`].
#[derive(Debug)]
pub struct Workflows;

#[async_trait]
impl ManageWorkflows for Workflows {
    type Spawned<'s, W: WorkflowFn + GetInterface> = RemoteWorkflow;
    type Error = HostError;
    type Fmt = Wasm;

    async fn new_workflow_raw(
        &self,
        definition_id: &str,
        args: Vec<u8>,
        handles: UntypedHandles<Self::Fmt>,
    ) -> Result<Self::Spawned<'_, ()>, Self::Error> {
        imp::new_workflow(definition_id, args, handles).await
    }

    fn downcast<W: WorkflowFn + GetInterface>(
        &self,
        spawned: Self::Spawned<'_, ()>,
    ) -> Self::Spawned<'_, W> {
        spawned
    }
}

pin_project! {
    /// Handle to a remote workflow (usually, a child workflow previously spawned using
    /// [`Workflows`]).
    ///
    /// The handle can be polled for completion.
    #[derive(Debug)]
    #[repr(transparent)]
    pub struct RemoteWorkflow {
        #[pin]
        inner: imp::RemoteWorkflow,
    }
}

// TODO: allow to abort workflows for symmetry with tasks

impl Future for RemoteWorkflow {
    type Output = Result<(), JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/// Configuration for a single workflow channel during workflow instantiation.
#[derive(Debug)]
pub enum ChannelSpawnConfig<T> {
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

/// Spawn [environment](TakeHandle) that can be used to configure channels before spawning
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
    type RawReceiver = ReceiverConfig<Fmt, (), ()>;
    type Receiver<T, C: Codec<T>> = ReceiverConfig<Fmt, T, C>;
    type RawSender = SenderConfig<Fmt, (), ()>;
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

impl<Fmt, T, C> TryFromRaw<ReceiverConfig<Fmt, (), ()>> for ReceiverConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    type Error = Infallible;

    #[inline]
    fn try_from_raw(raw: ReceiverConfig<Fmt, (), ()>) -> Result<Self, Self::Error> {
        Ok(Self {
            spawner: raw.spawner,
            path: raw.path,
            _ty: PhantomData,
        })
    }
}

impl<Fmt, T, C> IntoRaw<ReceiverConfig<Fmt, (), ()>> for ReceiverConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    #[inline]
    fn into_raw(self) -> ReceiverConfig<Fmt, (), ()> {
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

impl<Fmt, T, C> TryFromRaw<SenderConfig<Fmt, (), ()>> for SenderConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    type Error = Infallible;

    #[inline]
    fn try_from_raw(raw: SenderConfig<Fmt, (), ()>) -> Result<Self, Self::Error> {
        Ok(Self {
            spawner: raw.spawner,
            path: raw.path,
            _ty: PhantomData,
        })
    }
}

impl<Fmt, T, C> IntoRaw<SenderConfig<Fmt, (), ()>> for SenderConfig<Fmt, T, C>
where
    Fmt: HandleFormat,
    C: Codec<T>,
{
    #[inline]
    fn into_raw(self) -> SenderConfig<Fmt, (), ()> {
        SenderConfig {
            spawner: self.spawner,
            path: self.path,
            _ty: PhantomData,
        }
    }
}

/// Builder of handle pairs: ones to be retained in the parent workflow and ones to be provided
/// to the child workflow.
pub struct HandlesBuilder<W: WithHandle, Fmt: HandleFormat = Wasm> {
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

impl<W, Fmt> Default for HandlesBuilder<W, Fmt>
where
    W: GetInterface + WithHandle,
    Fmt: HandleFormat,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<W, Fmt> HandlesBuilder<W, Fmt>
where
    Fmt: HandleFormat,
    W: GetInterface + WithHandle,
{
    /// Creates a new builder based on the interface of `W`.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn new() -> Self {
        let interface = W::interface();
        let spawner = Spawner {
            inner: Rc::new(SpawnerInner::new(&interface)),
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
        let untyped = UntypedHandles {
            handles: untyped.collect(),
        };

        let config = W::try_from_untyped(untyped).unwrap();
        Self { spawner, config }
    }

    /// Returns a [handle](TakeHandle) that can be used to configure created workflow channels.
    pub fn config(&self) -> &InEnv<W, Spawner<Fmt>> {
        &self.config
    }
}

impl<W: WithHandle> HandlesBuilder<W> {
    /// Builds the handles pair. This method must be called in the client environment.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub async fn build(self) -> (InEnv<W, Wasm>, InEnv<W, ForSelf>) {
        drop(self.config);
        let spawner = Rc::try_unwrap(self.spawner.inner).map_err(drop).unwrap();
        let channels = spawner.channels.into_inner();

        let pairs = channels.into_iter().map(|(path, config)| async {
            let pair = match config {
                Handle::Receiver(rx_config) => {
                    Handle::Receiver(ChannelPair::for_remote_receiver(rx_config).await)
                }
                Handle::Sender(sx_config) => {
                    Handle::Sender(ChannelPair::for_remote_sender(sx_config).await)
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
struct ChannelPair {
    sender: Option<RawSender>,
    receiver: Option<RawReceiver>,
}

impl ChannelPair {
    fn new(sender: RawSender, receiver: RawReceiver) -> Self {
        Self {
            sender: Some(sender),
            receiver: Some(receiver),
        }
    }

    async fn for_remote_receiver(config: ChannelSpawnConfig<RawReceiver>) -> Self {
        let (sender, receiver) = match config {
            ChannelSpawnConfig::New => channel().await,
            ChannelSpawnConfig::Closed => (RawSender::closed(), RawReceiver::closed()),
            ChannelSpawnConfig::Existing(rx) => (RawSender::closed(), rx),
        };
        Self::new(sender, receiver)
    }

    async fn for_remote_sender(config: ChannelSpawnConfig<RawSender>) -> Self {
        let (sender, receiver) = match config {
            ChannelSpawnConfig::New => channel().await,
            ChannelSpawnConfig::Closed => (RawSender::closed(), RawReceiver::closed()),
            ChannelSpawnConfig::Existing(sx) => (sx, RawReceiver::closed()),
        };
        Self::new(sender, receiver)
    }
}

/// Handles format for handles retained for the parent workflow of a spawned child.
#[derive(Debug)]
pub struct ForSelf {
    inner: HandleMap<ChannelPair>,
}

impl HandleFormat for ForSelf {
    type RawReceiver = RawSender;
    type Receiver<T, C: Codec<T>> = Sender<T, C>;
    type RawSender = RawReceiver;
    type Sender<T, C: Codec<T>> = Receiver<T, C>;
}

impl TakeHandles<Self> for ForSelf {
    fn take_receiver(&mut self, path: HandlePath<'_>) -> Result<RawSender, AccessError> {
        let pair = ReceiverAt(path).get_mut(&mut self.inner)?;
        pair.sender.take().ok_or_else(|| {
            AccessErrorKind::custom("attempted to take the same handle twice")
                .with_location(ReceiverAt(path))
        })
    }

    fn take_sender(&mut self, path: HandlePath<'_>) -> Result<RawReceiver, AccessError> {
        let pair = SenderAt(path).get_mut(&mut self.inner)?;
        pair.receiver.take().ok_or_else(|| {
            AccessErrorKind::custom("attempted to take the same handle twice")
                .with_location(ReceiverAt(path))
        })
    }

    fn drain(&mut self) -> UntypedHandles<Self> {
        let handles = self.inner.iter_mut().filter_map(|(path, handle)| {
            let handle = match handle {
                Handle::Receiver(pair) => Handle::Receiver(pair.sender.take()?),
                Handle::Sender(pair) => Handle::Sender(pair.receiver.take()?),
            };
            Some((path.clone(), handle))
        });
        UntypedHandles {
            handles: handles.collect(),
        }
    }
}

#[derive(Debug)]
struct ForChild {
    inner: HandleMap<ChannelPair>,
}

impl TakeHandles<Wasm> for ForChild {
    fn take_receiver(&mut self, path: HandlePath<'_>) -> Result<RawReceiver, AccessError> {
        let pair = ReceiverAt(path).get_mut(&mut self.inner)?;
        pair.receiver.take().ok_or_else(|| {
            AccessErrorKind::custom("attempted to take the same handle twice")
                .with_location(ReceiverAt(path))
        })
    }

    fn take_sender(&mut self, path: HandlePath<'_>) -> Result<RawSender, AccessError> {
        let pair = SenderAt(path).get_mut(&mut self.inner)?;
        pair.sender.take().ok_or_else(|| {
            AccessErrorKind::custom("attempted to take the same handle twice")
                .with_location(ReceiverAt(path))
        })
    }

    fn drain(&mut self) -> UntypedHandles<Wasm> {
        let handles = mem::take(&mut self.inner).into_iter();
        let handles = handles.filter_map(|(path, handle)| {
            let handle = match handle {
                Handle::Receiver(pair) => Handle::Receiver(pair.receiver?),
                Handle::Sender(pair) => Handle::Sender(pair.sender?),
            };
            Some((path, handle))
        });
        UntypedHandles {
            handles: handles.collect(),
        }
    }
}
