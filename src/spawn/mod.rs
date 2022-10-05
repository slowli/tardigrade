//! Spawning and managing child workflows.
//!
//! # Examples
//!
//! ```
//! # use futures::{SinkExt, StreamExt};
//! # use std::error;
//! # use tardigrade::{
//! #     channel::{Sender, Receiver},
//! #     workflow::{GetInterface, Handle, SpawnWorkflow, TaskHandle, TakeHandle, Wasm, WorkflowFn},
//! #     Json,
//! # };
//! // Assume we want to spawn a child workflow defined as follows:
//! #[derive(Debug, GetInterface, TakeHandle)]
//! # #[tardigrade(handle = "ChildHandle", auto_interface)]
//! pub struct ChildWorkflow(());
//!
//! #[tardigrade::handle]
//! #[derive(Debug)]
//! pub struct ChildHandle<Env> {
//!     pub commands: Handle<Receiver<String, Json>, Env>,
//!     pub events: Handle<Sender<String, Json>, Env>,
//! }
//!
//! impl WorkflowFn for ChildWorkflow {
//!     type Args = ();
//!     type Codec = Json;
//! }
//! # impl SpawnWorkflow for ChildWorkflow {
//! #     fn spawn(_args: (), handle: ChildHandle<Wasm>) -> TaskHandle {
//! #         TaskHandle::new(async move {
//! #             handle.commands.map(Ok).forward(handle.events).await.unwrap();
//! #         })
//! #     }
//! # }
//!
//! // To spawn a workflow, we should use the following code
//! // in the parent workflow:
//! use tardigrade::spawn::{ManageWorkflowsExt, Workflows, WorkflowHandle};
//! # use tardigrade::test::Runtime;
//!
//! # let mut runtime = Runtime::default();
//! # runtime.workflow_registry_mut().insert::<ChildWorkflow, _>("child");
//! # runtime.run(async {
//! let builder = Workflows.new_workflow::<ChildWorkflow>("child", ())?;
//! // It is possible to customize child workflow initialization via
//! // `builder.handle()`, but zero config is fine as well.
//! let mut child = builder.build()?;
//! // `child` contains handles to the created channels.
//! child.api.commands.send("ping".to_owned()).await?;
//! # drop(child.api.commands);
//! let event: String = child.api.events.next().await.unwrap();
//! # assert_eq!(event, "ping");
//! child.workflow.await?;
//! # Ok::<_, Box<dyn error::Error>>(())
//! # }).unwrap();
//! ```

use futures::{
    stream::{Fuse, FusedStream},
    FutureExt, Sink, Stream, StreamExt,
};
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};

use std::{
    borrow::Cow,
    cell::RefCell,
    collections::HashMap,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

pub use tardigrade_shared::SpawnError;

use crate::{
    channel::{RawReceiver, RawSender, Receiver, SendError, Sender},
    interface::{
        AccessError, AccessErrorKind, ChannelKind, InboundChannel, Interface, OutboundChannel,
    },
    trace::{FutureUpdate, TracedFutures, Tracer},
    workflow::{GetInterface, TakeHandle, UntypedHandle, WorkflowFn},
    Decode, Encode,
};
use tardigrade_shared::JoinError;

#[cfg(target_arch = "wasm32")]
#[path = "imp_wasm32.rs"]
pub(crate) mod imp;
#[cfg(not(target_arch = "wasm32"))]
#[path = "imp_mock.rs"]
mod imp;

/// Configuration for a single workflow channel during workflow instantiation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[doc(hidden)] // used only in low-level `ManageWorkflows` API
#[allow(clippy::unsafe_derive_deserialize)] // unsafe methods do not concern type data
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

impl<T> ChannelSpawnConfig<T> {
    pub fn map_ref<U>(&self, map_fn: impl FnOnce(&T) -> U) -> ChannelSpawnConfig<U> {
        match self {
            Self::New => ChannelSpawnConfig::New,
            Self::Closed => ChannelSpawnConfig::Closed,
            Self::Existing(value) => ChannelSpawnConfig::Existing(map_fn(value)),
        }
    }
}

/// Configuration of the spawned workflow channels.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[doc(hidden)] // used only in low-level `ManageWorkflows` API
#[allow(clippy::unsafe_derive_deserialize)] // unsafe methods do not concern type data
pub struct ChannelsConfig<In, Out = In> {
    /// Configurations of inbound channels.
    pub inbound: HashMap<String, ChannelSpawnConfig<In>>,
    /// Configurations of outbound channels.
    pub outbound: HashMap<String, ChannelSpawnConfig<Out>>,
}

impl<In, Out> Default for ChannelsConfig<In, Out> {
    fn default() -> Self {
        Self {
            inbound: HashMap::new(),
            outbound: HashMap::new(),
        }
    }
}

impl<In, Out> ChannelsConfig<In, Out> {
    /// Creates channel configuration from the provided interface.
    pub fn from_interface(interface: &Interface) -> Self {
        let inbound = interface
            .inbound_channels()
            .map(|(name, _)| (name.to_owned(), ChannelSpawnConfig::default()))
            .collect();
        let outbound = interface
            .outbound_channels()
            .map(|(name, _)| (name.to_owned(), ChannelSpawnConfig::default()))
            .collect();
        Self { inbound, outbound }
    }

    fn close_channel(&mut self, kind: ChannelKind, name: &str) {
        match kind {
            ChannelKind::Inbound => {
                *self.inbound.get_mut(name).unwrap() = ChannelSpawnConfig::Closed;
            }
            ChannelKind::Outbound => {
                *self.outbound.get_mut(name).unwrap() = ChannelSpawnConfig::Closed;
            }
        }
    }

    fn copy_outbound_channel(&mut self, name: &str, source: Out) {
        *self.outbound.get_mut(name).unwrap() = ChannelSpawnConfig::Existing(source);
    }
}

/// Manager of [`Interface`]s that allows obtaining an interface by a string identifier.
pub trait ManageInterfaces {
    /// Returns the interface spec of a workflow with the specified ID.
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>>;
}

/// Specifier of inbound and outbound channel handles when [spawning workflows](WorkflowBuilder).
///
/// Depending on the environment (e.g., workflow code vs host code), channel handles
/// can be specified in different ways. This trait encapsulates this variability.
pub trait SpecifyWorkflowChannels {
    /// Type of an inbound channel handle.
    type Inbound;
    /// Type of an outbound channel handle.
    type Outbound;
}

/// Manager of workflows that allows spawning workflows of a certain type.
///
/// This trait is low-level; use [`ManageWorkflowsExt`] for a high-level alternative.
pub trait ManageWorkflows<'a, W: WorkflowFn>: ManageInterfaces + SpecifyWorkflowChannels {
    /// Handle to an instantiated workflow.
    type Handle;
    /// Error spawning a workflow.
    type Error: 'static + Send + Sync;

    /// Creates a new workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be spawned, e.g., if the provided `args`
    /// are incorrect.
    #[doc(hidden)] // implementation detail; should only be used via `WorkflowBuilder`
    fn create_workflow(
        &'a self,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<Self::Inbound, Self::Outbound>,
    ) -> Result<Self::Handle, Self::Error>;
}

/// Extension trait for [workflow managers](ManageWorkflows).
pub trait ManageWorkflowsExt<'a>: ManageWorkflows<'a, ()> + Sized {
    /// Returns a workflow builder for the specified definition and args.
    ///
    /// # Errors
    ///
    /// Returns an error if the definition is unknown, or does not conform to the interface
    /// specified via the type param of this trait.
    fn new_workflow<W>(
        &'a self,
        definition_id: &'a str,
        args: W::Args,
    ) -> Result<WorkflowBuilder<'a, Self, W>, AccessError>
    where
        W: WorkflowFn + GetInterface + TakeHandle<Spawner<Self>, Id = ()>,
        Self: ManageWorkflows<'a, W>,
    {
        let provided_interface = self
            .interface(definition_id)
            .ok_or(AccessErrorKind::Unknown)?;
        W::interface().check_compatibility(&provided_interface)?;
        Ok(WorkflowBuilder::new(
            self,
            provided_interface.into_owned(),
            definition_id,
            args,
        ))
    }
}

impl<'a, M: ManageWorkflows<'a, ()>> ManageWorkflowsExt<'a> for M {}

/// Client-side connection to a [workflow manager][`ManageWorkflows`].
#[derive(Debug)]
pub struct Workflows;

impl SpecifyWorkflowChannels for Workflows {
    type Inbound = RawReceiver;
    type Outbound = RawSender;
}

impl<'a, W> ManageWorkflows<'a, W> for Workflows
where
    W: WorkflowFn + TakeHandle<RemoteWorkflow, Id = ()>,
{
    type Handle = WorkflowHandle<W>;
    type Error = SpawnError;

    fn create_workflow(
        &'a self,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<RawReceiver, RawSender>,
    ) -> Result<Self::Handle, Self::Error> {
        let mut workflow = <Self as ManageWorkflows<'a, ()>>::create_workflow(
            self,
            definition_id,
            args,
            channels,
        )?;
        let api = W::take_handle(&mut workflow, &()).unwrap();
        Ok(WorkflowHandle { api, workflow })
    }
}

/// Wrapper for remote components of a workflow, such as [`Sender`]s and [`Receiver`]s.
#[derive(Debug)]
#[non_exhaustive]
pub enum Remote<T> {
    /// The remote handle is not captured.
    NotCaptured,
    /// The remote handle is available.
    Some(T),
}

impl<T> Remote<T> {
    /// Unwraps and returns the contained handle.
    ///
    /// # Panics
    ///
    /// Panics if the handle is not captured.
    pub fn unwrap(self) -> T {
        match self {
            Self::Some(value) => value,
            Self::NotCaptured => panic!("handle not captured"),
        }
    }

    fn map<U>(self, map_fn: impl FnOnce(T) -> U) -> Remote<U> {
        match self {
            Self::NotCaptured => Remote::NotCaptured,
            Self::Some(value) => Remote::Some(map_fn(value)),
        }
    }
}

#[derive(Debug)]
struct SpawnerInner<Ch: SpecifyWorkflowChannels> {
    definition_id: String,
    interface: Interface,
    args: Vec<u8>,
    channels: RefCell<ChannelsConfig<Ch::Inbound, Ch::Outbound>>,
}

impl<Ch: SpecifyWorkflowChannels> SpawnerInner<Ch> {
    fn new(interface: Interface, definition_id: &str, args: Vec<u8>) -> Self {
        Self {
            definition_id: definition_id.to_owned(),
            channels: RefCell::new(ChannelsConfig::from_interface(&interface)),
            interface,
            args,
        }
    }

    fn close_inbound_channel(&self, channel_name: &str) {
        let mut borrow = self.channels.borrow_mut();
        borrow.close_channel(ChannelKind::Inbound, channel_name);
    }

    fn close_outbound_channel(&self, channel_name: &str) {
        let mut borrow = self.channels.borrow_mut();
        borrow.close_channel(ChannelKind::Outbound, channel_name);
    }

    fn copy_outbound_channel(&self, channel_name: &str, sender: Ch::Outbound) {
        let mut borrow = self.channels.borrow_mut();
        borrow.copy_outbound_channel(channel_name, sender);
    }
}

/// Spawn [environment](TakeHandle) that can be used to configure channels before spawning
/// a workflow.
pub struct Spawner<Ch: SpecifyWorkflowChannels = Workflows> {
    inner: Rc<SpawnerInner<Ch>>,
}

impl<Ch> fmt::Debug for Spawner<Ch>
where
    Ch: SpecifyWorkflowChannels + fmt::Debug,
    Ch::Inbound: fmt::Debug,
    Ch::Outbound: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Spawner")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<Ch: SpecifyWorkflowChannels> Clone for Spawner<Ch> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<Ch: SpecifyWorkflowChannels> TakeHandle<Spawner<Ch>> for Interface {
    type Id = ();
    type Handle = Self;

    fn take_handle(env: &mut Spawner<Ch>, _id: &Self::Id) -> Result<Self::Handle, AccessError> {
        Ok(env.inner.interface.clone())
    }
}

impl<Ch: SpecifyWorkflowChannels> TakeHandle<Spawner<Ch>> for () {
    type Id = ();
    type Handle = UntypedHandle<Spawner<Ch>>;

    fn take_handle(env: &mut Spawner<Ch>, _id: &Self::Id) -> Result<Self::Handle, AccessError> {
        UntypedHandle::take_handle(env, &())
    }
}

/// Configurator of an [inbound workflow channel](Receiver).
pub struct ReceiverConfig<Ch: SpecifyWorkflowChannels, T, C> {
    spawner: Spawner<Ch>,
    channel_name: String,
    _ty: PhantomData<fn(C) -> T>,
}

impl<Ch, T, C> fmt::Debug for ReceiverConfig<Ch, T, C>
where
    Ch: SpecifyWorkflowChannels + fmt::Debug,
    Ch::Inbound: fmt::Debug,
    Ch::Outbound: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReceiverConfig")
            .field("spawner", &self.spawner)
            .field("channel_name", &self.channel_name)
            .finish()
    }
}

impl<Ch, T, C> ReceiverConfig<Ch, T, C>
where
    Ch: SpecifyWorkflowChannels,
    C: Encode<T>,
{
    /// Closes the channel immediately on workflow instantiation.
    pub fn close(&self) {
        self.spawner.inner.close_inbound_channel(&self.channel_name);
    }
}

impl<Ch, T, C> TakeHandle<Spawner<Ch>> for Receiver<T, C>
where
    Ch: SpecifyWorkflowChannels,
    C: Encode<T>,
{
    type Id = str;
    type Handle = ReceiverConfig<Ch, T, C>;

    fn take_handle(env: &mut Spawner<Ch>, id: &Self::Id) -> Result<Self::Handle, AccessError> {
        Ok(ReceiverConfig {
            spawner: env.clone(),
            channel_name: id.to_owned(),
            _ty: PhantomData,
        })
    }
}

/// Configurator of an [outbound workflow channel](Sender).
pub struct SenderConfig<Ch: SpecifyWorkflowChannels, T, C> {
    spawner: Spawner<Ch>,
    channel_name: String,
    _ty: PhantomData<fn(C) -> T>,
}

impl<Ch, T, C> fmt::Debug for SenderConfig<Ch, T, C>
where
    Ch: SpecifyWorkflowChannels + fmt::Debug,
    Ch::Inbound: fmt::Debug,
    Ch::Outbound: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SenderConfig")
            .field("spawner", &self.spawner)
            .field("channel_name", &self.channel_name)
            .finish()
    }
}

impl<Ch, T, C> SenderConfig<Ch, T, C>
where
    Ch: SpecifyWorkflowChannels,
    C: Encode<T>,
{
    /// Closes the channel immediately on workflow instantiation.
    pub fn close(&self) {
        self.spawner
            .inner
            .close_outbound_channel(&self.channel_name);
    }
}

impl<T, C: Encode<T>> SenderConfig<Workflows, T, C> {
    /// Copies the channel from the provided `sender`. Thus, the created workflow will send
    /// messages over the same channel as `sender`.
    pub fn copy_from(&self, sender: Sender<T, C>) {
        self.spawner
            .inner
            .copy_outbound_channel(&self.channel_name, sender.into_raw());
    }
}

impl<Ch, T, C> TakeHandle<Spawner<Ch>> for Sender<T, C>
where
    Ch: SpecifyWorkflowChannels,
    C: Encode<T>,
{
    type Id = str;
    type Handle = SenderConfig<Ch, T, C>;

    fn take_handle(env: &mut Spawner<Ch>, id: &Self::Id) -> Result<Self::Handle, AccessError> {
        Ok(SenderConfig {
            spawner: env.clone(),
            channel_name: id.to_owned(),
            _ty: PhantomData,
        })
    }
}

impl<Ch, C> TakeHandle<Spawner<Ch>> for Tracer<C>
where
    Ch: SpecifyWorkflowChannels,
    C: Encode<FutureUpdate>,
{
    type Id = str;
    type Handle = SenderConfig<Ch, FutureUpdate, C>;

    fn take_handle(env: &mut Spawner<Ch>, id: &Self::Id) -> Result<Self::Handle, AccessError> {
        Ok(SenderConfig {
            spawner: env.clone(),
            channel_name: id.to_owned(),
            _ty: PhantomData,
        })
    }
}

/// Builder allowing to configure workflow aspects, such as channels, before instantiation.
pub struct WorkflowBuilder<'a, M, W>
where
    M: SpecifyWorkflowChannels,
    W: TakeHandle<Spawner<M>>,
{
    spawner: Spawner<M>,
    handle: <W as TakeHandle<Spawner<M>>>::Handle,
    manager: &'a M,
}

impl<M, W> fmt::Debug for WorkflowBuilder<'_, M, W>
where
    M: SpecifyWorkflowChannels,
    Spawner<M>: fmt::Debug,
    W: TakeHandle<Spawner<M>>,
    <W as TakeHandle<Spawner<M>>>::Handle: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SpawnBuilder")
            .field("spawner", &self.spawner)
            .field("handle", &self.handle)
            .finish()
    }
}

impl<'a, M, W> WorkflowBuilder<'a, M, W>
where
    M: ManageWorkflows<'a, W>,
    W: WorkflowFn + TakeHandle<Spawner<M>, Id = ()>,
{
    fn new(manager: &'a M, interface: Interface, definition_id: &str, args: W::Args) -> Self {
        let raw_args = W::Codec::default().encode_value(args);
        let mut spawner = Spawner {
            inner: Rc::new(SpawnerInner::new(interface, definition_id, raw_args)),
        };
        let handle = W::take_handle(&mut spawner, &()).unwrap();
        Self {
            spawner,
            handle,
            manager,
        }
    }

    /// Returns a [handle](TakeHandle) that can be used to configure created workflow channels.
    pub fn handle(&self) -> &<W as TakeHandle<Spawner<M>>>::Handle {
        &self.handle
    }

    /// Instantiates the child workflow and returns a handle to it.
    ///
    /// # Errors
    ///
    /// Returns an error if instantiation fails for whatever reason. Error handling is specific
    /// to the [manager](ManageWorkflows) being used.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn build(self) -> Result<M::Handle, M::Error> {
        drop(self.handle);
        let spawner = Rc::try_unwrap(self.spawner.inner).map_err(drop).unwrap();
        self.manager.create_workflow(
            &spawner.definition_id,
            spawner.args,
            spawner.channels.into_inner(),
        )
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

/// Handle to a remote workflow together with channel handles connected to the workflow.
#[non_exhaustive]
pub struct WorkflowHandle<W: TakeHandle<RemoteWorkflow>> {
    /// Channel handles associated with the workflow. Each inbound channel in the remote workflow
    /// is mapped to a local outbound channel, and vice versa.
    pub api: <W as TakeHandle<RemoteWorkflow>>::Handle,
    /// Workflow handle that can be polled for completion.
    pub workflow: RemoteWorkflow,
}

impl<W> fmt::Debug for WorkflowHandle<W>
where
    W: TakeHandle<RemoteWorkflow>,
    <W as TakeHandle<RemoteWorkflow>>::Handle: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowHandle")
            .field("api", &self.api)
            .field("workflow", &self.workflow)
            .finish()
    }
}

impl<T, C: Encode<T> + Default> TakeHandle<RemoteWorkflow> for Receiver<T, C> {
    type Id = str;
    type Handle = Remote<Sender<T, C>>;

    fn take_handle(env: &mut RemoteWorkflow, id: &str) -> Result<Self::Handle, AccessError> {
        let raw_sender = env
            .inner
            .take_inbound_channel(id)
            .ok_or_else(|| AccessErrorKind::Unknown.with_location(InboundChannel(id)))?;
        Ok(raw_sender.map(|raw| raw.with_codec(C::default())))
    }
}

impl<T, C: Decode<T>> Stream for Remote<Receiver<T, C>> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::NotCaptured => Poll::Ready(None),
            Self::Some(receiver) => Pin::new(receiver).poll_next(cx),
        }
    }
}

impl<T, C: Decode<T> + Default> TakeHandle<RemoteWorkflow> for Sender<T, C> {
    type Id = str;
    type Handle = Remote<Receiver<T, C>>;

    fn take_handle(env: &mut RemoteWorkflow, id: &str) -> Result<Self::Handle, AccessError> {
        let raw_receiver = env
            .inner
            .take_outbound_channel(id)
            .ok_or_else(|| AccessErrorKind::Unknown.with_location(OutboundChannel(id)))?;
        Ok(raw_receiver.map(|raw| raw.with_codec(C::default())))
    }
}

impl<T, C: Encode<T>> Sink<T> for Remote<Sender<T, C>> {
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.get_mut() {
            Self::NotCaptured => Poll::Ready(Err(SendError::Closed)),
            Self::Some(sender) => Pin::new(sender).poll_ready(cx),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        match self.get_mut() {
            Self::NotCaptured => Err(SendError::Closed),
            Self::Some(sender) => Pin::new(sender).start_send(item),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.get_mut() {
            Self::NotCaptured => Poll::Ready(Err(SendError::Closed)),
            Self::Some(sender) => Pin::new(sender).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.get_mut() {
            Self::NotCaptured => Poll::Ready(Ok(())),
            Self::Some(sender) => Pin::new(sender).poll_close(cx),
        }
    }
}

impl<C: Decode<FutureUpdate> + Default> TakeHandle<RemoteWorkflow> for Tracer<C> {
    type Id = str;
    type Handle = Remote<TracerHandle<C>>;

    fn take_handle(env: &mut RemoteWorkflow, id: &Self::Id) -> Result<Self::Handle, AccessError> {
        let receiver = Sender::<FutureUpdate, C>::take_handle(env, id)?;
        Ok(receiver.map(|receiver| TracerHandle {
            receiver: receiver.fuse(),
            futures: TracedFutures::default(),
        }))
    }
}

/// Handle for traced futures from a [`RemoteWorkflow`].
///
/// [`RemoteWorkflow`]: crate::spawn::RemoteWorkflow
#[derive(Debug)]
pub struct TracerHandle<C> {
    receiver: Fuse<Receiver<FutureUpdate, C>>,
    futures: TracedFutures,
}

impl<C> TracerHandle<C>
where
    C: Decode<FutureUpdate> + Default,
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
