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
//! #     workflow::{GetInterface, Handle, SpawnWorkflow, TakeHandle, Wasm, WorkflowEnv, WorkflowFn},
//! #     Json,
//! # };
//! // Assume we want to spawn a child workflow defined as follows:
//! #[derive(Debug, GetInterface, TakeHandle)]
//! #[tardigrade(handle = "ChildHandle", auto_interface)]
//! pub struct ChildWorkflow(());
//!
//! #[tardigrade::handle]
//! #[derive(Debug)]
//! pub struct ChildHandle<Env: WorkflowEnv> {
//!     pub commands: Handle<Receiver<String, Json>, Env>,
//!     pub events: Handle<Sender<String, Json>, Env>,
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
//! # runtime.workflow_registry_mut().insert::<ChildWorkflow, _>("child");
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
use futures::{Sink, Stream};
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

pub use crate::error::HostError;

use crate::{
    channel::{RawReceiver, RawSender, Receiver, SendError, Sender},
    interface::{AccessError, AccessErrorKind, ChannelHalf, Interface, ReceiverName, SenderName},
    task::JoinError,
    workflow::{
        DescribeEnv, GetInterface, Handle, TakeHandle, WithHandle, WorkflowEnv, WorkflowFn,
    },
    Decode, Encode,
};

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
    /// Configurations of channel receivers.
    pub receivers: HashMap<String, ChannelSpawnConfig<In>>,
    /// Configurations of channel senders.
    pub senders: HashMap<String, ChannelSpawnConfig<Out>>,
}

impl<In, Out> Default for ChannelsConfig<In, Out> {
    fn default() -> Self {
        Self {
            receivers: HashMap::new(),
            senders: HashMap::new(),
        }
    }
}

impl<In, Out> ChannelsConfig<In, Out> {
    /// Creates channel configuration from the provided interface.
    pub fn from_interface(interface: &Interface) -> Self {
        let receivers = interface
            .receivers()
            .map(|(name, _)| (name.to_owned(), ChannelSpawnConfig::default()))
            .collect();
        let senders = interface
            .senders()
            .map(|(name, _)| (name.to_owned(), ChannelSpawnConfig::default()))
            .collect();
        Self { receivers, senders }
    }

    fn close_channel(&mut self, kind: ChannelHalf, name: &str) {
        match kind {
            ChannelHalf::Receiver => {
                *self.receivers.get_mut(name).unwrap() = ChannelSpawnConfig::Closed;
            }
            ChannelHalf::Sender => {
                *self.senders.get_mut(name).unwrap() = ChannelSpawnConfig::Closed;
            }
        }
    }

    fn copy_sender(&mut self, name: &str, source: Out) {
        *self.senders.get_mut(name).unwrap() = ChannelSpawnConfig::Existing(source);
    }
}

/// Manager of [`Interface`]s that allows obtaining an interface by a string identifier.
pub trait ManageInterfaces {
    /// Returns the interface spec of a workflow with the specified ID.
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>>;
}

/// Specifier of channel handles when [spawning workflows](WorkflowBuilder).
///
/// Depending on the environment (e.g., workflow code vs host code), channel handles
/// can be specified in different ways. This trait encapsulates this variability.
pub trait SpecifyWorkflowChannels {
    /// Type of a channel receiver handle.
    type Receiver;
    /// Type of a channel sender handle.
    type Sender;
}

/// Manager of workflows that allows spawning workflows of a certain type.
///
/// This trait is low-level; use [`ManageWorkflowsExt`] for a high-level alternative.
#[async_trait]
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
    async fn create_workflow(
        &'a self,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<Self::Receiver, Self::Sender>,
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
    type Receiver = RawReceiver;
    type Sender = RawSender;
}

#[async_trait]
impl<'a, W> ManageWorkflows<'a, W> for Workflows
where
    W: WorkflowFn + TakeHandle<RemoteWorkflow, Id = ()>,
{
    type Handle = WorkflowHandle<W>;
    type Error = HostError;

    async fn create_workflow(
        &'a self,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<RawReceiver, RawSender>,
    ) -> Result<Self::Handle, Self::Error> {
        let mut workflow =
            <Self as ManageWorkflows<()>>::create_workflow(self, definition_id, args, channels)
                .await?;
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
    channels: RefCell<ChannelsConfig<Ch::Receiver, Ch::Sender>>,
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

    fn close_receiver(&self, channel_name: &str) {
        let mut borrow = self.channels.borrow_mut();
        borrow.close_channel(ChannelHalf::Receiver, channel_name);
    }

    fn close_sender(&self, channel_name: &str) {
        let mut borrow = self.channels.borrow_mut();
        borrow.close_channel(ChannelHalf::Sender, channel_name);
    }

    fn copy_sender(&self, channel_name: &str, sender: Ch::Sender) {
        let mut borrow = self.channels.borrow_mut();
        borrow.copy_sender(channel_name, sender);
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
    Ch::Receiver: fmt::Debug,
    Ch::Sender: fmt::Debug,
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

impl<Ch: SpecifyWorkflowChannels> WorkflowEnv for Spawner<Ch> {
    type Receiver<T, C: Encode<T> + Decode<T>> = ReceiverConfig<Ch, T, C>;
    type Sender<T, C: Encode<T> + Decode<T>> = SenderConfig<Ch, T, C>;

    fn take_receiver<T, C: Encode<T> + Decode<T>>(
        &mut self,
        id: &str,
    ) -> Result<Self::Receiver<T, C>, AccessError> {
        Ok(ReceiverConfig {
            spawner: self.clone(),
            channel_name: id.to_owned(),
            _ty: PhantomData,
        })
    }

    fn take_sender<T, C: Encode<T> + Decode<T>>(
        &mut self,
        id: &str,
    ) -> Result<Self::Sender<T, C>, AccessError> {
        Ok(SenderConfig {
            spawner: self.clone(),
            channel_name: id.to_owned(),
            _ty: PhantomData,
        })
    }
}

impl<Ch: SpecifyWorkflowChannels> DescribeEnv for Spawner<Ch> {
    fn interface(&self) -> Cow<'_, Interface> {
        Cow::Borrowed(&self.inner.interface)
    }
}

/// Configurator of a workflow channel [`Receiver`].
pub struct ReceiverConfig<Ch: SpecifyWorkflowChannels, T, C> {
    spawner: Spawner<Ch>,
    channel_name: String,
    _ty: PhantomData<fn(C) -> T>,
}

impl<Ch, T, C> fmt::Debug for ReceiverConfig<Ch, T, C>
where
    Ch: SpecifyWorkflowChannels + fmt::Debug,
    Ch::Receiver: fmt::Debug,
    Ch::Sender: fmt::Debug,
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
        self.spawner.inner.close_receiver(&self.channel_name);
    }
}

/// Configurator of a workflow channel [`Sender`].
pub struct SenderConfig<Ch: SpecifyWorkflowChannels, T, C> {
    spawner: Spawner<Ch>,
    channel_name: String,
    _ty: PhantomData<fn(C) -> T>,
}

impl<Ch, T, C> fmt::Debug for SenderConfig<Ch, T, C>
where
    Ch: SpecifyWorkflowChannels + fmt::Debug,
    Ch::Receiver: fmt::Debug,
    Ch::Sender: fmt::Debug,
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
        self.spawner.inner.close_sender(&self.channel_name);
    }
}

impl<T, C: Encode<T>> SenderConfig<Workflows, T, C> {
    /// Copies the channel from the provided `sender`. Thus, the created workflow will send
    /// messages over the same channel as `sender`.
    pub fn copy_from(&self, sender: Sender<T, C>) {
        self.spawner
            .inner
            .copy_sender(&self.channel_name, sender.into_raw());
    }
}

/// Builder allowing to configure workflow aspects, such as channels, before instantiation.
pub struct WorkflowBuilder<'a, M, W>
where
    M: SpecifyWorkflowChannels,
    W: WithHandle,
{
    spawner: Spawner<M>,
    handle: Handle<W, Spawner<M>>,
    manager: &'a M,
}

impl<M, W> fmt::Debug for WorkflowBuilder<'_, M, W>
where
    M: SpecifyWorkflowChannels,
    Spawner<M>: fmt::Debug,
    W: WithHandle,
    Handle<W, Spawner<M>>: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SpawnBuilder")
            .field("spawner", &self.spawner)
            .field("handle", &self.handle)
            .finish()
    }
}

impl<'a, M, W, H, E> WorkflowBuilder<'a, M, W>
where
    M: ManageWorkflows<'a, W, Handle = H, Error = E>,
    W: WorkflowFn + TakeHandle<Spawner<M>, Id = ()>,
{
    fn new(manager: &'a M, interface: Interface, definition_id: &str, args: W::Args) -> Self {
        let raw_args = W::Codec::encode_value(args);
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
    pub fn handle(&self) -> &Handle<W, Spawner<M>> {
        &self.handle
    }

    /// Instantiates the child workflow and returns a handle to it.
    ///
    /// # Errors
    ///
    /// Returns an error if instantiation fails for whatever reasons. Error handling is specific
    /// to the [manager](ManageWorkflows) being used.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub async fn build(self) -> Result<H, E> {
        drop(self.handle);
        let spawner = Rc::try_unwrap(self.spawner.inner).map_err(drop).unwrap();
        self.manager
            .create_workflow(
                &spawner.definition_id,
                spawner.args,
                spawner.channels.into_inner(),
            )
            .await
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
pub struct WorkflowHandle<W: WithHandle> {
    /// Channel handles associated with the workflow. Each channel receiver in the remote workflow
    /// is mapped to a local sender, and vice versa.
    pub api: Handle<W, RemoteWorkflow>,
    /// Workflow handle that can be polled for completion.
    pub workflow: RemoteWorkflow,
}

impl<W> fmt::Debug for WorkflowHandle<W>
where
    W: WithHandle,
    Handle<W, RemoteWorkflow>: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowHandle")
            .field("api", &self.api)
            .field("workflow", &self.workflow)
            .finish()
    }
}

impl WorkflowEnv for RemoteWorkflow {
    type Receiver<T, C: Encode<T> + Decode<T>> = Remote<Sender<T, C>>;
    type Sender<T, C: Encode<T> + Decode<T>> = Remote<Receiver<T, C>>;

    fn take_receiver<T, C: Encode<T> + Decode<T>>(
        &mut self,
        id: &str,
    ) -> Result<Self::Receiver<T, C>, AccessError> {
        let raw_sender = self
            .inner
            .take_receiver(id)
            .ok_or_else(|| AccessErrorKind::Unknown.with_location(ReceiverName(id)))?;
        Ok(raw_sender.map(Sender::from_raw))
    }

    fn take_sender<T, C: Encode<T> + Decode<T>>(
        &mut self,
        id: &str,
    ) -> Result<Self::Sender<T, C>, AccessError> {
        let raw_receiver = self
            .inner
            .take_sender(id)
            .ok_or_else(|| AccessErrorKind::Unknown.with_location(SenderName(id)))?;
        Ok(raw_receiver.map(Receiver::from_raw))
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
