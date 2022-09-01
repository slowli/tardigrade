//! Spawning and managing child workflows.

#![allow(missing_docs, clippy::missing_errors_doc)]

use futures::{
    stream::{Fuse, FusedStream},
    FutureExt, StreamExt,
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

use crate::{
    channel::{Receiver, Sender},
    interface::{
        AccessError, AccessErrorKind, InboundChannel, Interface, OutboundChannel, ValidateInterface,
    },
    trace::{FutureUpdate, TracedFutures, Tracer},
    workflow::{TakeHandle, UntypedHandle, WorkflowFn},
    Decode, Encode,
};
pub use tardigrade_shared::SpawnError;
use tardigrade_shared::{
    abi::{FromWasmError, TryFromWasm},
    JoinError,
};

#[cfg(target_arch = "wasm32")]
#[path = "imp_wasm32.rs"]
pub(crate) mod imp;
#[cfg(not(target_arch = "wasm32"))]
#[path = "imp_mock.rs"]
mod imp;

/// Configuration of a workflow channel.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ChannelSpawnConfig {
    /// Create a new channel.
    New,
    /// Close the channel on workflow creation.
    Closed,
}

impl Default for ChannelSpawnConfig {
    fn default() -> Self {
        Self::New
    }
}

impl TryFromWasm for ChannelSpawnConfig {
    type Abi = i32;

    fn into_abi_in_wasm(self) -> Self::Abi {
        match self {
            Self::New => 0,
            Self::Closed => 1,
        }
    }

    fn try_from_wasm(abi: Self::Abi) -> Result<Self, FromWasmError> {
        match abi {
            0 => Ok(Self::New),
            1 => Ok(Self::Closed),
            _ => Err(FromWasmError::new("Invalid `ChannelSpawnConfig` value")),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChannelHandles {
    pub inbound: HashMap<String, ChannelSpawnConfig>,
    pub outbound: HashMap<String, ChannelSpawnConfig>,
}

/// Manager of workflows.
pub trait ManageWorkflows: 'static {
    /// Instantiated workflow.
    type Handle;

    /// Returns the interface spec of a workflow with the specified ID.
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface<()>>>;

    /// Creates a new workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be spawned, e.g., if the provided `args`
    /// are incorrect.
    fn create_workflow(
        &self,
        definition_id: &str,
        args: Vec<u8>,
        handles: &ChannelHandles,
    ) -> Result<Self::Handle, SpawnError>;
}

pub trait ManageWorkflowsExt: ManageWorkflows {
    fn definition<'a>(
        &'a self,
        definition_id: &'a str,
    ) -> Option<WorkflowDefinition<'a, Self, ()>> {
        self.interface(definition_id)
            .map(|interface| WorkflowDefinition {
                definition_id,
                interface: interface.into_owned(),
                manager: self,
            })
    }
}

impl<M: ManageWorkflows> ManageWorkflowsExt for M {}

/// Client-side connection to a [workflow manager][`ManageWorkflows`].
#[derive(Debug)]
pub struct Workflows;

#[derive(Debug)]
pub(crate) enum RemoteHandle<T> {
    None,
    NotCaptured,
    Some(T),
}

#[derive(Debug)]
pub struct WorkflowDefinition<'a, M: ?Sized, W> {
    definition_id: &'a str,
    interface: Interface<W>,
    manager: &'a M,
}

impl<'a, M: ManageWorkflows + ?Sized> WorkflowDefinition<'a, M, ()> {
    pub fn downcast<W>(self) -> Result<WorkflowDefinition<'a, M, W>, AccessError>
    where
        W: ValidateInterface<Id = ()>,
    {
        Ok(WorkflowDefinition {
            definition_id: self.definition_id,
            interface: self.interface.downcast()?,
            manager: self.manager,
        })
    }
}

impl<M: ManageWorkflows + ?Sized, W> WorkflowDefinition<'_, M, W> {
    pub fn interface(&self) -> &Interface<W> {
        &self.interface
    }
}

impl<'a, M, W> WorkflowDefinition<'a, M, W>
where
    M: ManageWorkflows + ?Sized,
    W: TakeHandle<Spawner, Id = ()> + WorkflowFn,
{
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn new_workflow(&self, args: W::Args) -> WorkflowBuilder<'a, M, W> {
        let raw_args = W::Codec::default().encode_value(args);
        let mut spawner = Spawner {
            inner: Rc::new(SpawnerInner::new(
                &self.interface,
                self.definition_id,
                raw_args,
            )),
        };
        let handle = W::take_handle(&mut spawner, &()).unwrap();
        WorkflowBuilder {
            spawner,
            handle,
            manager: self.manager,
        }
    }
}

#[derive(Debug)]
struct SpawnerInner {
    definition_id: String,
    interface: Interface<()>,
    args: Vec<u8>,
    channels: RefCell<ChannelHandles>,
}

impl SpawnerInner {
    fn new<W>(interface: &Interface<W>, definition_id: &str, args: Vec<u8>) -> Self {
        let inbound = interface
            .inbound_channels()
            .map(|(name, _)| (name.to_owned(), ChannelSpawnConfig::default()))
            .collect();
        let outbound = interface
            .outbound_channels()
            .map(|(name, _)| (name.to_owned(), ChannelSpawnConfig::default()))
            .collect();
        Self {
            definition_id: definition_id.to_owned(),
            interface: interface.clone().erase(),
            args,
            channels: RefCell::new(ChannelHandles { inbound, outbound }),
        }
    }

    fn close_inbound_channel(&self, channel_name: &str) {
        let mut borrow = self.channels.borrow_mut();
        let channel = borrow.inbound.get_mut(channel_name).unwrap_or_else(|| {
            panic!(
                "attempted closing non-existing inbound channel `{}`",
                channel_name
            );
        });
        *channel = ChannelSpawnConfig::Closed;
    }

    fn close_outbound_channel(&self, channel_name: &str) {
        let mut borrow = self.channels.borrow_mut();
        let channel = borrow.outbound.get_mut(channel_name).unwrap_or_else(|| {
            panic!(
                "attempted closing non-existing outbound channel `{}`",
                channel_name
            );
        });
        *channel = ChannelSpawnConfig::Closed;
    }
}

/// Spawn environment.
#[derive(Debug, Clone)]
pub struct Spawner {
    inner: Rc<SpawnerInner>,
}

impl TakeHandle<Spawner> for Interface<()> {
    type Id = ();
    type Handle = Self;

    fn take_handle(env: &mut Spawner, _id: &Self::Id) -> Result<Self::Handle, AccessError> {
        Ok(env.inner.interface.clone())
    }
}

impl TakeHandle<Spawner> for () {
    type Id = ();
    type Handle = UntypedHandle<Spawner>;

    fn take_handle(env: &mut Spawner, _id: &Self::Id) -> Result<Self::Handle, AccessError> {
        UntypedHandle::take_handle(env, &())
    }
}

#[derive(Debug)]
pub struct ReceiverConfig<T, C> {
    spawner: Spawner,
    channel_name: String,
    _ty: PhantomData<fn(C) -> T>,
}

impl<T, C: Encode<T>> ReceiverConfig<T, C> {
    pub fn close(&self) {
        self.spawner.inner.close_inbound_channel(&self.channel_name);
    }
}

impl<T, C: Encode<T>> TakeHandle<Spawner> for Receiver<T, C> {
    type Id = str;
    type Handle = ReceiverConfig<T, C>;

    fn take_handle(env: &mut Spawner, id: &Self::Id) -> Result<Self::Handle, AccessError> {
        Ok(ReceiverConfig {
            spawner: env.clone(),
            channel_name: id.to_owned(),
            _ty: PhantomData,
        })
    }
}

#[derive(Debug)]
pub struct SenderConfig<T, C> {
    spawner: Spawner,
    channel_name: String,
    _ty: PhantomData<fn(C) -> T>,
}

impl<T, C: Encode<T>> SenderConfig<T, C> {
    pub fn close(&self) {
        self.spawner
            .inner
            .close_outbound_channel(&self.channel_name);
    }
}

impl<T, C: Encode<T>> TakeHandle<Spawner> for Sender<T, C> {
    type Id = str;
    type Handle = SenderConfig<T, C>;

    fn take_handle(env: &mut Spawner, id: &Self::Id) -> Result<Self::Handle, AccessError> {
        Ok(SenderConfig {
            spawner: env.clone(),
            channel_name: id.to_owned(),
            _ty: PhantomData,
        })
    }
}

impl<C: Encode<FutureUpdate>> TakeHandle<Spawner> for Tracer<C> {
    type Id = str;
    type Handle = SenderConfig<FutureUpdate, C>;

    fn take_handle(env: &mut Spawner, id: &Self::Id) -> Result<Self::Handle, AccessError> {
        Ok(SenderConfig {
            spawner: env.clone(),
            channel_name: id.to_owned(),
            _ty: PhantomData,
        })
    }
}

pub struct WorkflowBuilder<'a, M: ?Sized, W: TakeHandle<Spawner>> {
    spawner: Spawner,
    handle: <W as TakeHandle<Spawner>>::Handle,
    manager: &'a M,
}

impl<M: ?Sized, W> fmt::Debug for WorkflowBuilder<'_, M, W>
where
    W: TakeHandle<Spawner>,
    <W as TakeHandle<Spawner>>::Handle: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SpawnBuilder")
            .field("spawner", &self.spawner)
            .field("handle", &self.handle)
            .finish()
    }
}

impl<M, W> WorkflowBuilder<'_, M, W>
where
    M: ManageWorkflows + ?Sized,
    W: TakeHandle<Spawner>,
{
    pub fn handle(&self) -> &<W as TakeHandle<Spawner>>::Handle {
        &self.handle
    }

    #[doc(hidden)] // used by the runtime crate
    pub fn build_into_handle(self) -> Result<M::Handle, SpawnError> {
        drop(self.handle);
        let spawner = Rc::try_unwrap(self.spawner.inner).unwrap();
        self.manager.create_workflow(
            &spawner.definition_id,
            spawner.args,
            &spawner.channels.into_inner(),
        )
    }
}

impl<M, W> WorkflowBuilder<'_, M, W>
where
    M: ManageWorkflows<Handle = RemoteWorkflow> + ?Sized,
    W: TakeHandle<Spawner> + TakeHandle<RemoteWorkflow, Id = ()>,
{
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn build(self) -> Result<WorkflowHandle<W>, SpawnError> {
        let mut workflow = self.build_into_handle()?;
        let api = W::take_handle(&mut workflow, &()).unwrap();
        Ok(WorkflowHandle { api, workflow })
    }
}

pin_project! {
    #[derive(Debug)]
    #[repr(transparent)]
    pub struct RemoteWorkflow {
        #[pin]
        inner: imp::RemoteWorkflow,
    }
}

impl Future for RemoteWorkflow {
    type Output = Result<(), JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/// Handle to a remote workflow.
#[non_exhaustive]
pub struct WorkflowHandle<W: TakeHandle<RemoteWorkflow>> {
    pub api: <W as TakeHandle<RemoteWorkflow>>::Handle,
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
    type Handle = Option<Sender<T, C>>;

    fn take_handle(env: &mut RemoteWorkflow, id: &str) -> Result<Self::Handle, AccessError> {
        let raw_sender = env.inner.take_inbound_channel(id);
        match raw_sender {
            RemoteHandle::None => Err(AccessErrorKind::Unknown.with_location(InboundChannel(id))),
            RemoteHandle::NotCaptured => Ok(None),
            RemoteHandle::Some(raw) => Ok(Some(raw.with_codec(C::default()))),
        }
    }
}

impl<T, C: Decode<T> + Default> TakeHandle<RemoteWorkflow> for Sender<T, C> {
    type Id = str;
    type Handle = Option<Receiver<T, C>>;

    fn take_handle(env: &mut RemoteWorkflow, id: &str) -> Result<Self::Handle, AccessError> {
        let raw_receiver = env.inner.take_outbound_channel(id);
        match raw_receiver {
            RemoteHandle::None => Err(AccessErrorKind::Unknown.with_location(OutboundChannel(id))),
            RemoteHandle::NotCaptured => Ok(None),
            RemoteHandle::Some(raw) => Ok(Some(raw.with_codec(C::default()))),
        }
    }
}

impl<C: Decode<FutureUpdate> + Default> TakeHandle<RemoteWorkflow> for Tracer<C> {
    type Id = str;
    type Handle = Option<TracerHandle<C>>;

    fn take_handle(env: &mut RemoteWorkflow, id: &Self::Id) -> Result<Self::Handle, AccessError> {
        let receiver = Sender::<FutureUpdate, C>::take_handle(env, id)?;
        Ok(receiver.map(|receiver| TracerHandle {
            receiver: receiver.fuse(),
            futures: TracedFutures::default(),
        }))
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
