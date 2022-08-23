//! Spawning and managing child workflows.

#![allow(missing_docs, clippy::missing_errors_doc)]

use pin_project_lite::pin_project;

use std::{
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
    workflow::{TakeHandle, WorkflowFn},
    Decode, Encode,
};
use tardigrade_shared::{JoinError, SpawnError};

#[cfg(not(target_arch = "wasm32"))]
#[path = "imp_mock.rs"]
mod imp;

#[derive(Debug)]
enum RemoteHandle<T> {
    None,
    NotCaptured,
    Some(T),
}

#[derive(Debug)]
pub struct WorkflowDefinition<W> {
    id: String,
    interface: Interface<W>,
}

impl WorkflowDefinition<()> {
    pub fn new(id: &str) -> Option<Self> {
        let interface_bytes = imp::workflow_interface(id)?;
        Some(Self {
            id: id.to_owned(),
            interface: Interface::from_bytes(&interface_bytes),
        })
    }

    pub fn downcast<W>(self) -> Result<WorkflowDefinition<W>, AccessError>
    where
        W: ValidateInterface<Id = ()>,
    {
        Ok(WorkflowDefinition {
            id: self.id,
            interface: self.interface.downcast()?,
        })
    }
}

impl<W> WorkflowDefinition<W> {
    pub fn interface(&self) -> &Interface<W> {
        &self.interface
    }
}

impl<W> WorkflowDefinition<W>
where
    W: TakeHandle<Spawner, Id = ()> + WorkflowFn,
{
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn spawn(&self, args: W::Args) -> SpawnBuilder<W> {
        let raw_args = W::Codec::default().encode_value(args);
        let mut spawner = Spawner::new(&self.id, raw_args);
        let handle = W::take_handle(&mut spawner, &()).unwrap();
        SpawnBuilder { spawner, handle }
    }
}

/// Spawn environment.
#[derive(Debug, Clone)]
pub struct Spawner {
    inner: Rc<imp::Spawner>,
}

impl Spawner {
    fn new(id: &str, args: Vec<u8>) -> Self {
        Self {
            inner: Rc::new(imp::Spawner::new(id, args)),
        }
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

pub struct SpawnBuilder<W: TakeHandle<Spawner>> {
    spawner: Spawner,
    handle: <W as TakeHandle<Spawner>>::Handle,
}

impl<W> fmt::Debug for SpawnBuilder<W>
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

impl<W> SpawnBuilder<W>
where
    W: TakeHandle<Spawner> + TakeHandle<RemoteWorkflow, Id = ()>,
{
    pub fn handle(&self) -> &<W as TakeHandle<Spawner>>::Handle {
        &self.handle
    }

    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn build(self) -> Result<WorkflowHandle<W>, SpawnError> {
        drop(self.handle);

        let mut workflow = RemoteWorkflow {
            inner: Rc::try_unwrap(self.spawner.inner).unwrap().spawn()?,
        };
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
