//! Mock implementations for spawning child workflows.

use pin_project_lite::pin_project;

use std::{
    cell::RefCell,
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::RemoteHandle;
use crate::{
    channel::{imp::raw_channel, RawReceiver, RawSender},
    interface::Interface,
    test::Runtime,
    workflow::{UntypedHandle, Wasm},
    JoinHandle, Raw,
};
use tardigrade_shared::{JoinError, SpawnError};

pub(super) fn workflow_interface(id: &str) -> Option<Vec<u8>> {
    Runtime::with(|rt| {
        rt.workflow_registry()
            .interface(id)
            .map(Interface::to_bytes)
    })
}

#[derive(Debug)]
struct ChannelPair {
    sx: Option<RawSender>,
    rx: Option<RawReceiver>,
}

impl Default for ChannelPair {
    fn default() -> Self {
        let (sx, rx) = raw_channel();
        Self {
            sx: Some(RawSender::new(sx, Raw)),
            rx: Some(RawReceiver::new(rx, Raw)),
        }
    }
}

#[derive(Debug)]
struct SpawnerInner {
    inbound_channels: HashMap<String, ChannelPair>,
    outbound_channels: HashMap<String, ChannelPair>,
}

impl SpawnerInner {
    fn split(mut self) -> (UntypedHandle<super::RemoteWorkflow>, UntypedHandle<Wasm>) {
        let inbound_channels = self
            .inbound_channels
            .iter_mut()
            .map(|(name, channel)| (name.clone(), channel.rx.take().unwrap()))
            .collect();
        let outbound_channels = self
            .outbound_channels
            .iter_mut()
            .map(|(name, channel)| (name.clone(), channel.sx.take().unwrap()))
            .collect();
        let remote = UntypedHandle {
            inbound_channels,
            outbound_channels,
        };

        let inbound_channels = self
            .inbound_channels
            .into_iter()
            .map(|(name, channel)| (name, channel.sx))
            .collect();
        let outbound_channels = self
            .outbound_channels
            .into_iter()
            .map(|(name, channel)| (name, channel.rx))
            .collect();
        let local = UntypedHandle {
            inbound_channels,
            outbound_channels,
        };
        (local, remote)
    }
}

#[derive(Debug)]
pub(super) struct Spawner {
    inner: RefCell<SpawnerInner>,
    id: String,
    args: Vec<u8>,
}

impl Spawner {
    pub fn new(id: &str, args: Vec<u8>) -> Self {
        let interface = workflow_interface(id).unwrap_or_else(|| {
            panic!("workflow `{}` is not registered", id);
        });
        let interface = Interface::from_bytes(&interface);

        let inbound_channels = interface
            .inbound_channels()
            .map(|(name, _)| (name.to_owned(), ChannelPair::default()))
            .collect();
        let outbound_channels = interface
            .outbound_channels()
            .map(|(name, _)| (name.to_owned(), ChannelPair::default()))
            .collect();
        Self {
            inner: RefCell::new(SpawnerInner {
                inbound_channels,
                outbound_channels,
            }),
            id: id.to_owned(),
            args,
        }
    }

    pub fn close_inbound_channel(&self, channel_name: &str) {
        let mut borrow = self.inner.borrow_mut();
        let channel = borrow
            .inbound_channels
            .get_mut(channel_name)
            .unwrap_or_else(|| {
                panic!(
                    "attempted closing non-existing inbound channel `{}`",
                    channel_name
                );
            });
        channel.sx = None;
    }

    pub fn close_outbound_channel(&self, channel_name: &str) {
        let mut borrow = self.inner.borrow_mut();
        let channel = borrow
            .outbound_channels
            .get_mut(channel_name)
            .unwrap_or_else(|| {
                panic!(
                    "attempted closing non-existing outbound channel `{}`",
                    channel_name
                );
            });
        channel.rx = None;
    }

    #[allow(clippy::unnecessary_wraps)] // for uniformity with WASM bindings
    pub fn spawn(self) -> Result<RemoteWorkflow, SpawnError> {
        let (local, remote) = self.inner.into_inner().split();
        let main_task =
            Runtime::with(|rt| rt.workflow_registry().spawn(&self.id, self.args, remote));

        Ok(RemoteWorkflow {
            handle: local,
            main_task: crate::spawn("_workflow", main_task.into_inner()),
        })
    }
}

pin_project! {
    #[derive(Debug)]
    pub(super) struct RemoteWorkflow {
        handle: UntypedHandle<super::RemoteWorkflow>,
        #[pin]
        main_task: JoinHandle<()>,
    }
}

impl RemoteWorkflow {
    pub fn take_inbound_channel(&mut self, channel_name: &str) -> RemoteHandle<RawSender> {
        match self.handle.inbound_channels.get_mut(channel_name) {
            Some(channel) => match channel.take() {
                Some(channel) => RemoteHandle::Some(channel),
                None => RemoteHandle::NotCaptured,
            },
            None => RemoteHandle::None,
        }
    }

    pub fn take_outbound_channel(&mut self, channel_name: &str) -> RemoteHandle<RawReceiver> {
        match self.handle.outbound_channels.get_mut(channel_name) {
            Some(channel) => match channel.take() {
                Some(channel) => RemoteHandle::Some(channel),
                None => RemoteHandle::NotCaptured,
            },
            None => RemoteHandle::None,
        }
    }
}

impl Future for RemoteWorkflow {
    type Output = Result<(), JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().main_task.poll(cx)
    }
}
