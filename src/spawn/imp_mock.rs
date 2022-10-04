//! Mock implementations for spawning child workflows.

use pin_project_lite::pin_project;

use std::{
    borrow::Cow,
    collections::HashMap,
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
};
use tardigrade_shared::interface::Interface;

use super::{
    ChannelSpawnConfig, ChannelsConfig, ManageInterfaces, ManageWorkflows, Remote, Workflows,
};
use crate::{
    channel::{imp::raw_channel, RawReceiver, RawSender},
    interface::ChannelKind,
    test::Runtime,
    workflow::{UntypedHandle, Wasm},
    JoinHandle, Raw,
};
use tardigrade_shared::{JoinError, SpawnError};

impl ManageInterfaces for Workflows {
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        Runtime::with(|rt| {
            rt.workflow_registry()
                .interface(definition_id)
                .map(|interface| Cow::Owned(interface.clone()))
        })
    }
}

impl ManageWorkflows<'_, ()> for Workflows {
    type Handle = super::RemoteWorkflow;
    type Error = SpawnError;

    fn create_workflow(
        &self,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<RawReceiver, RawSender>,
    ) -> Result<Self::Handle, Self::Error> {
        let (local_handles, remote_handles) = channels.create_handles();
        let main_task =
            Runtime::with_mut(|rt| rt.create_workflow(definition_id, args, remote_handles));
        let main_task = JoinHandle::from_handle(main_task);
        Ok(super::RemoteWorkflow::from_parts(main_task, local_handles))
    }
}

impl<T> Remote<T> {
    fn from_option(option: Option<T>) -> Self {
        match option {
            None => Self::NotCaptured,
            Some(value) => Self::Some(value),
        }
    }
}

#[derive(Debug)]
struct ChannelPair {
    sx: Option<RawSender>,
    rx: Option<RawReceiver>,
}

impl ChannelPair {
    fn closed(local_channel_kind: ChannelKind) -> Self {
        let mut pair = Self::default();
        match local_channel_kind {
            ChannelKind::Inbound => pair.sx = None,
            ChannelKind::Outbound => pair.rx = None,
        }
        pair
    }
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

impl From<RawSender> for ChannelPair {
    fn from(sx: RawSender) -> Self {
        Self {
            sx: Some(sx),
            rx: None,
        }
    }
}

impl From<RawReceiver> for ChannelPair {
    fn from(rx: RawReceiver) -> Self {
        Self {
            sx: None,
            rx: Some(rx),
        }
    }
}

impl ChannelsConfig<RawReceiver, RawSender> {
    fn create_handles(self) -> (UntypedHandle<super::RemoteWorkflow>, UntypedHandle<Wasm>) {
        let mut inbound_channel_pairs: HashMap<_, _> =
            Self::map_config(self.inbound, ChannelKind::Inbound);
        let mut outbound_channel_pairs: HashMap<_, _> =
            Self::map_config(self.outbound, ChannelKind::Outbound);

        let inbound_channels = inbound_channel_pairs
            .iter_mut()
            .map(|(name, channel)| (name.clone(), channel.rx.take().unwrap()))
            .collect();
        let outbound_channels = outbound_channel_pairs
            .iter_mut()
            .map(|(name, channel)| (name.clone(), channel.sx.take().unwrap()))
            .collect();
        let remote = UntypedHandle {
            inbound_channels,
            outbound_channels,
        };

        let inbound_channels = inbound_channel_pairs
            .into_iter()
            .map(|(name, channel)| (name, Remote::from_option(channel.sx)))
            .collect();
        let outbound_channels = outbound_channel_pairs
            .into_iter()
            .map(|(name, channel)| (name, Remote::from_option(channel.rx)))
            .collect();
        let local = UntypedHandle {
            inbound_channels,
            outbound_channels,
        };
        (local, remote)
    }

    fn map_config<T: Into<ChannelPair>>(
        config: HashMap<String, ChannelSpawnConfig<T>>,
        local_channel_kind: ChannelKind,
    ) -> HashMap<String, ChannelPair> {
        config
            .into_iter()
            .map(|(name, config)| {
                let pair = match config {
                    ChannelSpawnConfig::New => ChannelPair::default(),
                    ChannelSpawnConfig::Closed => ChannelPair::closed(local_channel_kind),
                    ChannelSpawnConfig::Copy(handle) => handle.into(),
                };
                (name, pair)
            })
            .collect()
    }
}

pin_project! {
    #[derive(Debug)]
    pub(super) struct RemoteWorkflow {
        #[pin]
        main_task: JoinHandle<()>,
        handle: UntypedHandle<super::RemoteWorkflow>,
    }
}

impl RemoteWorkflow {
    pub fn take_inbound_channel(&mut self, channel_name: &str) -> Option<Remote<RawSender>> {
        self.handle
            .inbound_channels
            .get_mut(channel_name)
            .map(|channel| mem::replace(channel, Remote::NotCaptured))
    }

    pub fn take_outbound_channel(&mut self, channel_name: &str) -> Option<Remote<RawReceiver>> {
        self.handle
            .outbound_channels
            .get_mut(channel_name)
            .map(|channel| mem::replace(channel, Remote::NotCaptured))
    }
}

impl super::RemoteWorkflow {
    fn from_parts(main_task: JoinHandle<()>, handle: UntypedHandle<super::RemoteWorkflow>) -> Self {
        Self {
            inner: RemoteWorkflow { main_task, handle },
        }
    }
}

impl Future for RemoteWorkflow {
    type Output = Result<(), JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().main_task.poll(cx)
    }
}
