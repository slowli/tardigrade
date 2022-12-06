//! Mock implementations for spawning child workflows.

use async_trait::async_trait;
use pin_project_lite::pin_project;

use std::{
    borrow::Cow,
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
};
use tardigrade_shared::interface::{HandleMapKey, Interface};

use super::{
    ChannelSpawnConfig, ChannelsConfig, ManageInterfaces, ManageWorkflows, Remote, Workflows,
};
use crate::{
    channel::{imp::raw_channel, RawReceiver, RawSender},
    error::HostError,
    interface::{AccessError, ChannelHalf, HandleMap, HandlePath, ReceiverAt, SenderAt},
    task::{JoinError, JoinHandle},
    test::Runtime,
    workflow::{UntypedHandle, Wasm},
    ChannelId,
};

impl ManageInterfaces for Workflows {
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        Runtime::with(|rt| {
            rt.workflow_registry()
                .interface(definition_id)
                .map(|interface| Cow::Owned(interface.clone()))
        })
    }
}

#[async_trait]
impl ManageWorkflows<()> for Workflows {
    type Handle<'s> = super::RemoteWorkflow;
    type Error = HostError;

    async fn create_workflow(
        &self,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<RawReceiver, RawSender>,
    ) -> Result<Self::Handle<'_>, Self::Error> {
        let (local_handles, remote_handles) = create_handles(channels);
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
    fn new(channel_id: ChannelId) -> Self {
        let (sx, rx) = raw_channel(channel_id);
        Self {
            sx: Some(RawSender::new(sx)),
            rx: Some(RawReceiver::new(rx)),
        }
    }

    fn closed(local_half: ChannelHalf) -> Self {
        let mut pair = Self::new(0);
        match local_half {
            ChannelHalf::Receiver => pair.sx = None,
            ChannelHalf::Sender => pair.rx = None,
        }
        pair
    }

    fn from_config<T: Into<ChannelPair>>(
        config: ChannelSpawnConfig<T>,
        local_half: ChannelHalf,
    ) -> Self {
        match config {
            ChannelSpawnConfig::New => {
                let channel_id = Runtime::with_mut(Runtime::allocate_channel_id);
                Self::new(channel_id)
            }
            ChannelSpawnConfig::Closed => Self::closed(local_half),
            ChannelSpawnConfig::Existing(handle) => handle.into(),
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

fn create_handles(
    config: ChannelsConfig<RawReceiver, RawSender>,
) -> (UntypedHandle<super::RemoteWorkflow>, UntypedHandle<Wasm>) {
    let mut channel_pairs = map_config(config);
    let handles = channel_pairs
        .iter_mut()
        .map(|(path, channel)| {
            let handle = channel
                .as_mut()
                .map_receiver(|channel| channel.rx.take().unwrap())
                .map_sender(|channel| channel.sx.take().unwrap());
            (path.clone(), handle)
        })
        .collect();
    let remote = UntypedHandle { handles };

    let handles = channel_pairs
        .into_iter()
        .map(|(path, channel)| {
            let handle = channel
                .map_receiver(|channel| Remote::from_option(channel.sx))
                .map_sender(|channel| Remote::from_option(channel.rx));
            (path, handle)
        })
        .collect();
    let local = UntypedHandle { handles };
    (local, remote)
}

fn map_config(config: ChannelsConfig<RawReceiver, RawSender>) -> HandleMap<ChannelPair> {
    config
        .into_iter()
        .map(|(path, config)| {
            let pair = config
                .map_sender(|sx| ChannelPair::from_config(sx, ChannelHalf::Sender))
                .map_receiver(|rx| ChannelPair::from_config(rx, ChannelHalf::Receiver));
            (path, pair)
        })
        .collect()
}

pin_project! {
    #[derive(Debug)]
    pub(super) struct RemoteWorkflow {
        #[pin]
        main_task: JoinHandle,
        handle: UntypedHandle<super::RemoteWorkflow>,
    }
}

impl RemoteWorkflow {
    pub fn take_receiver(
        &mut self,
        path: HandlePath<'_>,
    ) -> Result<Remote<RawSender>, AccessError> {
        let handle = ReceiverAt(path).get_mut(&mut self.handle.handles)?;
        Ok(mem::replace(handle, Remote::NotCaptured))
    }

    pub fn take_sender(
        &mut self,
        path: HandlePath<'_>,
    ) -> Result<Remote<RawReceiver>, AccessError> {
        let handle = SenderAt(path).get_mut(&mut self.handle.handles)?;
        Ok(mem::replace(handle, Remote::NotCaptured))
    }
}

impl super::RemoteWorkflow {
    fn from_parts(main_task: JoinHandle, handle: UntypedHandle<super::RemoteWorkflow>) -> Self {
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
