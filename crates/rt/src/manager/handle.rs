//! Handles for workflows in a [`WorkflowManager`] and their components (e.g., channels).

use anyhow::Context;

use std::{fmt, marker::PhantomData, ops::Range};

use crate::{
    manager::{ChannelInfo, WorkflowManager},
    module::WorkflowAndChannelIds,
    utils::Message,
    PersistedWorkflow,
};
use tardigrade::{
    channel::{Receiver, SendError, Sender},
    interface::{AccessError, AccessErrorKind, InboundChannel, Interface, OutboundChannel},
    trace::{FutureUpdate, TracedFutures, Tracer},
    workflow::{GetInterface, TakeHandle, UntypedHandle},
    ChannelId, Decode, Encode, WorkflowId,
};

/// Handle to a workflow in a [`WorkflowManager`].
///
/// This type is used as a type param for the [`TakeHandle`] trait. The returned handles
/// allow interacting with the workflow (e.g., [send messages](MessageSender) via inbound channels
/// and [take messages](MessageReceiver) from outbound channels).
///
/// See [`AsyncEnv`] for a more high-level, future-based alternative.
///
/// [`AsyncEnv`]: crate::manager::future::AsyncEnv
///
/// # Examples
///
/// ```
/// use tardigrade::interface::{InboundChannel, OutboundChannel};
/// use tardigrade_rt::manager::WorkflowHandle;
///
/// # fn test_wrapper(workflow: WorkflowHandle<'_, ()>) -> anyhow::Result<()> {
/// // Assume we have a dynamically typed workflow:
/// let mut workflow: WorkflowHandle<()> = // ...
/// #   workflow;
/// // We can create a handle to manipulate the workflow.
/// let mut handle = workflow.handle();
///
/// // Let's send a message via an inbound channel.
/// let message = b"hello".to_vec();
/// handle[InboundChannel("commands")].send(message)?;
///
/// // Let's then take outbound messages from a certain channel:
/// let messages = handle[OutboundChannel("events")]
///     .take_messages()
///     .unwrap();
/// let messages: Vec<Vec<u8>> = messages.decode().unwrap();
/// // ^ `decode().unwrap()` always succeeds because the codec
/// // for untyped workflows is just an identity.
///
/// // It is possible to access the underlying workflow state:
/// let persisted = workflow.persisted();
/// println!("{:?}", persisted.tasks().collect::<Vec<_>>());
/// let now = persisted.current_time();
/// # Ok(())
/// # }
/// ```
pub struct WorkflowHandle<'a, W> {
    manager: &'a WorkflowManager,
    ids: WorkflowAndChannelIds,
    _ty: PhantomData<fn(W)>,
}

impl<W> fmt::Debug for WorkflowHandle<'_, W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowEnv")
            .field("manager", &self.manager)
            .field("ids", &self.ids)
            .finish()
    }
}

impl<'a> WorkflowHandle<'a, ()> {
    pub(crate) fn new(manager: &'a WorkflowManager, ids: WorkflowAndChannelIds) -> Self {
        Self {
            manager,
            ids,
            _ty: PhantomData,
        }
    }

    #[cfg(test)]
    pub(crate) fn ids(&self) -> &WorkflowAndChannelIds {
        &self.ids
    }

    /// Attempts to downcast this handle to a specific workflow interface.
    ///
    /// # Errors
    ///
    /// Returns an error on workflow interface mismatch.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn downcast<W: GetInterface>(self) -> Result<WorkflowHandle<'a, W>, AccessError> {
        let interface = self
            .manager
            .interface_for_workflow(self.ids.workflow_id)
            .unwrap();
        W::interface().check_compatibility(interface)?;
        Ok(self.downcast_unchecked())
    }

    pub(crate) fn downcast_unchecked<W>(self) -> WorkflowHandle<'a, W> {
        WorkflowHandle {
            manager: self.manager,
            ids: self.ids,
            _ty: PhantomData,
        }
    }
}

impl<W: TakeHandle<Self, Id = ()>> WorkflowHandle<'_, W> {
    /// Returns the ID of this workflow.
    pub fn id(&self) -> WorkflowId {
        self.ids.workflow_id
    }

    /// Returns the current persisted state of the workflow.
    pub fn persisted(&self) -> PersistedWorkflow {
        self.manager.persisted_workflow(self.ids.workflow_id)
    }

    /// Returns a handle for the workflow that allows interacting with its channels.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn handle(&mut self) -> <W as TakeHandle<Self>>::Handle {
        W::take_handle(self, &()).unwrap()
    }
}

/// Handle for an [inbound workflow channel](Receiver) that allows sending messages
/// via the channel.
#[derive(Debug)]
pub struct MessageSender<'a, T, C> {
    manager: &'a WorkflowManager,
    channel_id: ChannelId,
    pub(super) codec: C,
    _item: PhantomData<fn(T)>,
}

impl<'a, T, C: Encode<T>> MessageSender<'a, T, C> {
    /// Returns the ID of the channel this sender is connected to.
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    /// Returns the current state of the channel.
    #[allow(clippy::missing_panics_doc)] // false positive: channels are never removed
    pub fn channel_info(&self) -> ChannelInfo {
        self.manager.channel(self.channel_id).unwrap()
    }

    /// Sends a message over the channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel is full or closed.
    pub fn send(&mut self, message: T) -> Result<(), SendError> {
        let raw_message = self.codec.encode_value(message);
        self.manager.send_message(self.channel_id, raw_message)
    }

    /// Closes this channel from the host side.
    pub fn close(self) {
        self.manager.close_host_sender(self.channel_id);
    }
}

impl<'a, T, C, W> TakeHandle<WorkflowHandle<'a, W>> for Receiver<T, C>
where
    C: Encode<T> + Default,
{
    type Id = str;
    type Handle = MessageSender<'a, T, C>;

    fn take_handle(env: &mut WorkflowHandle<'a, W>, id: &str) -> Result<Self::Handle, AccessError> {
        if let Some(channel_id) = env.ids.channel_ids.inbound.get(id).copied() {
            Ok(MessageSender {
                manager: env.manager,
                channel_id,
                codec: C::default(),
                _item: PhantomData,
            })
        } else {
            Err(AccessErrorKind::Unknown.with_location(InboundChannel(id)))
        }
    }
}

/// Handle for an [outbound workflow channel](Sender) that allows taking messages
/// from the channel.
#[derive(Debug)]
pub struct MessageReceiver<'a, T, C> {
    manager: &'a WorkflowManager,
    channel_id: ChannelId,
    pub(super) can_receive_messages: bool,
    pub(super) codec: C,
    _item: PhantomData<fn() -> T>,
}

impl<T, C: Decode<T>> MessageReceiver<'_, T, C> {
    /// Returns the ID of the channel this receiver is connected to.
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    /// Returns the current state of the channel.
    #[allow(clippy::missing_panics_doc)] // false positive: channels are never removed
    pub fn channel_info(&self) -> ChannelInfo {
        self.manager.channel(self.channel_id).unwrap()
    }

    /// Checks whether this receiver can be used to receive messages from the channel.
    /// This is possible if the channel receiver is not held by a workflow.
    pub fn can_receive_messages(&self) -> bool {
        self.can_receive_messages
    }

    /// Takes messages from the channel, or `None` if messages are not received by host
    /// (i.e., [`Self::can_receive_messages()`] returns `false`).
    pub fn take_messages(&mut self) -> Option<TakenMessages<'_, T, C>> {
        if !self.can_receive_messages {
            return None;
        }

        let (start_idx, raw_messages) = self.manager.take_outbound_messages(self.channel_id);
        Some(TakenMessages {
            start_idx,
            raw_messages,
            codec: &mut self.codec,
            _item: PhantomData,
        })
    }

    /// Closes this channel from the host side. If [`Self::can_receive_messages()`] returns `false`,
    /// this is a no-op.
    pub fn close(self) {
        if self.can_receive_messages {
            self.manager.close_host_receiver(self.channel_id);
        }
    }
}

/// Result of taking messages from an outbound workflow channel.
#[derive(Debug)]
pub struct TakenMessages<'a, T, C> {
    start_idx: usize,
    raw_messages: Vec<Message>,
    codec: &'a mut C,
    _item: PhantomData<fn() -> T>,
}

impl<T, C: Decode<T>> TakenMessages<'_, T, C> {
    /// Returns zero-based indices of the taken messages.
    pub fn message_indices(&self) -> Range<usize> {
        self.start_idx..(self.start_idx + self.raw_messages.len())
    }

    /// Tries to decode the taken messages.
    ///
    /// # Errors
    ///
    /// Returns a decoding error, if any.
    pub fn decode(self) -> Result<Vec<T>, C::Error> {
        self.raw_messages
            .into_iter()
            .map(|bytes| self.codec.try_decode_bytes(bytes.into()))
            .collect()
    }
}

impl<'a, T, C, W> TakeHandle<WorkflowHandle<'a, W>> for Sender<T, C>
where
    C: Decode<T> + Default,
{
    type Id = str;
    type Handle = MessageReceiver<'a, T, C>;

    fn take_handle(env: &mut WorkflowHandle<'a, W>, id: &str) -> Result<Self::Handle, AccessError> {
        if let Some(channel_id) = env.ids.channel_ids.outbound.get(id).copied() {
            let channel_info = env.manager.channel(channel_id).unwrap();
            Ok(MessageReceiver {
                manager: env.manager,
                channel_id,
                can_receive_messages: channel_info.receiver_workflow_id().is_none(),
                codec: C::default(),
                _item: PhantomData,
            })
        } else {
            Err(AccessErrorKind::Unknown.with_location(OutboundChannel(id)))
        }
    }
}

/// Handle allowing to trace futures.
#[derive(Debug)]
pub struct TracerHandle<'a, C> {
    pub(super) receiver: MessageReceiver<'a, FutureUpdate, C>,
    pub(super) futures: TracedFutures,
}

impl<'a, C> TracerHandle<'a, C>
where
    C: Decode<FutureUpdate>,
{
    /// Returns current information about the underlying channel.
    pub fn channel_info(&self) -> ChannelInfo {
        self.receiver.channel_info()
    }

    /// Returns a reference to the traced futures.
    pub fn futures(&self) -> &TracedFutures {
        &self.futures
    }

    /// Sets futures, usually after restoring the handle.
    pub fn set_futures(&mut self, futures: TracedFutures) {
        self.futures = futures;
    }

    /// Returns traced futures, consuming this handle.
    pub fn into_futures(self) -> TracedFutures {
        self.futures
    }

    /// Takes tracing messages from the workflow and updates traced future states accordingly.
    ///
    /// # Errors
    ///
    /// Returns an error if decoding tracing messages fails, or if the updates are inconsistent
    /// w.r.t. the current tracer state (which could happen if resuming a workflow without
    /// calling [`Self::set_futures()`]).
    pub fn take_traces(&mut self) -> anyhow::Result<()> {
        if let Some(messages) = self.receiver.take_messages() {
            let updates = messages.decode().context("cannot decode `FutureUpdate`")?;
            for update in updates {
                self.futures
                    .update(update)
                    .context("incorrect `FutureUpdate` (was the tracing state persisted?)")?;
            }
        }
        Ok(())
    }
}

impl<'a, C, W> TakeHandle<WorkflowHandle<'a, W>> for Tracer<C>
where
    C: Decode<FutureUpdate> + Default,
{
    type Id = str;
    type Handle = TracerHandle<'a, C>;

    fn take_handle(env: &mut WorkflowHandle<'a, W>, id: &str) -> Result<Self::Handle, AccessError> {
        Ok(TracerHandle {
            receiver: Sender::<FutureUpdate, C>::take_handle(env, id)?,
            futures: TracedFutures::default(),
        })
    }
}

impl<'a> TakeHandle<WorkflowHandle<'a, ()>> for Interface {
    type Id = ();
    type Handle = Self;

    fn take_handle(
        env: &mut WorkflowHandle<'a, ()>,
        _id: &Self::Id,
    ) -> Result<Self::Handle, AccessError> {
        Ok(env
            .manager
            .interface_for_workflow(env.ids.workflow_id)
            .cloned()
            .unwrap())
        // ^ `unwrap()` is safe by construction: we only hand over `WorkflowEnv` for
        // existing workflows.
    }
}

impl<'a> TakeHandle<WorkflowHandle<'a, ()>> for () {
    type Id = ();
    type Handle = UntypedHandle<WorkflowHandle<'a, ()>>;

    fn take_handle(
        env: &mut WorkflowHandle<'a, ()>,
        _id: &Self::Id,
    ) -> Result<Self::Handle, AccessError> {
        UntypedHandle::take_handle(env, &())
    }
}
