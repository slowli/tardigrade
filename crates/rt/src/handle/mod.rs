//! Handles for `WorkflowManager`.

use anyhow::Context;

use std::{fmt, marker::PhantomData, ops::Range};

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub mod future;

use crate::{
    manager::{ChannelInfo, WorkflowManager},
    module::WorkflowAndChannelIds,
    receipt::{ExecutionError, Receipt},
    utils::Message,
    ChannelId, PersistedWorkflow, WorkflowId,
};
use tardigrade::{
    channel::{Receiver, Sender},
    interface::{
        AccessError, AccessErrorKind, InboundChannel, Interface, OutboundChannel, ValidateInterface,
    },
    trace::{FutureUpdate, TracedFutures, Tracer},
    workflow::{TakeHandle, UntypedHandle},
    Decode, Encode,
};
use tardigrade_shared::SendError;

/// Environment for executing [`Workflow`]s.
///
/// This type is used as a type param for the [`TakeHandle`] trait. The returned handles
/// allow interacting with the workflow (e.g., [send messages](MessageSender) via inbound channels
/// and [take messages](MessageReceiver) from outbound channels).
///
/// See [`AsyncEnv`](future::AsyncEnv) for a more high-level, future-based alternative.
///
/// # Examples
///
/// ```
/// use tardigrade::interface::{InboundChannel, OutboundChannel};
/// use tardigrade_rt::{handle::WorkflowHandle, Workflow};
///
/// # fn test_wrapper(workflow: Workflow<()>) -> anyhow::Result<()> {
/// // Assume we have a dynamically typed workflow:
/// let mut workflow: Workflow<()> = // ...
/// #   workflow;
/// // We can create a handle to manipulate the workflow.
/// let mut handle = WorkflowHandle::new(&mut workflow).handle();
///
/// // Let's send a message via an inbound channel.
/// let message = b"hello".to_vec();
/// let receipt = handle.api[InboundChannel("commands")]
///     .send(message)?
///     .flush()?;
/// // `receipt` contains information about executed functions,
/// // spawned tasks, timers, etc.
/// println!("{:?}", receipt.executions());
///
/// // Let's then take outbound messages from a certain channel:
/// let receipt = handle.api[OutboundChannel("events")].take_messages()?;
/// let messages: Vec<Vec<u8>> = receipt.into_inner().decode().unwrap();
/// // ^ `decode().unwrap()` always succeeds because the codec
/// // for untyped workflows is just an identity.
///
/// // It is possible to access / manipulate the underlying `Workflow`:
/// let receipt = handle.with(|workflow| {
///     println!("{:?}", workflow.tasks().collect::<Vec<_>>());
///     let now = workflow.current_time();
///     workflow.set_current_time(now + chrono::Duration::seconds(1))
/// })?;
/// println!("{:?}", receipt.executions());
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

    /// # Errors
    ///
    /// Returns an error on workflow interface mismatch.
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn downcast<W: ValidateInterface<Id = ()>>(
        self,
    ) -> Result<WorkflowHandle<'a, W>, AccessError> {
        let interface = self
            .manager
            .interface_for_workflow(self.ids.workflow_id)
            .unwrap();
        W::validate_interface(interface, &())?;
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

// FIXME: improve API so that the handle cannot be invalidated (e.g., by `tick()`)
impl<W: TakeHandle<Self, Id = ()>> WorkflowHandle<'_, W> {
    /// Returns the ID of this workflow.
    pub fn id(&self) -> WorkflowId {
        self.ids.workflow_id
    }

    /// Executes this workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if an error occurs during execution.
    pub fn tick(&self) -> Result<Receipt, ExecutionError> {
        self.manager.tick_workflow(self.ids.workflow_id)
    }

    /// Returns the current persisted state of the workflow.
    pub fn persisted(&self) -> PersistedWorkflow {
        self.manager.persisted_workflow(self.ids.workflow_id)
    }

    /// Returns a handle for the workflow.
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
    codec: C,
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
        self.manager.channel_info(self.channel_id).unwrap()
    }

    /// Sends a message over the channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow is currently not waiting for messages
    /// on the associated channel, or if the channel is closed.
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
    codec: C,
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
        self.manager.channel_info(self.channel_id).unwrap()
    }

    /// Takes messages from the channel and progresses the flow marking the channel as flushed.
    ///
    /// # Errors
    ///
    /// Returns an error if workflow execution traps.
    pub fn take_messages(&mut self) -> TakenMessages<'_, T, C> {
        let (start_idx, raw_messages) = self.manager.take_outbound_messages(self.channel_id);
        TakenMessages {
            start_idx,
            raw_messages,
            codec: &mut self.codec,
            _item: PhantomData,
        }
    }

    /// Closes this channel from the host side.
    pub fn close(self) {
        self.manager.close_host_receiver(self.channel_id);
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
            Ok(MessageReceiver {
                manager: env.manager,
                channel_id,
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
    receiver: MessageReceiver<'a, FutureUpdate, C>,
    futures: TracedFutures,
}

impl<'a, C> TracerHandle<'a, C>
where
    C: Decode<FutureUpdate>,
{
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
    /// Returns an error if an [`ExecutionError`] occurs when flushing tracing messages, or
    /// if decoding messages fails.
    pub fn take_traces(&mut self) -> anyhow::Result<()> {
        let messages = self.receiver.take_messages();
        let updates = messages.decode().context("cannot decode `FutureUpdate`")?;
        for update in updates {
            self.futures
                .update(update)
                .context("incorrect `FutureUpdate` (was the tracing state persisted?)")?;
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

impl<'a> TakeHandle<WorkflowHandle<'a, ()>> for Interface<()> {
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
