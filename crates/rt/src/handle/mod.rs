//! Handles for [`Workflow`].

// FIXME: state of traced futures is lost across saves!

use anyhow::Context;

use std::{cell::RefCell, fmt, marker::PhantomData, ops::Range, rc::Rc};

pub mod future;

use crate::{
    receipt::{ExecutionError, Receipt},
    ConsumeError, Workflow,
};
use tardigrade::{
    channel::{Receiver, Sender},
    interface::{
        AccessError, AccessErrorKind, DataInput, InboundChannel, Interface, OutboundChannel,
    },
    trace::{FutureUpdate, TracedFutures, Tracer},
    workflow::{TakeHandle, UntypedHandle},
    Data, Decoder, Encoder,
};

/// Environment for a [`Workflow`].
///
/// This type is used as a type param for the [`TakeHandle`] trait. The returned handles
/// allow interacting with the workflow (e.g., [send messages](MessageSender) via inbound channels
/// and [take messages](MessageReceiver) from outbound channels).
pub struct WorkflowEnv<'a, W> {
    inner: Rc<RefCell<&'a mut Workflow<W>>>,
}

impl<W> fmt::Debug for WorkflowEnv<'_, W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowEnv")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<W> Clone for WorkflowEnv<'_, W> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<'a, W> WorkflowEnv<'a, W> {
    fn new(workflow: &'a mut Workflow<W>) -> Self {
        Self {
            inner: Rc::new(RefCell::new(workflow)),
        }
    }

    fn with<T>(&self, action: impl FnOnce(&mut Workflow<W>) -> T) -> T {
        let mut borrow = self.inner.borrow_mut();
        action(*borrow)
    }
}

/// Handle for an [inbound channel](Receiver) that allows sending messages
/// via the channel.
#[derive(Debug)]
pub struct MessageSender<'a, T, C, W> {
    env: WorkflowEnv<'a, W>,
    channel_name: String,
    codec: C,
    _item: PhantomData<fn(T)>,
}

impl<'a, T, C: Encoder<T>, W> MessageSender<'a, T, C, W> {
    /// Sends a message.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow is currently not waiting for messages
    /// on the associated inbound channel.
    pub fn send(&mut self, message: T) -> Result<SentMessage<'a, W>, ConsumeError> {
        let raw_message = self.codec.encode_value(message);
        self.env
            .with(|workflow| workflow.push_inbound_message(&self.channel_name, raw_message))
            .map(|()| SentMessage {
                env: self.env.clone(),
            })
    }
}

/// Result of sending a message over an inbound channel.
#[derive(Debug)]
#[must_use = "must be `flush`ed to progress the workflow"]
pub struct SentMessage<'a, W> {
    env: WorkflowEnv<'a, W>,
}

impl<W> SentMessage<'_, W> {
    /// Progresses the workflow after an inbound message is consumed.
    ///
    /// # Errors
    ///
    /// Returns an error if workflow execution traps.
    pub fn flush(self) -> Result<Receipt, ExecutionError> {
        self.env.with(Workflow::tick)
    }
}

impl<'a, T, C, W> TakeHandle<WorkflowEnv<'a, W>> for Receiver<T, C>
where
    C: Encoder<T> + Default,
{
    type Id = str;
    type Handle = MessageSender<'a, T, C, W>;

    fn take_handle(env: &mut WorkflowEnv<'a, W>, id: &str) -> Result<Self::Handle, AccessError> {
        let channel_exists =
            env.with(|workflow| workflow.interface().inbound_channel(id).is_some());
        if channel_exists {
            Ok(MessageSender {
                env: env.clone(),
                channel_name: id.to_owned(),
                codec: C::default(),
                _item: PhantomData,
            })
        } else {
            Err(AccessErrorKind::Unknown.for_handle(InboundChannel(id)))
        }
    }
}

/// Handle for an [outbound workflow channel](Sender) that allows taking messages
/// from the channel.
#[derive(Debug)]
pub struct MessageReceiver<'a, T, C, W> {
    env: WorkflowEnv<'a, W>,
    channel_name: String,
    codec: C,
    _item: PhantomData<fn() -> T>,
}

impl<T, C: Decoder<T>, W> MessageReceiver<'_, T, C, W> {
    /// Takes messages from the channel and progresses the flow marking the channel as flushed.
    ///
    /// # Errors
    ///
    /// Returns an error if workflow execution traps.
    pub fn take_messages(&mut self) -> Result<Receipt<TakenMessages<T, C>>, ExecutionError> {
        let (start_idx, raw_messages, exec_result) = self.env.with(|workflow| {
            let (start_idx, messages) = workflow.take_outbound_messages(&self.channel_name);
            (start_idx, messages, workflow.tick())
        });
        let messages = TakenMessages {
            start_idx,
            raw_messages,
            codec: &mut self.codec,
            _item: PhantomData,
        };
        exec_result.map(|receipt| receipt.map(|()| messages))
    }
}

/// Result of taking messages from an outbound workflow channel.
#[derive(Debug)]
pub struct TakenMessages<'a, T, C> {
    start_idx: usize,
    raw_messages: Vec<Vec<u8>>,
    codec: &'a mut C,
    _item: PhantomData<fn() -> T>,
}

impl<T, C: Decoder<T>> TakenMessages<'_, T, C> {
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
            .map(|bytes| self.codec.try_decode_bytes(bytes))
            .collect()
    }
}

impl<'a, T, C, W> TakeHandle<WorkflowEnv<'a, W>> for Sender<T, C>
where
    C: Decoder<T> + Default,
{
    type Id = str;
    type Handle = MessageReceiver<'a, T, C, W>;

    fn take_handle(env: &mut WorkflowEnv<'a, W>, id: &str) -> Result<Self::Handle, AccessError> {
        let channel_exists =
            env.with(|workflow| workflow.interface().outbound_channel(id).is_some());
        if channel_exists {
            Ok(MessageReceiver {
                env: env.clone(),
                channel_name: id.to_owned(),
                codec: C::default(),
                _item: PhantomData,
            })
        } else {
            Err(AccessErrorKind::Unknown.for_handle(OutboundChannel(id)))
        }
    }
}

impl<'a, T, C, W> TakeHandle<WorkflowEnv<'a, W>> for Data<T, C>
where
    C: Decoder<T> + Default,
{
    type Id = str;
    type Handle = T;

    fn take_handle(env: &mut WorkflowEnv<'a, W>, id: &str) -> Result<Self::Handle, AccessError> {
        let input_bytes = env.with(|workflow| workflow.data_input(id));
        if let Some(bytes) = input_bytes {
            C::default()
                .try_decode_bytes(bytes)
                .map_err(|err| AccessErrorKind::Custom(Box::new(err)).for_handle(DataInput(id)))
        } else {
            Err(AccessErrorKind::Unknown.for_handle(DataInput(id)))
        }
    }
}

/// Handle allowing to trace futures.
#[derive(Debug)]
pub struct TracerHandle<'a, C, W> {
    receiver: MessageReceiver<'a, FutureUpdate, C, W>,
    futures: TracedFutures,
}

impl<'a, C, W> TracerHandle<'a, C, W>
where
    C: Decoder<FutureUpdate>,
{
    /// Returns a reference to the traced futures.
    pub fn futures(&self) -> &TracedFutures {
        &self.futures
    }

    /// Takes tracing messages from the workflow and updates traced future states accordingly.
    ///
    /// # Errors
    ///
    /// Returns an error if an [`ExecutionError`] occurs when flushing tracing messages, or
    /// if decoding messages fails.
    pub fn take_traces(&mut self) -> anyhow::Result<Receipt> {
        let receipt = self
            .receiver
            .take_messages()
            .context("cannot flush tracing messages")?;
        let receipt = receipt
            .map(TakenMessages::decode)
            .transpose()
            .context("cannot decode `FutureUpdate`")?;

        let receipt = receipt.map(|updates| {
            for update in updates {
                if let Err(err) = self.futures.update(update) {
                    log::warn!(
                        target: "tardigrade_rt",
                        "Error tracing futures: {}. This shouldn't happen normally \
                         (is workflow module produced properly?)",
                        err
                    );
                }
            }
        });
        Ok(receipt)
    }
}

impl<'a, C, W> TakeHandle<WorkflowEnv<'a, W>> for Tracer<C>
where
    C: Decoder<FutureUpdate> + Default,
{
    type Id = str;
    type Handle = TracerHandle<'a, C, W>;

    fn take_handle(env: &mut WorkflowEnv<'a, W>, id: &str) -> Result<Self::Handle, AccessError> {
        Ok(TracerHandle {
            receiver: Sender::<FutureUpdate, C>::take_handle(env, id)?,
            futures: TracedFutures::default(), // FIXME: restore state
        })
    }
}

/// Handle for a [`Workflow`] allowing to access inbound / outbound channels and data inputs.
pub struct WorkflowHandle<'a, W>
where
    W: TakeHandle<WorkflowEnv<'a, W>, Id = ()> + 'a,
{
    /// API interface of the workflow, e.g. its inbound and outbound channels.
    pub api: <W as TakeHandle<WorkflowEnv<'a, W>>>::Handle,
    env: WorkflowEnv<'a, W>,
}

impl<'a, W> fmt::Debug for WorkflowHandle<'a, W>
where
    W: TakeHandle<WorkflowEnv<'a, W>, Id = ()> + 'a,
    <W as TakeHandle<WorkflowEnv<'a, W>>>::Handle: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowHandle")
            .field("api", &self.api)
            .field("env", &self.env)
            .finish()
    }
}

impl<'a, W> WorkflowHandle<'a, W>
where
    W: TakeHandle<WorkflowEnv<'a, W>, Id = ()> + 'a,
{
    pub(super) fn new(workflow: &'a mut Workflow<W>) -> Result<Self, AccessError> {
        let mut env = WorkflowEnv::new(workflow);
        Ok(Self {
            api: W::take_handle(&mut env, &())?,
            env,
        })
    }

    /// Performs an action on the workflow without dropping the handle.
    pub fn with<T>(&mut self, action: impl FnOnce(&mut Workflow<W>) -> T) -> T {
        self.env.with(action)
    }
}

impl<'a> TakeHandle<WorkflowEnv<'a, ()>> for Interface<()> {
    type Id = ();
    type Handle = Self;

    fn take_handle(
        env: &mut WorkflowEnv<'a, ()>,
        _id: &Self::Id,
    ) -> Result<Self::Handle, AccessError> {
        Ok(env.with(|workflow| workflow.interface().clone()))
    }
}

impl<'a> TakeHandle<WorkflowEnv<'a, ()>> for () {
    type Id = ();
    type Handle = UntypedHandle<WorkflowEnv<'a, ()>>;

    fn take_handle(
        env: &mut WorkflowEnv<'a, ()>,
        _id: &Self::Id,
    ) -> Result<Self::Handle, AccessError> {
        UntypedHandle::take_handle(env, &())
    }
}
