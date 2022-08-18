//! Handles for [`Workflow`], allowing to interact with it (e.g., send and receive messages).
//!
//! There are 2 types of handles provided:
//!
//! - [`WorkflowEnv`] / [`WorkflowHandle`] provides low-level synchronous API.
//! - [`AsyncEnv`] provides more high-level, future-based API.
//!
//! See [`WorkflowEnv`] and [`AsyncEnv`] docs for examples of usage.
//!
//! [`AsyncEnv`]: crate::handle::future::AsyncEnv

use anyhow::Context;

use std::{cell::RefCell, fmt, marker::PhantomData, ops::Range, rc::Rc};

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub mod future;

use crate::{
    receipt::{ExecutionError, Receipt},
    ConsumeError, Workflow,
};
use tardigrade::{
    channel::{Receiver, Sender},
    interface::{AccessError, AccessErrorKind, InboundChannel, Interface, OutboundChannel},
    trace::{FutureUpdate, TracedFutures, Tracer},
    workflow::{EnvExtensions, TakeHandle, UntypedHandle},
    Decode, Encode,
};

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
/// use tardigrade_rt::{handle::WorkflowEnv, Workflow};
///
/// # fn test_wrapper(workflow: Workflow<()>) -> anyhow::Result<()> {
/// // Assume we have a dynamically typed workflow:
/// let mut workflow: Workflow<()> = // ...
/// #   workflow;
/// // We can create a handle to manipulate the workflow.
/// let mut handle = WorkflowEnv::new(&mut workflow).handle();
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
pub struct WorkflowEnv<'a, W> {
    inner: Rc<RefCell<&'a mut Workflow<W>>>,
    extensions: EnvExtensions,
}

impl<W> fmt::Debug for WorkflowEnv<'_, W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowEnv")
            .field("inner", &self.inner)
            .field("extensions", &self.extensions)
            .finish()
    }
}

impl<'a, W> WorkflowEnv<'a, W> {
    /// Creates a new environment for the provided [`Workflow`].
    pub fn new(workflow: &'a mut Workflow<W>) -> Self {
        Self {
            inner: Rc::new(RefCell::new(workflow)),
            extensions: EnvExtensions::default(),
        }
    }

    /// Returns a mutable reference to environment extensions.
    pub fn extensions(&mut self) -> &mut EnvExtensions {
        &mut self.extensions
    }

    fn with<T>(&self, action: impl FnOnce(&mut Workflow<W>) -> T) -> T {
        let mut borrow = self.inner.borrow_mut();
        action(*borrow)
    }
}

impl<'a, W> WorkflowEnv<'a, W>
where
    W: TakeHandle<WorkflowEnv<'a, W>, Id = ()> + 'a,
{
    /// Creates a workflow handle from this environment.
    #[allow(clippy::missing_panics_doc)] // TODO: is `unwrap()` really safe here?
    pub fn handle(mut self) -> WorkflowHandle<'a, W> {
        WorkflowHandle {
            api: W::take_handle(&mut self, &()).unwrap(),
            env: self,
        }
    }
}

/// Handle for an [inbound workflow channel](Receiver) that allows sending messages
/// via the channel.
#[derive(Debug)]
pub struct MessageSender<'a, T, C, W> {
    workflow: Rc<RefCell<&'a mut Workflow<W>>>,
    channel_name: String,
    codec: C,
    _item: PhantomData<fn(T)>,
}

impl<'a, T, C: Encode<T>, W> MessageSender<'a, T, C, W> {
    /// Sends a message over the channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow is currently not waiting for messages
    /// on the associated channel, or if the channel is closed.
    pub fn send(&mut self, message: T) -> Result<SentMessage<'a, W>, ConsumeError> {
        let raw_message = self.codec.encode_value(message);
        self.workflow
            .borrow_mut()
            .push_inbound_message(&self.channel_name, raw_message)?;
        Ok(SentMessage {
            workflow: Rc::clone(&self.workflow),
        })
    }

    /// Closes this channel from the host side.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel is already closed, or the workflow
    /// is currently not waiting for messages on the associated inbound channel.
    pub fn close(self) -> Result<SentMessage<'a, W>, ConsumeError> {
        self.workflow
            .borrow_mut()
            .close_inbound_channel(&self.channel_name)?;
        Ok(SentMessage {
            workflow: Rc::clone(&self.workflow),
        })
    }
}

/// Result of sending a message over an inbound channel.
#[derive(Debug)]
#[must_use = "must be `flush`ed to progress the workflow"]
pub struct SentMessage<'a, W> {
    workflow: Rc<RefCell<&'a mut Workflow<W>>>,
}

impl<W> SentMessage<'_, W> {
    /// Progresses the workflow after an inbound message is consumed.
    ///
    /// # Errors
    ///
    /// Returns an error if workflow execution traps.
    pub fn flush(self) -> Result<Receipt, ExecutionError> {
        self.workflow.borrow_mut().tick()
    }
}

impl<'a, T, C, W> TakeHandle<WorkflowEnv<'a, W>> for Receiver<T, C>
where
    C: Encode<T> + Default,
{
    type Id = str;
    type Handle = MessageSender<'a, T, C, W>;

    fn take_handle(env: &mut WorkflowEnv<'a, W>, id: &str) -> Result<Self::Handle, AccessError> {
        let channel_exists =
            env.with(|workflow| workflow.interface().inbound_channel(id).is_some());
        if channel_exists {
            Ok(MessageSender {
                workflow: Rc::clone(&env.inner),
                channel_name: id.to_owned(),
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
pub struct MessageReceiver<'a, T, C, W> {
    workflow: Rc<RefCell<&'a mut Workflow<W>>>,
    channel_name: String,
    codec: C,
    _item: PhantomData<fn() -> T>,
}

impl<T, C: Decode<T>, W> MessageReceiver<'_, T, C, W> {
    /// Takes messages from the channel and progresses the flow marking the channel as flushed.
    ///
    /// # Errors
    ///
    /// Returns an error if workflow execution traps.
    pub fn take_messages(&mut self) -> Result<Receipt<TakenMessages<T, C>>, ExecutionError> {
        let (start_idx, raw_messages, exec_result) = {
            let mut workflow = self.workflow.borrow_mut();
            let (start_idx, messages) = workflow.take_outbound_messages(&self.channel_name);
            (start_idx, messages, workflow.tick())
        };
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
            .map(|bytes| self.codec.try_decode_bytes(bytes))
            .collect()
    }
}

impl<'a, T, C, W> TakeHandle<WorkflowEnv<'a, W>> for Sender<T, C>
where
    C: Decode<T> + Default,
{
    type Id = str;
    type Handle = MessageReceiver<'a, T, C, W>;

    fn take_handle(env: &mut WorkflowEnv<'a, W>, id: &str) -> Result<Self::Handle, AccessError> {
        let channel_exists =
            env.with(|workflow| workflow.interface().outbound_channel(id).is_some());
        if channel_exists {
            Ok(MessageReceiver {
                workflow: Rc::clone(&env.inner),
                channel_name: id.to_owned(),
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
pub struct TracerHandle<'a, C, W> {
    receiver: MessageReceiver<'a, FutureUpdate, C, W>,
    futures: TracedFutures,
}

impl<'a, C, W> TracerHandle<'a, C, W>
where
    C: Decode<FutureUpdate>,
{
    /// Returns a reference to the traced futures.
    pub fn futures(&self) -> &TracedFutures {
        &self.futures
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
    pub fn take_traces(&mut self) -> anyhow::Result<Receipt> {
        let receipt = self
            .receiver
            .take_messages()
            .context("cannot flush tracing messages")?;
        let receipt = receipt
            .map(TakenMessages::decode)
            .transpose()
            .context("cannot decode `FutureUpdate`")?;

        receipt
            .map(|updates| {
                updates.into_iter().try_fold((), |_, update| {
                    self.futures
                        .update(update)
                        .context("incorrect `FutureUpdate` (was the tracing state persisted?)")
                })
            })
            .transpose()
    }
}

impl<'a, C, W> TakeHandle<WorkflowEnv<'a, W>> for Tracer<C>
where
    C: Decode<FutureUpdate> + Default,
{
    type Id = str;
    type Handle = TracerHandle<'a, C, W>;

    fn take_handle(env: &mut WorkflowEnv<'a, W>, id: &str) -> Result<Self::Handle, AccessError> {
        Ok(TracerHandle {
            receiver: Sender::<FutureUpdate, C>::take_handle(env, id)?,
            futures: Self::take_handle(&mut env.extensions, id)?,
        })
    }
}

/// Handle for a [`Workflow`] allowing to access inbound / outbound channels.
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
