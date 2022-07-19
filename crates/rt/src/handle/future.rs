//! Async handles for [`Workflow`].

use anyhow::Context as _;
use chrono::{DateTime, Utc};
use futures::{channel::mpsc, future, FutureExt, Sink, Stream, StreamExt};
use pin_project_lite::pin_project;

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    handle::EnvExtensions,
    receipt::{ExecutionError, Receipt},
    workflow::Workflow,
    FutureId,
};
use tardigrade::{
    channel::{Receiver, Sender},
    interface::{
        AccessError, AccessErrorKind, DataInput, InboundChannel, Interface, OutboundChannel,
    },
    trace::{FutureUpdate, TracedFuture, TracedFutures, Tracer},
    workflow::{TakeHandle, UntypedHandle},
    Data, Decoder, Encoder,
};

/// Future for [`Schedule::create_timer()`].
pub type TimerFuture = Pin<Box<dyn Future<Output = DateTime<Utc>> + Send>>;

/// Scheduler that allows creating futures completing at the specified timestamp.
pub trait Schedule: Send + 'static {
    /// Creates a timer with the specified expiration timestamp.
    fn create_timer(&mut self, expires_at: DateTime<Utc>) -> TimerFuture;
}

impl fmt::Debug for dyn Schedule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Schedule").finish_non_exhaustive()
    }
}

/// [Scheduler](Schedule) implementation from [`async-io`] (a part of [`async-std`] suite).
///
/// [`async-io`]: https://docs.rs/async-io/
/// [`async-std`]: https://docs.rs/async-std/
#[cfg(feature = "async-io")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-io")))]
#[derive(Debug)]
pub struct AsyncIoScheduler;

#[cfg(feature = "async-io")]
impl Schedule for AsyncIoScheduler {
    fn create_timer(&mut self, timestamp: DateTime<Utc>) -> TimerFuture {
        use async_io::Timer;
        use std::time::{Instant, SystemTime};

        let timestamp = SystemTime::from(timestamp);
        let (now_instant, now) = (Instant::now(), SystemTime::now());
        match timestamp.duration_since(now) {
            Ok(diff) => {
                let timer = Timer::at(now_instant + diff);
                let timer = FutureExt::map(timer, move |instant| {
                    let new_time = now + (instant - now_instant);
                    new_time.into()
                });
                Box::pin(timer)
            }
            Err(_) => Box::pin(future::ready(now.into())),
        }
    }
}

/// Asynchronous environment for executing [`Workflow`]s.
///
/// This type is used as a type param for the [`TakeHandle`] trait. The returned handles
/// allow interacting with the workflow (e.g., [send messages](MessageSender) via inbound channels
/// and [take messages](MessageReceiver) from outbound channels).
///
/// # Examples
///
/// ```
/// use async_std::task;
/// use futures::prelude::*;
/// use tardigrade::interface::{InboundChannel, OutboundChannel};
/// use tardigrade_rt::{handle::future::{AsyncEnv, AsyncIoScheduler}, Workflow};
///
/// # async fn test_wrapper(workflow: Workflow<()>) -> anyhow::Result<()> {
/// // Assume we have a dynamically typed workflow:
/// let workflow: Workflow<()> = // ...
/// #   workflow;
/// // First, create an environment to execute the workflow in.
/// let mut env = AsyncEnv::new(workflow, AsyncIoScheduler);
/// let mut handle = env.handle();
/// // Run the environment in a separate task.
/// task::spawn(async move { env.run().await });
///
/// // Let's send a message via an inbound channel...
/// let message = b"hello".to_vec();
/// handle[InboundChannel("commands")].send(message).await?;
///
/// // ...and wait for some outbound messages
/// let events = handle[OutboundChannel("events")].by_ref();
/// let events: Vec<Vec<u8>> = events.take(2).try_collect().await.unwrap();
/// // ^ `unwrap()` always succeeds because the codec for untyped workflows
/// // is just an identity.
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct AsyncEnv<W> {
    workflow: Workflow<W>,
    scheduler: Box<dyn Schedule>,
    inbound_channels: HashMap<String, mpsc::Receiver<Vec<u8>>>,
    outbound_channels: HashMap<String, mpsc::UnboundedSender<Vec<u8>>>,
    receipts: Option<mpsc::UnboundedSender<Receipt>>,
    extensions: EnvExtensions,
}

#[derive(Debug)]
enum ListenedEventOutput<'a> {
    Channel {
        name: &'a str,
        message: Option<Vec<u8>>,
    },
    Timer(DateTime<Utc>),
}

/// Terminal status of a [`Workflow`].
#[derive(Debug)]
#[non_exhaustive]
pub enum Termination {
    /// The workflow is finished.
    Finished,
    /// The workflow has stalled: its progress does not depend on any external futures
    /// (inbound messages or timers).
    Stalled,
}

impl<W> AsyncEnv<W> {
    /// Creates an async environment for a `workflow` that uses the specified `scheduler`
    /// for timers.
    pub fn new(workflow: Workflow<W>, scheduler: impl Schedule) -> Self {
        Self {
            workflow,
            scheduler: Box::new(scheduler),
            inbound_channels: HashMap::new(),
            outbound_channels: HashMap::new(),
            receipts: None,
            extensions: EnvExtensions::default(),
        }
    }

    /// Returns the receiver for execution [`Receipt`]s.
    pub fn receipts(&mut self) -> impl Stream<Item = Receipt> {
        let (sx, rx) = mpsc::unbounded();
        self.receipts = Some(sx);
        rx
    }

    /// Retrieves the underlying workflow, consuming the environment.
    pub fn into_inner(self) -> Workflow<W> {
        self.workflow
    }

    /// Returns a mutable reference to environment extensions.
    pub fn extensions(&mut self) -> &mut EnvExtensions {
        &mut self.extensions
    }

    /// Executes the enclosed [`Workflow`] until it is terminated or an error occurs.
    /// As the workflow executes, outbound messages and [`Receipt`]s will be sent using
    /// respective channels.
    ///
    /// Note that it is possible to cancel this future (e.g., by [`select`]ing between it
    /// and a cancellation signal) and continue working with the enclosed workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if workflow execution traps.
    ///
    /// [`select`]: futures::select
    pub async fn run(&mut self) -> Result<Termination, ExecutionError> {
        loop {
            if let Some(termination) = self.tick().await? {
                return Ok(termination);
            }
        }
    }

    async fn tick(&mut self) -> Result<Option<Termination>, ExecutionError> {
        if self.workflow.is_finished() {
            return Ok(Some(Termination::Finished));
        }

        // First, flush all outbound messages (synchronous and cheap).
        self.flush_outbound_messages()?;
        if self.workflow.is_finished() {
            return Ok(Some(Termination::Finished));
        }

        // Determine external events listened by the workflow.
        let events = self.workflow.listened_events();
        if events.is_empty() {
            return Ok(Some(Termination::Stalled));
        }

        let channel_futures = self.inbound_channels.iter_mut().filter_map(|(name, rx)| {
            if events.inbound_channels.contains(name) {
                let fut = rx
                    .next()
                    .map(|message| ListenedEventOutput::Channel { name, message })
                    .left_future();
                Some(fut)
            } else {
                None
            }
        });

        // TODO: cache `timer_event`?
        let timer_event = events.nearest_timer.map(|timestamp| {
            let timer = self.scheduler.create_timer(timestamp);
            timer.map(ListenedEventOutput::Timer).right_future()
        });
        let all_futures = channel_futures.chain(timer_event);

        // This is the only `await` placement in the future, and it happens when the workflow
        // is safe to save: it has no outbound messages.
        let (output, ..) = future::select_all(all_futures).await;
        let receipt = match output {
            ListenedEventOutput::Channel { name, message } => {
                if let Some(message) = message {
                    self.workflow.push_inbound_message(name, message).unwrap();
                } else {
                    self.workflow.close_inbound_channel(name).unwrap();
                }
                // ^ `unwrap()`s above are safe: we know `workflow` listens to the channel
                self.workflow.tick()?
            }

            ListenedEventOutput::Timer(timestamp) => self.workflow.set_current_time(timestamp)?,
        };
        self.send_receipt(receipt);

        Ok(None)
    }

    /// Repeatedly flushes messages to the outbound channels until no messages are left.
    fn flush_outbound_messages(&mut self) -> Result<(), ExecutionError> {
        loop {
            let mut messages_sent = false;
            for (name, sx) in &mut self.outbound_channels {
                let (_, messages) = self.workflow.take_outbound_messages(name);
                messages_sent = messages_sent || !messages.is_empty();
                for message in messages {
                    sx.unbounded_send(message).ok();
                    // ^ We don't care if outbound messages are no longer listened to.
                }
            }

            if messages_sent {
                let receipt = self.workflow.tick()?;
                self.send_receipt(receipt);
            } else {
                break Ok(());
            }
        }
    }

    fn send_receipt(&mut self, receipt: Receipt) {
        if let Some(receipts) = &mut self.receipts {
            receipts.unbounded_send(receipt).ok();
            // ^ We don't care if nobody listens to receipts.
        }
    }
}

impl<W> AsyncEnv<W>
where
    W: TakeHandle<AsyncEnv<W>, Id = ()>,
{
    /// Creates a workflow handle from this environment.
    #[allow(clippy::missing_panics_doc)] // TODO: is `unwrap()` really safe here?
    pub fn handle(&mut self) -> W::Handle {
        W::take_handle(self, &()).unwrap()
    }
}

pin_project! {
    /// Async handle for an [inbound workflow channel](Receiver) that allows sending messages
    /// via the channel.
    ///
    /// Dropping the handle while [`AsyncEnv::run()`] is executing will signal to the workflow
    /// that the corresponding channel is closed by the host.
    #[derive(Debug)]
    pub struct MessageSender<T, C> {
        #[pin]
        raw_sender: mpsc::Sender<Vec<u8>>,
        codec: C,
        _item: PhantomData<fn(T)>,
    }
}

impl<T, C: Clone> Clone for MessageSender<T, C> {
    fn clone(&self) -> Self {
        Self {
            raw_sender: self.raw_sender.clone(),
            codec: self.codec.clone(),
            _item: PhantomData,
        }
    }
}

impl<T, C: Encoder<T>> Sink<T> for MessageSender<T, C> {
    type Error = mpsc::SendError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().raw_sender.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let projection = self.project();
        let item = projection.codec.encode_value(item);
        projection.raw_sender.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().raw_sender.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().raw_sender.poll_close(cx)
    }
}

impl<T, C, W> TakeHandle<AsyncEnv<W>> for Receiver<T, C>
where
    C: Encoder<T> + Default,
{
    type Id = str;
    type Handle = MessageSender<T, C>;

    fn take_handle(env: &mut AsyncEnv<W>, id: &str) -> Result<Self::Handle, AccessError> {
        let channel_exists = env.workflow.interface().inbound_channel(id).is_some();
        if channel_exists {
            let (sx, rx) = mpsc::channel(1);
            env.inbound_channels.insert(id.to_owned(), rx);
            Ok(MessageSender {
                raw_sender: sx,
                codec: C::default(),
                _item: PhantomData,
            })
        } else {
            Err(AccessErrorKind::Unknown.with_location(InboundChannel(id)))
        }
    }
}

pin_project! {
    /// Async handle for an [outbound workflow channel](Sender) that allows taking messages
    /// from the channel.
    ///
    /// Dropping the handle has no effect on workflow execution (outbound channels cannot
    /// be closed by the host), but allows to save resources, since outbound messages
    /// would be dropped rather than buffered.
    #[derive(Debug)]
    pub struct MessageReceiver<T, C> {
        #[pin]
        raw_receiver: mpsc::UnboundedReceiver<Vec<u8>>,
        codec: C,
        _item: PhantomData<fn() -> T>,
    }
}

impl<T, C: Decoder<T>> Stream for MessageReceiver<T, C> {
    type Item = Result<T, C::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projection = self.project();
        match projection.raw_receiver.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(bytes)) => Poll::Ready(Some(projection.codec.try_decode_bytes(bytes))),
        }
    }
}

impl<T, C, W> TakeHandle<AsyncEnv<W>> for Sender<T, C>
where
    C: Decoder<T> + Default,
{
    type Id = str;
    type Handle = MessageReceiver<T, C>;

    fn take_handle(env: &mut AsyncEnv<W>, id: &str) -> Result<Self::Handle, AccessError> {
        let channel_exists = env.workflow.interface().outbound_channel(id).is_some();
        if channel_exists {
            let (sx, rx) = mpsc::unbounded();
            env.outbound_channels.insert(id.to_owned(), sx);
            Ok(MessageReceiver {
                raw_receiver: rx,
                codec: C::default(),
                _item: PhantomData,
            })
        } else {
            Err(AccessErrorKind::Unknown.with_location(OutboundChannel(id)))
        }
    }
}

impl<T, C, W> TakeHandle<AsyncEnv<W>> for Data<T, C>
where
    C: Decoder<T> + Default,
{
    type Id = str;
    type Handle = T;

    fn take_handle(env: &mut AsyncEnv<W>, id: &str) -> Result<Self::Handle, AccessError> {
        let input_bytes = env.workflow.data_input(id);
        if let Some(bytes) = input_bytes {
            C::default()
                .try_decode_bytes(bytes)
                .map_err(|err| AccessErrorKind::Custom(Box::new(err)).with_location(DataInput(id)))
        } else {
            Err(AccessErrorKind::Unknown.with_location(DataInput(id)))
        }
    }
}

pin_project! {
    /// Async handle allowing to trace futures.
    ///
    /// This handle is a [`Stream`] emitting updated future states as the updates are received
    /// from the workflow.
    #[derive(Debug)]
    pub struct TracerHandle<C> {
        #[pin]
        receiver: MessageReceiver<FutureUpdate, C>,
        channel_name: String,
        futures: TracedFutures,
    }
}

impl<C> TracerHandle<C> {
    /// Returns a reference to the traced futures.
    pub fn futures(&self) -> &TracedFutures {
        &self.futures
    }

    /// Returns traced futures, consuming this handle.
    pub fn into_futures(self) -> TracedFutures {
        self.futures
    }
}

impl<C: Decoder<FutureUpdate>> Stream for TracerHandle<C> {
    type Item = anyhow::Result<(FutureId, TracedFuture)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projection = self.project();
        let update = match projection.receiver.poll_next(cx) {
            Poll::Ready(Some(Ok(update))) => update,
            Poll::Ready(Some(Err(err))) => {
                let res = Err(err).context("cannot decode `FutureUpdate`");
                return Poll::Ready(Some(res));
            }
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };

        let future_id = update.id;
        let update_result = projection
            .futures
            .update(update)
            .map(|()| (future_id, projection.futures[future_id].clone()))
            .context("invalid update");
        Poll::Ready(Some(update_result))
    }
}

impl<C: Decoder<FutureUpdate> + Default, W> TakeHandle<AsyncEnv<W>> for Tracer<C> {
    type Id = str;
    type Handle = TracerHandle<C>;

    fn take_handle(env: &mut AsyncEnv<W>, id: &str) -> Result<Self::Handle, AccessError> {
        let receiver = Sender::<FutureUpdate, C>::take_handle(env, id)?;
        Ok(TracerHandle {
            receiver,
            channel_name: id.to_owned(),
            futures: Self::take_handle(&mut env.extensions, id)?,
        })
    }
}

impl<'a> TakeHandle<AsyncEnv<()>> for Interface<()> {
    type Id = ();
    type Handle = Self;

    fn take_handle(env: &mut AsyncEnv<()>, _id: &Self::Id) -> Result<Self::Handle, AccessError> {
        Ok(env.workflow.interface().clone())
    }
}

impl TakeHandle<AsyncEnv<()>> for () {
    type Id = ();
    type Handle = UntypedHandle<AsyncEnv<()>>;

    fn take_handle(env: &mut AsyncEnv<()>, _id: &Self::Id) -> Result<Self::Handle, AccessError> {
        UntypedHandle::take_handle(env, &())
    }
}
