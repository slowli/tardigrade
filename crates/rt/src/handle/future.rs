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
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    manager::WorkflowManager,
    receipt::{ExecutionError, ExecutionResult},
    ChannelId, FutureId,
};
use tardigrade::{
    trace::{FutureUpdate, TracedFuture, TracedFutures},
    Decode, Encode,
};

/// Future for [`Schedule::create_timer()`].
pub type TimerFuture = Pin<Box<dyn Future<Output = DateTime<Utc>> + Send>>;

/// Scheduler that allows creating futures completing at the specified timestamp.
pub trait Schedule: Send + Sync + 'static {
    /// Creates a timer with the specified expiration timestamp.
    fn create_timer(&self, expires_at: DateTime<Utc>) -> TimerFuture;
}

impl fmt::Debug for dyn Schedule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Schedule").finish_non_exhaustive()
    }
}

impl<T: Schedule + ?Sized> Schedule for Arc<T> {
    fn create_timer(&self, expires_at: DateTime<Utc>) -> TimerFuture {
        (&**self).create_timer(expires_at)
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
    fn create_timer(&self, timestamp: DateTime<Utc>) -> TimerFuture {
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

#[derive(Debug)]
enum ListenedEventOutput {
    Channel {
        id: ChannelId,
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

/// Asynchronous environment for executing [`Workflow`]s.
///
/// This type is used as a type param for the [`TakeHandle`] trait. The returned handles
/// allow interacting with the workflow (e.g., [send messages](MessageSender) via inbound channels
/// and [take messages](MessageReceiver) from outbound channels).
///
/// # Error handling
///
/// By default, workflow execution via [`Self::run()`] terminates immediately after a trap.
/// Only rudimentary cleanup is performed; thus, the workflow may be in an inconsistent state.
/// To change this behavior, set a rollback strategy via [`Self::set_rollback_strategy()`],
/// such as [`Rollback::any_trap()`]. As an example, rolling back receiving a message
/// means that from the workflow perspective, the message was never received in the first place,
/// and all progress resulting from receiving the message is lost (new tasks, timers, etc.).
/// Whether this makes sense, depends on a use case; e.g., it seems reasonable to roll back
/// deserialization errors for dynamically typed workflows.
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
pub struct AsyncEnv {
    scheduler: Box<dyn Schedule>,
    inbound_channels: HashMap<ChannelId, mpsc::Receiver<Vec<u8>>>,
    outbound_channels: HashMap<ChannelId, mpsc::UnboundedSender<Vec<u8>>>,
    results_sx: Option<mpsc::UnboundedSender<ExecutionResult>>,
    drop_erroneous_messages: bool,
}

impl AsyncEnv {
    /// Creates an async environment for a `workflow` that uses the specified `scheduler`
    /// for timers.
    pub fn new(scheduler: impl Schedule) -> Self {
        Self {
            scheduler: Box::new(scheduler),
            inbound_channels: HashMap::new(),
            outbound_channels: HashMap::new(),
            results_sx: None,
            drop_erroneous_messages: false,
        }
    }

    /// Returns the receiver of [`ExecutionResult`]s generated during workflow execution.
    pub fn execution_results(&mut self) -> mpsc::UnboundedReceiver<ExecutionResult> {
        let (sx, rx) = mpsc::unbounded();
        self.results_sx = Some(sx);
        rx
    }

    /// Indicates that the environment should drop any inbound messages that lead
    /// to execution errors.
    pub fn drop_erroneous_messages(&mut self) {
        self.drop_erroneous_messages = true;
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
    /// Returns an error if workflow execution traps and the [`RollbackStrategy`] (if any)
    /// determines that the workflow cannot be rolled back.
    ///
    /// [`select`]: futures::select
    pub async fn run(
        &mut self,
        manager: &mut WorkflowManager,
    ) -> Result<Termination, ExecutionError> {
        loop {
            if let Some(termination) = self.tick(manager).await? {
                return Ok(termination);
            }
        }
    }

    async fn tick(
        &mut self,
        manager: &mut WorkflowManager,
    ) -> Result<Option<Termination>, ExecutionError> {
        if manager.is_finished() {
            return Ok(Some(Termination::Finished));
        }

        let nearest_timer_expiration = self.tick_manager(manager)?;
        self.gc(manager);
        if manager.is_finished() {
            return Ok(Some(Termination::Finished));
        } else if nearest_timer_expiration.is_none() && self.inbound_channels.is_empty() {
            return Ok(Some(Termination::Stalled));
        }

        // Determine external events listened by the workflow.
        let channel_futures = self.inbound_channels.iter_mut().map(|(&id, rx)| {
            rx.next()
                .map(move |message| ListenedEventOutput::Channel { id, message })
                .left_future()
        });

        // TODO: cache `timer_event`?
        let timer_event = nearest_timer_expiration.map(|timestamp| {
            let timer = self.scheduler.create_timer(timestamp);
            timer.map(ListenedEventOutput::Timer).right_future()
        });
        let all_futures = channel_futures.chain(timer_event);

        let (output, ..) = future::select_all(all_futures).await;
        match output {
            ListenedEventOutput::Channel { id, message } => {
                if let Some(message) = message {
                    manager.send_message(id, message).unwrap();
                } else {
                    manager.close_channel_sender(id);
                }
            }

            ListenedEventOutput::Timer(timestamp) => {
                manager.set_current_time(timestamp);
            }
        };

        Ok(None)
    }

    fn tick_manager(
        &mut self,
        manager: &mut WorkflowManager,
    ) -> Result<Option<DateTime<Utc>>, ExecutionError> {
        loop {
            let mut tick_result = match manager.tick() {
                Ok(result) => result,
                Err(blocked) => break Ok(blocked.nearest_timer_expiration()),
            };

            let recovered_from_error =
                if self.drop_erroneous_messages && tick_result.can_drop_erroneous_message() {
                    tick_result.drop_erroneous_message();
                    true
                } else {
                    false
                };
            let event = match tick_result.into_inner() {
                Ok(receipt) => ExecutionResult::Ok(receipt),
                Err(err) => {
                    if recovered_from_error {
                        ExecutionResult::RolledBack(err)
                    } else {
                        return Err(err);
                    }
                }
            };
            if let Some(sx) = &self.results_sx {
                sx.unbounded_send(event).ok();
            }

            self.flush_outbound_messages(manager);
        }
    }

    /// Flushes messages to the outbound channels until no messages are left.
    fn flush_outbound_messages(&self, manager: &WorkflowManager) {
        for (&id, sx) in &self.outbound_channels {
            let (_, messages) = manager.take_outbound_messages(id);
            for message in messages {
                sx.unbounded_send(message.into()).ok();
                // ^ We don't care if outbound messages are no longer listened to.
            }
        }
    }

    /// Garbage-collect receivers for closed inbound channels. This will signal
    /// to the consumers that the channel cannot be written to.
    fn gc(&mut self, manager: &WorkflowManager) {
        self.inbound_channels
            .retain(|&id, _| !manager.channel_info(id).is_closed());
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

impl<T, C: Encode<T>> super::MessageSender<'_, T, C> {
    /// Registers this sender in `env`, allowing to later asynchronously send messages.
    pub fn into_async(self, env: &mut AsyncEnv) -> MessageSender<T, C> {
        let (sx, rx) = mpsc::channel(1);
        env.inbound_channels.insert(self.channel_id, rx);
        MessageSender {
            raw_sender: sx,
            codec: self.codec,
            _item: PhantomData,
        }
    }
}

impl<T, C: Encode<T>> Sink<T> for MessageSender<T, C> {
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

impl<T, C: Decode<T>> super::MessageReceiver<'_, T, C> {
    /// Registers this receiver in `env`, allowing to later asynchronously receive messages.
    pub fn into_async(self, env: &mut AsyncEnv) -> MessageReceiver<T, C> {
        let (sx, rx) = mpsc::unbounded();
        env.outbound_channels.insert(self.channel_id, sx);
        MessageReceiver {
            raw_receiver: rx,
            codec: self.codec,
            _item: PhantomData,
        }
    }
}

impl<T, C: Decode<T>> Stream for MessageReceiver<T, C> {
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

pin_project! {
    /// Async handle allowing to trace futures.
    ///
    /// This handle is a [`Stream`] emitting updated future states as the updates are received
    /// from the workflow.
    #[derive(Debug)]
    pub struct TracerHandle<C> {
        #[pin]
        receiver: MessageReceiver<FutureUpdate, C>,
        futures: TracedFutures,
    }
}

impl<C> TracerHandle<C> {
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
}

impl<C: Decode<FutureUpdate>> super::TracerHandle<'_, C> {
    /// Registers this tracer in `env`, allowing to later asynchronously receive traces.
    pub fn into_async(self, env: &mut AsyncEnv) -> TracerHandle<C> {
        TracerHandle {
            receiver: self.receiver.into_async(env),
            futures: self.futures,
        }
    }
}

impl<C: Decode<FutureUpdate>> Stream for TracerHandle<C> {
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
