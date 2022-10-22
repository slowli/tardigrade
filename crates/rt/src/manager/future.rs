//! Async handles for workflows.

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
    manager::{TickResult, WorkflowManager},
    receipt::ExecutionError,
};
use tardigrade::{ChannelId, Decode, Encode};

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
        (**self).create_timer(expires_at)
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

/// Terminal status of a [`WorkflowManager`].
#[derive(Debug)]
#[non_exhaustive]
pub enum Termination {
    /// The workflow manager is finished: all workflows managed by it have run to completion.
    Finished,
    /// The manager has stalled: its progress does not depend on any external futures
    /// (inbound messages or timers).
    Stalled,
}

/// Asynchronous environment for driving workflows in a [`WorkflowManager`].
///
/// # Error handling
///
/// By default, workflow execution via [`Self::run()`] terminates immediately after a trap.
/// To drop incoming messages that have led to an error, call [`Self::drop_erroneous_messages()`].
/// Rolling back receiving a message
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
/// # use tardigrade::WorkflowId;
/// use tardigrade_rt::manager::{
///     future::{AsyncEnv, AsyncIoScheduler}, WorkflowHandle, WorkflowManager,
/// };
///
/// # async fn test_wrapper(
/// #     manager: WorkflowManager,
/// #     workflow_id: WorkflowId,
/// # ) -> anyhow::Result<()> {
/// // Assume we have a dynamically typed workflow:
/// let mut manager: WorkflowManager = // ...
/// #   manager;
/// let mut workflow = manager.workflow(workflow_id).unwrap();
///
/// // First, create an environment to execute the workflow in.
/// let mut env = AsyncEnv::new(AsyncIoScheduler);
/// // Take relevant channels from the workflow and convert them to async form.
/// let mut handle = workflow.handle();
/// let mut commands_sx = handle.remove(InboundChannel("commands"))
///     .unwrap()
///     .into_async(&mut env);
/// let events_rx = handle.remove(OutboundChannel("events"))
///     .unwrap()
///     .into_async(&mut env);
///
/// // Run the environment in a separate task.
/// task::spawn(async move { env.run(&mut manager).await });
/// // Let's send a message via an inbound channel...
/// let message = b"hello".to_vec();
/// commands_sx.send(message).await?;
///
/// // ...and wait for some outbound messages
/// let events: Vec<Vec<u8>> = events_rx.take(2).try_collect().await.unwrap();
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
    results_sx: Option<mpsc::UnboundedSender<TickResult<()>>>,
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

    /// Returns the receiver of [`TickResult`]s generated during workflow execution.
    pub fn tick_results(&mut self) -> mpsc::UnboundedReceiver<TickResult<()>> {
        let (sx, rx) = mpsc::unbounded();
        self.results_sx = Some(sx);
        rx
    }

    /// Indicates that the environment should drop any inbound messages that lead
    /// to execution errors.
    pub fn drop_erroneous_messages(&mut self) {
        self.drop_erroneous_messages = true;
    }

    /// Executes the enclosed [`WorkflowManager`] until all workflows in it are terminated,
    /// or an execution error occurs. As the workflows execute, outbound messages and
    /// [`TickResult`]s will be sent using respective channels.
    ///
    /// Note that it is possible to cancel this future (e.g., by [`select`]ing between it
    /// and a cancellation signal) and continue working with the enclosed workflow manager.
    ///
    /// # Errors
    ///
    /// Returns an error if workflow execution traps and is not rolled back due to
    /// [`Self::drop_erroneous_messages()`] flag being set.
    ///
    /// [`select`]: futures::select
    pub async fn run(
        &mut self,
        manager: &mut WorkflowManager,
    ) -> Result<Termination, ExecutionError> {
        loop {
            if let Some(termination) = self.tick(manager).await? {
                self.flush_outbound_messages(manager);
                return Ok(termination);
            }
        }
    }

    async fn tick(
        &mut self,
        manager: &mut WorkflowManager,
    ) -> Result<Option<Termination>, ExecutionError> {
        if manager.is_empty() {
            return Ok(Some(Termination::Finished));
        }

        let nearest_timer_expiration = self.tick_manager(manager)?;
        self.gc(manager);
        if manager.is_empty() {
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
                    manager.close_host_sender(id);
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
            let tick_result = match manager.tick() {
                Ok(result) => result,
                Err(blocked) => break Ok(blocked.nearest_timer_expiration()),
            };

            let (tick_result, recovered_from_error) =
                if self.drop_erroneous_messages && tick_result.can_drop_erroneous_message() {
                    (tick_result.drop_erroneous_message(), true)
                } else {
                    (tick_result.drop_extra(), false)
                };

            if tick_result.as_ref().is_err() && !recovered_from_error {
                return Err(tick_result.into_inner().unwrap_err());
            } else if let Some(sx) = &self.results_sx {
                sx.unbounded_send(tick_result).ok();
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
            .retain(|&id, _| !manager.channel(id).unwrap().is_closed());
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
        env.inbound_channels.insert(self.channel_id(), rx);
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
        if self.can_receive_messages {
            env.outbound_channels.insert(self.channel_id(), sx);
            // If the channel cannot receive messages, `sx` is immediately dropped,
            // thus, we don't improperly remove messages from the channel.
        }
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
