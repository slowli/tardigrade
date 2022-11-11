//! Async handles for workflows.

use chrono::{DateTime, Utc};
use futures::{channel::mpsc, future, FutureExt, Sink, Stream, StreamExt};
use pin_project_lite::pin_project;

use std::{
    collections::HashMap,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    manager::{self, AsManager, TickResult},
    receipt::ExecutionError,
    storage::{MessageError, ReadChannels, Storage},
    Schedule,
};
use tardigrade::{ChannelId, Decode, Encode};

#[derive(Debug)]
enum ListenedEventOutput {
    Channel {
        id: ChannelId,
        message: Option<Vec<u8>>,
    },
    Timer(DateTime<Utc>),
}

#[derive(Debug)]
struct OutboundChannel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    cursor: usize,
}

impl OutboundChannel {
    /// Returns `true` if the channel should be removed (e.g., if the EOF marker is reached,
    /// or if it's not listened to by the client.
    async fn relay_messages(&mut self, id: ChannelId, transaction: &impl ReadChannels) -> bool {
        loop {
            match transaction.channel_message(id, self.cursor).await {
                Ok(message) => {
                    self.cursor += 1;
                    if self.sender.unbounded_send(message).is_err() {
                        break true;
                    }
                }
                Err(MessageError::NonExistingIndex { is_closed }) => break is_closed,
                Err(err) => {
                    tracing::warn!(%err, "unexpected error when relaying messages");
                    break true;
                }
            }
        }
    }
}

/// Terminal status of a [`WorkflowManager`].
///
/// [`WorkflowManager`]: crate::manager::WorkflowManager
#[derive(Debug)]
#[non_exhaustive]
pub enum Termination {
    /// The workflow manager is finished: all workflows managed by it have run to completion.
    Finished,
    /// The manager has stalled: its progress does not depend on any external futures
    /// (inbound messages or timers).
    Stalled,
}

/// Environment for driving workflow execution in a [`WorkflowManager`].
///
/// [`WorkflowManager`]: crate::manager::WorkflowManager
///
/// # Error handling
///
/// By default, workflow execution via [`Self::drive()`] terminates immediately after a trap.
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
/// use tardigrade_rt::{driver::Driver, manager::WorkflowManager, AsyncIoScheduler};
/// # use tardigrade_rt::storage::LocalStorage;
///
/// # async fn test_wrapper(
/// #     manager: WorkflowManager<AsyncIoScheduler, LocalStorage>,
/// #     workflow_id: WorkflowId,
/// # ) -> anyhow::Result<()> {
/// // Assume we have a dynamically typed workflow:
/// let mut manager: WorkflowManager<AsyncIoScheduler, _> = // ...
/// #   manager;
/// let mut workflow = manager.workflow(workflow_id).await.unwrap();
///
/// // First, create a driver to execute the workflow in.
/// let mut driver = Driver::new();
/// // Take relevant channels from the workflow and convert them to async form.
/// let mut handle = workflow.handle();
/// let mut commands_sx = handle.remove(InboundChannel("commands"))
///     .unwrap()
///     .into_sink(&mut driver);
/// let events_rx = handle.remove(OutboundChannel("events"))
///     .unwrap()
///     .into_stream(&mut driver);
///
/// // Run the environment in a separate task.
/// task::spawn(async move { driver.drive(&mut manager).await });
/// // Let's send a message via an inbound channel...
/// let message = b"hello".to_vec();
/// commands_sx.send(message).await?;
///
/// // ...and wait for some outbound messages
/// let events: Vec<Vec<u8>> = events_rx.take(2).try_collect().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default)]
pub struct Driver {
    inbound_channels: HashMap<ChannelId, mpsc::Receiver<Vec<u8>>>,
    outbound_channels: HashMap<ChannelId, OutboundChannel>,
    results_sx: Option<mpsc::UnboundedSender<TickResult<()>>>,
    drop_erroneous_messages: bool,
}

impl Driver {
    /// Creates an async environment for a `workflow` that uses the specified `scheduler`
    /// for timers.
    pub fn new() -> Self {
        Self::default()
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
    /// [`WorkflowManager`]: crate::manager::WorkflowManager
    /// [`select`]: futures::select
    pub async fn drive<M>(&mut self, manager: &mut M) -> Result<Termination, ExecutionError>
    where
        M: AsManager,
        M::Clock: Schedule,
    {
        loop {
            if let Some(termination) = self.tick(manager).await? {
                self.flush_outbound_messages(manager).await;
                return Ok(termination);
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn tick<M>(&mut self, manager: &M) -> Result<Option<Termination>, ExecutionError>
    where
        M: AsManager,
        M::Clock: Schedule,
    {
        let manager_ref = manager.as_manager();
        let nearest_timer_expiration = self.tick_manager(manager).await?;
        self.gc(manager).await;
        if nearest_timer_expiration.is_none() && self.inbound_channels.is_empty() {
            let termination = if manager_ref.workflow_count().await == 0 {
                Termination::Finished
            } else {
                Termination::Stalled
            };
            return Ok(Some(termination));
        }

        // Determine external events listened by the workflow.
        let channel_futures = self.inbound_channels.iter_mut().map(|(&id, rx)| {
            rx.next()
                .map(move |message| ListenedEventOutput::Channel { id, message })
                .left_future()
        });

        // TODO: cache `timer_event`?
        let timer_event = nearest_timer_expiration.map(|timestamp| {
            let timer = manager_ref.clock.create_timer(timestamp);
            timer.map(ListenedEventOutput::Timer).right_future()
        });
        let all_futures = channel_futures.chain(timer_event);

        let (output, ..) = future::select_all(all_futures).await;
        match output {
            ListenedEventOutput::Channel { id, message } => {
                if let Some(message) = message {
                    manager_ref.send_message(id, message).await.unwrap();
                } else {
                    manager_ref.close_host_sender(id).await;
                }
            }

            ListenedEventOutput::Timer(timestamp) => {
                manager_ref.set_current_time(timestamp).await;
            }
        };

        Ok(None)
    }

    async fn tick_manager<M: AsManager>(
        &mut self,
        manager: &M,
    ) -> Result<Option<DateTime<Utc>>, ExecutionError> {
        loop {
            let tick_result = match manager.as_manager().tick().await {
                Ok(result) => result,
                Err(blocked) => break Ok(blocked.nearest_timer_expiration()),
            };

            let (tick_result, recovered_from_error) =
                if self.drop_erroneous_messages && tick_result.can_drop_erroneous_message() {
                    (tick_result.drop_erroneous_message().await, true)
                } else {
                    (tick_result.drop_extra(), false)
                };

            if tick_result.as_ref().is_err() && !recovered_from_error {
                return Err(tick_result.into_inner().unwrap_err());
            } else if let Some(sx) = &self.results_sx {
                sx.unbounded_send(tick_result).ok();
            }

            self.flush_outbound_messages(manager).await;
        }
    }

    /// Flushes messages to the outbound channels until no messages are left.
    async fn flush_outbound_messages<M: AsManager>(&mut self, manager: &M) {
        let manager = manager.as_manager();
        let transaction = &manager.storage.readonly_transaction().await;

        let channels = self.outbound_channels.iter_mut();
        let channel_tasks = channels.map(|(&id, channel)| async move {
            let should_drop = channel.relay_messages(id, transaction).await;
            Some(id).filter(|_| should_drop)
        });
        let dropped_channels = future::join_all(channel_tasks).await;
        for id in dropped_channels.into_iter().flatten() {
            self.outbound_channels.remove(&id);
        }
    }

    /// Garbage-collect receivers for closed inbound channels. This will signal
    /// to the consumers that the channel cannot be written to.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn gc<M: AsManager>(&mut self, manager: &M) {
        let manager = manager.as_manager();
        let channel_ids = self.inbound_channels.keys();
        let check_closed_tasks = channel_ids.map(|&id| async move {
            let is_closed = manager
                .channel(id)
                .await
                .map_or(true, |channel| channel.is_closed);
            Some(id).filter(|_| is_closed)
        });
        let closed_channels = future::join_all(check_closed_tasks).await;

        for id in closed_channels.into_iter().flatten() {
            self.inbound_channels.remove(&id);
            tracing::debug!(id, "removed closed inbound channel");
        }
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

impl<T, C: Encode<T>, M: AsManager> manager::MessageSender<'_, T, C, M> {
    /// Registers this sender in `driver`, allowing to later asynchronously send messages.
    pub fn into_sink(self, driver: &mut Driver) -> MessageSender<T, C> {
        let (sx, rx) = mpsc::channel(1);
        driver.inbound_channels.insert(self.channel_id(), rx);
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

impl<T, C: Decode<T> + Default, M: AsManager> manager::MessageReceiver<'_, T, C, M> {
    /// Registers this receiver in `driver`, allowing to later asynchronously receive messages.
    pub fn into_stream(self, driver: &mut Driver) -> MessageReceiver<T, C> {
        let (sx, rx) = mpsc::unbounded();
        let state = OutboundChannel {
            cursor: 0,
            sender: sx,
        };
        driver.outbound_channels.insert(self.channel_id(), state);
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
