//! Async handles for [`Workflow`].

#![allow(missing_docs)]

use chrono::{DateTime, Utc};
use futures::{channel::mpsc, future, FutureExt, Sink, SinkExt, Stream, StreamExt};
use pin_project_lite::pin_project;

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::{Instant, SystemTime},
};

use crate::{
    receipt::{ExecutionError, Receipt},
    workflow::Workflow,
};
use tardigrade::{
    channel::{Receiver, Sender},
    interface::{AccessError, AccessErrorKind, DataInput, InboundChannel, OutboundChannel},
    trace::Tracer,
    workflow::TakeHandle,
    Data, Decoder, Encoder,
};

pub type ClockFuture = Pin<Box<dyn Future<Output = DateTime<Utc>> + Send>>;

pub trait Schedule: Send + 'static {
    fn create_timer(&mut self, timestamp: DateTime<Utc>) -> ClockFuture;
}

impl fmt::Debug for dyn Schedule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Clock").finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct AsyncIoScheduler;

impl Schedule for AsyncIoScheduler {
    fn create_timer(&mut self, timestamp: DateTime<Utc>) -> ClockFuture {
        use async_io::Timer;

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
pub struct AsyncEnv<W> {
    workflow: Workflow<W>,
    scheduler: Box<dyn Schedule>,
    inbound_channels: HashMap<String, mpsc::Receiver<Vec<u8>>>,
    outbound_channels: HashMap<String, mpsc::UnboundedSender<Vec<u8>>>,
    receipts: Option<mpsc::Sender<Receipt>>,
}

#[derive(Debug)]
enum ListenedEventOutput<'a> {
    Channel {
        name: &'a str,
        message: Option<Vec<u8>>,
    },
    Timer(DateTime<Utc>),
}

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
    pub fn new(workflow: Workflow<W>, clock: impl Schedule) -> Self {
        Self {
            workflow,
            scheduler: Box::new(clock),
            inbound_channels: HashMap::new(),
            outbound_channels: HashMap::new(),
            receipts: None,
        }
    }

    /// # Errors
    ///
    /// Returns an error if workflow execution traps.
    pub async fn run(mut self) -> Result<Termination, ExecutionError> {
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

        // First, flush all outbound messages ("almost" sync and cheap).
        self.flush_outbound_messages().await?;

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

        let timer_event = events.nearest_timer.map(|timestamp| {
            let timer = self.scheduler.create_timer(timestamp);
            timer.map(ListenedEventOutput::Timer).right_future()
        });
        let all_futures = channel_futures.chain(timer_event);

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
        self.send_receipt(receipt).await;

        Ok(None)
    }

    /// Repeatedly flushes messages to the outbound channels until no messages are left.
    async fn flush_outbound_messages(&mut self) -> Result<(), ExecutionError> {
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
                self.send_receipt(receipt).await;
            } else {
                break Ok(());
            }
        }
    }

    async fn send_receipt(&mut self, receipt: Receipt) {
        if let Some(receipts) = &mut self.receipts {
            receipts.send(receipt).await.ok();
            // ^ We don't care if nobody listens to receipts.
        }
    }
}

impl<W> AsyncEnv<W>
where
    W: TakeHandle<AsyncEnv<W>, Id = ()>,
{
    #[allow(clippy::missing_panics_doc)] // TODO: is `unwrap()` really safe here?
    pub fn handle(&mut self) -> W::Handle {
        W::take_handle(self, &()).unwrap()
    }
}

pin_project! {
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
            Err(AccessErrorKind::Unknown.for_handle(InboundChannel(id)))
        }
    }
}

pin_project! {
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
            Err(AccessErrorKind::Unknown.for_handle(OutboundChannel(id)))
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
                .map_err(|err| AccessErrorKind::Custom(Box::new(err)).for_handle(DataInput(id)))
        } else {
            Err(AccessErrorKind::Unknown.for_handle(DataInput(id)))
        }
    }
}

// FIXME
impl<C, W> TakeHandle<AsyncEnv<W>> for Tracer<C> {
    type Id = str;
    type Handle = ();

    fn take_handle(_env: &mut AsyncEnv<W>, _id: &str) -> Result<Self::Handle, AccessError> {
        Ok(())
    }
}
