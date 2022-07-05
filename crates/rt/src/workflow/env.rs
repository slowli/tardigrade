//! Workflow environment.

// FIXME: state of traced futures is lost across saves!

use std::{cell::RefCell, marker::PhantomData, ops::Range, rc::Rc};

use crate::{
    receipt::{ExecutionError, Receipt},
    ConsumeError, FutureId, Workflow,
};
use tardigrade::{
    channel::{Receiver, Sender},
    trace::{FutureUpdate, TracedFuture, TracedFutures, Tracer},
    workflow::TakeHandle,
    Data, Decoder, Encoder,
};

#[derive(Debug)]
pub struct WorkflowEnv<'a, W> {
    inner: Rc<RefCell<&'a mut Workflow<W>>>,
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

#[derive(Debug)]
pub struct MessageSender<'a, T, C, W> {
    env: WorkflowEnv<'a, W>,
    channel_name: String,
    codec: C,
    _item: PhantomData<*const T>,
}

impl<T, C: Encoder<T>, W> MessageSender<'_, T, C, W> {
    pub fn send(&mut self, message: T) -> Result<(), ConsumeError> {
        let raw_message = self.codec.encode_value(message);
        self.env
            .with(|workflow| workflow.push_inbound_message(&self.channel_name, raw_message))
    }
}

// FIXME: Use additional struct to tick afterwards: `sx.send(message)?.flush()?`

impl<'a, T, C, W> TakeHandle<WorkflowEnv<'a, W>> for Receiver<T, C>
where
    C: Encoder<T> + Default,
{
    type Id = str;
    type Handle = MessageSender<'a, T, C, W>;

    // FIXME: check that channel exists
    fn take_handle(env: &mut WorkflowEnv<'a, W>, id: &str) -> Self::Handle {
        MessageSender {
            env: env.clone(),
            channel_name: id.to_owned(),
            codec: C::default(),
            _item: PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct MessageReceiver<'a, T, C, W> {
    env: WorkflowEnv<'a, W>,
    channel_name: String,
    codec: C,
    _item: PhantomData<fn() -> T>,
}

impl<T, C: Decoder<T>, W> MessageReceiver<'_, T, C, W> {
    pub fn message_indices(&self) -> Range<usize> {
        self.env.with(|workflow| {
            workflow
                .store
                .data()
                .outbound_message_indices(&self.channel_name)
        })
    }

    pub fn is_empty(&self) -> bool {
        self.message_indices().is_empty()
    }

    pub fn flush_messages(&mut self) -> Result<Receipt<Vec<T>>, ExecutionError> {
        let (raw_messages, exec_result) = self.env.with(|workflow| {
            let messages = workflow.take_outbound_messages(&self.channel_name);
            (messages, workflow.tick())
        });

        // FIXME: use fallible decoding
        let messages = raw_messages
            .into_iter()
            .map(|message| self.codec.decode_bytes(message))
            .collect();
        exec_result.map(|receipt| receipt.map(|()| messages))
    }
}

impl<'a, T, C, W> TakeHandle<WorkflowEnv<'a, W>> for Sender<T, C>
where
    C: Decoder<T> + Default,
{
    type Id = str;
    type Handle = MessageReceiver<'a, T, C, W>;

    fn take_handle(env: &mut WorkflowEnv<'a, W>, id: &str) -> Self::Handle {
        MessageReceiver {
            env: env.clone(),
            channel_name: id.to_owned(),
            codec: C::default(),
            _item: PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct DataPeeker<'a, T, C, W> {
    env: WorkflowEnv<'a, W>,
    input_name: String,
    codec: C,
    _item: PhantomData<fn() -> T>,
}

impl<T, C: Decoder<T>, W> DataPeeker<'_, T, C, W> {
    pub fn peek(&mut self) -> T {
        let raw_input = self
            .env
            .with(|workflow| workflow.data_input(&self.input_name))
            .unwrap();
        self.codec.decode_bytes(raw_input)
    }
}

impl<'a, T, C, W> TakeHandle<WorkflowEnv<'a, W>> for Data<T, C>
where
    C: Decoder<T> + Default,
{
    type Id = str;
    type Handle = DataPeeker<'a, T, C, W>;

    fn take_handle(env: &mut WorkflowEnv<'a, W>, id: &str) -> Self::Handle {
        DataPeeker {
            env: env.clone(),
            input_name: id.to_owned(),
            codec: C::default(),
            _item: PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct TracerHandle<'a, C, W> {
    receiver: MessageReceiver<'a, FutureUpdate, C, W>,
    futures: TracedFutures,
}

impl<'a, C, W> TracerHandle<'a, C, W>
where
    C: Decoder<FutureUpdate>,
{
    pub fn future(&self, id: FutureId) -> Option<&TracedFuture> {
        self.futures.get(&id)
    }

    pub fn futures(&self) -> impl Iterator<Item = (FutureId, &TracedFuture)> + '_ {
        self.futures.iter().map(|(id, state)| (*id, state))
    }

    pub fn flush(&mut self) -> Result<Receipt, ExecutionError> {
        let receipt = self.receiver.flush_messages()?;
        let receipt = receipt.map(|updates| {
            for update in updates {
                if let Err(err) = TracedFuture::update(&mut self.futures, update) {
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

    fn take_handle(env: &mut WorkflowEnv<'a, W>, id: &str) -> Self::Handle {
        TracerHandle {
            receiver: Sender::<FutureUpdate, C>::take_handle(env, id),
            futures: TracedFutures::new(),
        }
    }
}

pub struct WorkflowHandle<'a, W>
where
    W: TakeHandle<WorkflowEnv<'a, W>, Id = ()> + 'a,
{
    pub interface: <W as TakeHandle<WorkflowEnv<'a, W>>>::Handle,
    env: WorkflowEnv<'a, W>,
}

impl<'a, W> WorkflowHandle<'a, W>
where
    W: TakeHandle<WorkflowEnv<'a, W>, Id = ()> + 'a,
{
    pub(super) fn new(workflow: &'a mut Workflow<W>) -> Self {
        let mut env = WorkflowEnv::new(workflow);
        Self {
            interface: W::take_handle(&mut env, &()),
            env,
        }
    }

    pub fn with<T>(&mut self, action: impl FnOnce(&mut Workflow<W>) -> T) -> T {
        self.env.with(action)
    }
}
