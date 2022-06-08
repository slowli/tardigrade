//! Workflow environment.

use chrono::{DateTime, Utc};

use std::{cell::RefCell, collections::HashMap, marker::PhantomData, rc::Rc};

use super::{ConsumeError, Receipt, Workflow};
use crate::time::{TimerId, TimerState};
use tardigrade::{
    channel::{Receiver, Sender},
    Data, Decoder, Encoder,
};
use tardigrade_shared::workflow::{TakeHandle, WithHandle};

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
    pub fn send(&mut self, message: T) -> Result<Receipt, ConsumeError> {
        let raw_message = self.codec.encode_value(message);
        self.env
            .with(|workflow| workflow.consume_message(&self.channel_name, raw_message))
    }
}

impl<'a, T, C, W> WithHandle<WorkflowEnv<'a, W>> for Receiver<T, C>
where
    C: Encoder<T> + Default,
{
    type Handle = MessageSender<'a, T, C, W>;
}

impl<'a, T, C, W> TakeHandle<WorkflowEnv<'a, W>, &str> for Receiver<T, C>
where
    C: Encoder<T> + Default,
{
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
    pub fn flush_messages(&mut self) -> (Vec<T>, Receipt) {
        let (raw_messages, receipt) = self.env.with(|workflow| {
            let messages = workflow.take_outbound_messages(&self.channel_name);
            (messages, workflow.tick())
        });

        let messages = raw_messages
            .into_iter()
            .map(|message| self.codec.decode_bytes(message))
            .collect();
        (messages, receipt)
    }
}

impl<'a, T, C, W> WithHandle<WorkflowEnv<'a, W>> for Sender<T, C>
where
    C: Decoder<T> + Default,
{
    type Handle = MessageReceiver<'a, T, C, W>;
}

impl<'a, T, C, W> TakeHandle<WorkflowEnv<'a, W>, &str> for Sender<T, C>
where
    C: Decoder<T> + Default,
{
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

impl<'a, T, C, W> WithHandle<WorkflowEnv<'a, W>> for Data<T, C>
where
    C: Decoder<T> + Default,
{
    type Handle = DataPeeker<'a, T, C, W>;
}

impl<'a, T, C, W> TakeHandle<WorkflowEnv<'a, W>, &str> for Data<T, C>
where
    C: Decoder<T> + Default,
{
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
pub struct TimerHandle<'a, W> {
    env: WorkflowEnv<'a, W>,
}

impl<W> TimerHandle<'_, W> {
    pub fn current_time(&self) -> DateTime<Utc> {
        self.env.with(|workflow| workflow.current_time())
    }

    pub fn set_current_time(&mut self, time: DateTime<Utc>) -> Receipt {
        self.env.with(|workflow| {
            workflow.set_current_time(time);
            workflow.tick()
        })
    }

    pub fn timers(&self) -> HashMap<TimerId, TimerState> {
        self.env
            .with(|workflow| workflow.store.data().timers().inner().clone())
    }
}

#[non_exhaustive]
pub struct WorkflowHandle<'a, W>
where
    W: WithHandle<WorkflowEnv<'a, W>> + 'a,
{
    pub interface: <W as WithHandle<WorkflowEnv<'a, W>>>::Handle,
    pub timers: TimerHandle<'a, W>,
}

impl<'a, W> WorkflowHandle<'a, W>
where
    W: TakeHandle<WorkflowEnv<'a, W>, ()> + 'a,
{
    pub(super) fn new(workflow: &'a mut Workflow<W>) -> Self {
        let mut env = WorkflowEnv::new(workflow);
        Self {
            interface: W::take_handle(&mut env, ()),
            timers: TimerHandle { env },
        }
    }
}
