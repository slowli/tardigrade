//! Persistence for `State`.

use std::{
    collections::{HashMap, HashSet},
    error, fmt,
};

use crate::{
    state::{State, TaskQueue, TaskState, WakerId},
    time::Timers,
    TaskId,
};

#[derive(Debug)]
#[non_exhaustive]
pub enum PersistError {
    /// There is a pending task.
    PendingTask,
    /// There is an non-flushed outbound message.
    PendingOutboundMessage {
        /// Name of the channel with the message.
        channel_name: String,
    },
}

impl fmt::Display for PersistError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("workflow cannot be persisted at this point: ")?;
        match self {
            Self::PendingTask => formatter.write_str("there is a pending task"),
            Self::PendingOutboundMessage { channel_name } => {
                write!(
                    formatter,
                    "there is an non-flushed outbound message on channel `{}`",
                    channel_name
                )
            }
        }
    }
}

impl error::Error for PersistError {}

#[derive(Debug)]
struct InboundChannelState {
    is_acquired: bool,
    received_messages: usize,
    wakes_on_next_element: HashSet<WakerId>,
}

impl InboundChannelState {
    fn new(state: &super::InboundChannelState) -> Self {
        debug_assert!(state.pending_message.is_none());
        // FIXME: is this constraint really ensured?

        Self {
            is_acquired: state.is_acquired,
            received_messages: state.received_messages,
            wakes_on_next_element: state.wakes_on_next_element.clone(),
        }
    }
}

impl From<InboundChannelState> for super::InboundChannelState {
    fn from(persisted: InboundChannelState) -> Self {
        Self {
            is_acquired: persisted.is_acquired,
            received_messages: persisted.received_messages,
            pending_message: None,
            wakes_on_next_element: persisted.wakes_on_next_element,
        }
    }
}

#[derive(Debug)]
struct OutboundChannelState {
    flushed_messages: usize,
    wakes_on_flush: HashSet<WakerId>,
}

impl OutboundChannelState {
    fn new(state: &super::OutboundChannelState, name: &str) -> Result<Self, PersistError> {
        if state.messages.is_empty() {
            Ok(Self {
                flushed_messages: state.flushed_messages,
                wakes_on_flush: state.wakes_on_flush.clone(),
            })
        } else {
            Err(PersistError::PendingOutboundMessage {
                channel_name: name.to_owned(),
            })
        }
    }
}

impl From<OutboundChannelState> for super::OutboundChannelState {
    fn from(persisted: OutboundChannelState) -> Self {
        Self {
            flushed_messages: persisted.flushed_messages,
            messages: Vec::new(),
            wakes_on_flush: persisted.wakes_on_flush,
        }
    }
}

/// `Workflow` state that can be persisted between workflow invocations.
#[derive(Debug)]
pub struct WorkflowState {
    inbound_channels: HashMap<String, InboundChannelState>,
    outbound_channels: HashMap<String, OutboundChannelState>,
    timers: Timers,
    data_inputs: HashMap<String, Vec<u8>>,
    tasks: HashMap<TaskId, TaskState>,
}

impl WorkflowState {
    fn convert_channels<T, U: From<T>>(channels: HashMap<String, T>) -> HashMap<String, U> {
        channels
            .into_iter()
            .map(|(name, channel_state)| (name, channel_state.into()))
            .collect()
    }
}

impl From<WorkflowState> for State {
    fn from(persisted: WorkflowState) -> Self {
        let inbound_channels = WorkflowState::convert_channels(persisted.inbound_channels);
        let outbound_channels = WorkflowState::convert_channels(persisted.outbound_channels);

        Self {
            exports: None,
            inbound_channels,
            outbound_channels,
            timers: persisted.timers,
            data_inputs: persisted.data_inputs,
            tasks: persisted.tasks,
            current_task: None,
            task_queue: TaskQueue::default(),
            waker_queue: Vec::new(),
            current_wakeup_cause: None,
        }
    }
}

impl State {
    pub fn persist(&self) -> Result<WorkflowState, PersistError> {
        // Check that we're not losing info.
        if self.current_task.is_some()
            || !self.task_queue.is_empty()
            || !self.waker_queue.is_empty()
        {
            return Err(PersistError::PendingTask);
        }

        let outbound_channels = self
            .outbound_channels
            .iter()
            .map(|(name, state)| {
                let persisted_state = OutboundChannelState::new(state, name)?;
                Ok((name.clone(), persisted_state))
            })
            .collect::<Result<_, PersistError>>()?;
        let inbound_channels = self
            .inbound_channels
            .iter()
            .map(|(name, state)| (name.clone(), InboundChannelState::new(state)))
            .collect();

        Ok(WorkflowState {
            inbound_channels,
            outbound_channels,
            timers: self.timers.clone(),
            data_inputs: self.data_inputs.clone(),
            tasks: self.tasks.clone(),
        })
    }
}
