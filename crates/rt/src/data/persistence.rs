//! Persistence for `State`.

use anyhow::{anyhow, bail, ensure};

use std::{
    collections::{HashMap, HashSet},
    error, fmt,
};

use super::{
    helpers::Message,
    task::{TaskQueue, TaskState},
    time::Timers,
    WorkflowData,
};
use crate::{TaskId, WakerId};
use tardigrade_shared::workflow::Interface;

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

    fn restore<W>(
        self,
        interface: &Interface<W>,
        channel_name: &str,
    ) -> anyhow::Result<super::InboundChannelState> {
        interface.inbound_channel(channel_name).ok_or_else(|| {
            anyhow!(
                "inbound channel `{}` is present in persisted state, but not \
                 in workflow interface",
                channel_name
            )
        })?;

        Ok(super::InboundChannelState {
            is_acquired: self.is_acquired,
            received_messages: self.received_messages,
            pending_message: None,
            wakes_on_next_element: self.wakes_on_next_element,
        })
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

    fn restore<W>(
        self,
        interface: &Interface<W>,
        channel_name: &str,
    ) -> anyhow::Result<super::OutboundChannelState> {
        let spec = interface.outbound_channel(channel_name).ok_or_else(|| {
            anyhow!(
                "outbound channel `{}` is present in persisted state, but not \
                 in workflow interface",
                channel_name
            )
        })?;

        Ok(super::OutboundChannelState {
            capacity: spec.capacity,
            flushed_messages: self.flushed_messages,
            messages: Vec::new(),
            wakes_on_flush: self.wakes_on_flush,
        })
    }
}

/// `Workflow` state that can be persisted between workflow invocations.
#[derive(Debug)]
pub(crate) struct WorkflowState {
    inbound_channels: HashMap<String, InboundChannelState>,
    outbound_channels: HashMap<String, OutboundChannelState>,
    timers: Timers,
    data_inputs: HashMap<String, Message>,
    tasks: HashMap<TaskId, TaskState>,
}

impl WorkflowState {
    pub fn restore<W>(self, interface: &Interface<W>) -> anyhow::Result<WorkflowData> {
        let maybe_missing_data_input = interface
            .data_inputs()
            .find(|&(name, _)| !self.data_inputs.contains_key(name));
        if let Some((name, _)) = maybe_missing_data_input {
            bail!(
                "data input `{}` is present in persisted state, but not in workflow interface",
                name
            );
        }
        let data_inputs_len = interface.data_inputs().len();
        ensure!(
            data_inputs_len == self.data_inputs.len(),
            "mismatch between number of data inputs in workflow interface ({}) \
             and in persisted state ({})",
            data_inputs_len,
            self.data_inputs.len()
        );

        let inbound_channels_len = interface.inbound_channels().len();
        ensure!(
            inbound_channels_len == self.inbound_channels.len(),
            "mismatch between number of inbound channels in workflow interface ({}) \
             and in persisted state ({})",
            inbound_channels_len,
            self.inbound_channels.len()
        );
        let inbound_channels = self.inbound_channels.into_iter().map(|(name, state)| {
            let restored = state.restore(interface, &name)?;
            Ok((name, restored))
        });
        let inbound_channels = inbound_channels.collect::<anyhow::Result<_>>()?;

        let outbound_channels_len = interface.outbound_channels().len();
        ensure!(
            outbound_channels_len == self.outbound_channels.len(),
            "mismatch between number of outbound channels in workflow interface ({}) \
             and in persisted state ({})",
            outbound_channels_len,
            self.outbound_channels.len()
        );
        let outbound_channels = self.outbound_channels.into_iter().map(|(name, state)| {
            let restored = state.restore(interface, &name)?;
            Ok((name, restored))
        });
        let outbound_channels = outbound_channels.collect::<anyhow::Result<_>>()?;

        Ok(WorkflowData {
            exports: None,
            inbound_channels,
            outbound_channels,
            timers: self.timers,
            data_inputs: self.data_inputs,
            tasks: self.tasks,
            current_execution: None,
            task_queue: TaskQueue::default(),
            waker_queue: Vec::new(),
            current_wakeup_cause: None,
        })
    }
}

impl WorkflowData {
    pub(crate) fn persist(&self) -> Result<WorkflowState, PersistError> {
        // Check that we're not losing info.
        if self.current_execution.is_some()
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
