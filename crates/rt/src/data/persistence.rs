//! Persistence for `State`.

use anyhow::{anyhow, bail, ensure};
use serde::{Deserialize, Serialize};

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
use crate::{module::Services, TaskId, WakerId};
use tardigrade::interface::Interface;

/// Error persisting a [`Workflow`](crate::Workflow).
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

#[allow(clippy::trivially_copy_pass_by_ref)] // required by serde
fn flip_bool(&flag: &bool) -> bool {
    !flag
}

#[derive(Debug, Serialize, Deserialize)]
struct InboundChannelState {
    #[serde(default, skip_serializing_if = "flip_bool")]
    is_closed: bool,
    is_acquired: bool,
    received_messages: usize,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    wakes_on_next_element: HashSet<WakerId>,
}

impl InboundChannelState {
    fn new(state: &super::InboundChannelState) -> Self {
        debug_assert!(state.pending_message.is_none());
        Self {
            is_closed: state.is_closed,
            is_acquired: state.is_acquired,
            received_messages: state.received_messages,
            wakes_on_next_element: state.wakes_on_next_element.clone(),
        }
    }

    fn restore(
        self,
        interface: &Interface<()>,
        channel_name: &str,
    ) -> anyhow::Result<super::InboundChannelState> {
        interface.inbound_channel(channel_name).ok_or_else(|| {
            anyhow!(
                "inbound channel `{}` is present in persisted state, but not \
                 in workflow interface",
                channel_name
            )
        })?;

        Ok(self.into())
    }
}

impl From<InboundChannelState> for super::InboundChannelState {
    fn from(persisted: InboundChannelState) -> Self {
        Self {
            is_closed: persisted.is_closed,
            is_acquired: persisted.is_acquired,
            received_messages: persisted.received_messages,
            pending_message: None,
            wakes_on_next_element: persisted.wakes_on_next_element,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct OutboundChannelState {
    flushed_messages: usize,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    wakes_on_flush: HashSet<WakerId>,
}

impl OutboundChannelState {
    fn new(state: &super::OutboundChannelState) -> Self {
        debug_assert!(state.messages.is_empty());
        Self {
            flushed_messages: state.flushed_messages,
            wakes_on_flush: state.wakes_on_flush.clone(),
        }
    }

    fn restore(
        self,
        interface: &Interface<()>,
        channel_name: &str,
    ) -> anyhow::Result<super::OutboundChannelState> {
        interface.outbound_channel(channel_name).ok_or_else(|| {
            anyhow!(
                "outbound channel `{}` is present in persisted state, but not \
                 in workflow interface",
                channel_name
            )
        })?;
        Ok(self.into())
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
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WorkflowState {
    inbound_channels: HashMap<String, InboundChannelState>,
    outbound_channels: HashMap<String, OutboundChannelState>,
    timers: Timers,
    data_inputs: HashMap<String, Message>,
    tasks: HashMap<TaskId, TaskState>,
}

impl WorkflowState {
    pub fn restore(
        self,
        interface: Interface<()>,
        services: Services,
    ) -> anyhow::Result<WorkflowData> {
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
            let restored = state.restore(&interface, &name)?;
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
            let restored = state.restore(&interface, &name)?;
            Ok((name, restored))
        });
        let outbound_channels = outbound_channels.collect::<anyhow::Result<_>>()?;

        Ok(WorkflowData {
            exports: None,
            interface,
            inbound_channels,
            outbound_channels,
            timers: self.timers,
            services,
            data_inputs: self.data_inputs,
            tasks: self.tasks,
            current_execution: None,
            task_queue: TaskQueue::default(),
            waker_queue: Vec::new(),
            current_wakeup_cause: None,
        })
    }

    // NB. Should agree with logic in `Self::restore()`.
    pub fn restore_in_place(self, data: &mut WorkflowData) {
        data.inbound_channels = self
            .inbound_channels
            .into_iter()
            .map(|(name, state)| (name, state.into()))
            .collect();
        data.outbound_channels = self
            .outbound_channels
            .into_iter()
            .map(|(name, state)| (name, state.into()))
            .collect();

        data.timers = self.timers;
        data.tasks = self.tasks;
        data.data_inputs = self.data_inputs;
        data.current_execution = None;
        data.task_queue = TaskQueue::default();
        data.waker_queue = Vec::new();
        data.current_wakeup_cause = None;
    }
}

impl WorkflowData {
    pub(crate) fn check_persistence(&self) -> Result<(), PersistError> {
        // Check that we're not losing info.
        if self.current_execution.is_some()
            || !self.task_queue.is_empty()
            || !self.waker_queue.is_empty()
        {
            return Err(PersistError::PendingTask);
        }

        for (name, channel) in &self.outbound_channels {
            if !channel.messages.is_empty() {
                return Err(PersistError::PendingOutboundMessage {
                    channel_name: name.clone(),
                });
            }
        }
        Ok(())
    }

    // Must be preceded with `Self::check_persistence()`.
    pub(crate) fn persist(&self) -> WorkflowState {
        let outbound_channels = self
            .outbound_channels
            .iter()
            .map(|(name, state)| {
                let persisted_state = OutboundChannelState::new(state);
                (name.clone(), persisted_state)
            })
            .collect();
        let inbound_channels = self
            .inbound_channels
            .iter()
            .map(|(name, state)| (name.clone(), InboundChannelState::new(state)))
            .collect();

        WorkflowState {
            inbound_channels,
            outbound_channels,
            timers: self.timers.clone(),
            data_inputs: self.data_inputs.clone(),
            tasks: self.tasks.clone(),
        }
    }
}
