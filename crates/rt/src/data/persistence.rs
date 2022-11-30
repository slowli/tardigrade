//! Persistence for `State`.

use anyhow::anyhow;
use chrono::{DateTime, Utc};

use std::{collections::HashMap, error, fmt};

use super::{
    channel::{ChannelStates, ReceiverState, SenderState},
    spawn::ChildWorkflowStubs,
    task::TaskQueue,
    time::Timers,
    PersistedWorkflowData, WorkflowData,
};
use crate::{manager::Services, workflow::ChannelIds};
use tardigrade::{
    interface::{ChannelHalf, HandlePathBuf, Interface},
    ChannelId,
};

/// Error persisting a workflow.
#[derive(Debug)]
pub(crate) enum PersistError {
    /// There is a pending task.
    PendingTask,
    /// There is an non-flushed / non-consumed message.
    PendingMessage {
        /// Kind of the channel involved.
        channel_kind: ChannelHalf,
        /// ID of the channel with the message.
        channel_id: ChannelId,
    },
}

impl fmt::Display for PersistError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("workflow cannot be persisted at this point: ")?;
        match self {
            Self::PendingTask => formatter.write_str("there is a pending task"),

            Self::PendingMessage {
                channel_kind,
                channel_id,
            } => {
                write!(
                    formatter,
                    "there is an non-flushed {channel_kind} message on channel {channel_id}"
                )
            }
        }
    }
}

impl error::Error for PersistError {}

impl ReceiverState {
    fn check_on_restore(interface: &Interface, path: &HandlePathBuf) -> anyhow::Result<()> {
        interface.receiver(path).ok_or_else(|| {
            anyhow!(
                "receiver `{path}` is present in persisted state, but not \
                 in workflow interface"
            )
        })?;
        Ok(())
    }
}

impl SenderState {
    fn check_on_restore(interface: &Interface, path: &HandlePathBuf) -> anyhow::Result<()> {
        interface.sender(path).ok_or_else(|| {
            anyhow!(
                "sender `{path}` is present in persisted state, but not \
                 in workflow interface"
            )
        })?;
        Ok(())
    }
}

impl ChannelStates {
    fn check_on_restore(&self, interface: &Interface) -> anyhow::Result<()> {
        for path in self.mapping.receiver_paths() {
            ReceiverState::check_on_restore(interface, path)?;
        }
        for path in self.mapping.sender_paths() {
            SenderState::check_on_restore(interface, path)?;
        }
        Ok(())
    }
}

impl PersistedWorkflowData {
    pub(super) fn new(interface: &Interface, channel_ids: ChannelIds, now: DateTime<Utc>) -> Self {
        Self {
            channels: ChannelStates::new(channel_ids, interface),
            timers: Timers::new(now),
            tasks: HashMap::new(),
            child_workflows: HashMap::new(),
            child_workflow_stubs: ChildWorkflowStubs::default(),
            waker_queue: Vec::new(),
        }
    }

    pub fn restore(
        self,
        interface: &Interface,
        services: Services,
    ) -> anyhow::Result<WorkflowData> {
        self.channels.check_on_restore(interface)?;
        Ok(WorkflowData {
            persisted: self,
            services: Some(services),
            current_execution: None,
            task_queue: TaskQueue::default(),
            current_wakeup_cause: None,
        })
    }
}

impl WorkflowData {
    pub(crate) fn check_persistence(&self) -> Result<(), PersistError> {
        // Check that we're not losing info.
        if self.current_execution.is_some() || !self.task_queue.is_empty() {
            return Err(PersistError::PendingTask);
        }

        for (channel_id, state) in self.persisted.receivers() {
            if state.pending_message.is_some() {
                return Err(PersistError::PendingMessage {
                    channel_kind: ChannelHalf::Receiver,
                    channel_id,
                });
            }
        }
        for (channel_id, state) in self.persisted.senders() {
            if !state.messages.is_empty() {
                return Err(PersistError::PendingMessage {
                    channel_kind: ChannelHalf::Sender,
                    channel_id,
                });
            }
        }
        Ok(())
    }

    // Must be preceded with `Self::check_persistence()`.
    pub(crate) fn persist(&self) -> PersistedWorkflowData {
        self.persisted.clone()
    }
}
