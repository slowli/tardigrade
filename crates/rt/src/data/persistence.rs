//! Persistence for `State`.

use chrono::{DateTime, Utc};

use std::{collections::HashMap, error, fmt};

use super::{
    channel::{ChannelStates, ReceiverState, SenderState},
    task::TaskQueue,
    time::Timers,
    PersistedWorkflowData, WorkflowData,
};
use crate::{manager::Services, workflow::ChannelIds};
use tardigrade::{
    interface::{Handle, HandlePath, Interface, ReceiverAt, SenderAt},
    ChannelId,
};

/// Error persisting a workflow.
#[derive(Debug)]
pub(crate) enum PersistError {
    /// There is a pending task.
    PendingTask,
    /// There is an non-flushed / non-consumed message.
    PendingMessage {
        /// ID of the channel involved.
        channel_id: Handle<ChannelId>,
    },
}

impl fmt::Display for PersistError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("workflow cannot be persisted at this point: ")?;
        match self {
            Self::PendingTask => formatter.write_str("there is a pending task"),

            Self::PendingMessage { channel_id } => {
                write!(
                    formatter,
                    "there is an non-flushed message in {channel_id:?}"
                )
            }
        }
    }
}

impl error::Error for PersistError {}

impl ReceiverState {
    fn check_on_restore(interface: &Interface, path: HandlePath<'_>) -> anyhow::Result<()> {
        interface.handle(ReceiverAt(path))?;
        Ok(())
    }
}

impl SenderState {
    fn check_on_restore(interface: &Interface, path: HandlePath<'_>) -> anyhow::Result<()> {
        interface.handle(SenderAt(path))?;
        Ok(())
    }
}

impl ChannelStates {
    fn check_on_restore(&self, interface: &Interface) -> anyhow::Result<()> {
        for (path, spec) in &self.mapping {
            let path = path.as_ref();
            spec.as_ref()
                .map_receiver(|_| ReceiverState::check_on_restore(interface, path))
                .map_sender(|_| SenderState::check_on_restore(interface, path))
                .factor()?;
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
                    channel_id: Handle::Receiver(channel_id),
                });
            }
        }
        for (channel_id, state) in self.persisted.senders() {
            if !state.messages.is_empty() {
                return Err(PersistError::PendingMessage {
                    channel_id: Handle::Sender(channel_id),
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
