//! Persistence for `State`.

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use wasmtime::{Store, Val};

use std::{collections::HashMap, error, fmt};

use super::{
    channel::{ChannelStates, ReceiverState, SenderState},
    helpers::HostResource,
    spawn::ChildWorkflowStubs,
    task::TaskQueue,
    time::Timers,
    PersistedWorkflowData, WorkflowData,
};
use crate::{module::Services, workflow::ChannelIds};
use tardigrade::{
    interface::{ChannelHalf, Interface},
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
    fn check_on_restore(interface: &Interface, channel_name: &str) -> anyhow::Result<()> {
        interface.receiver(channel_name).ok_or_else(|| {
            anyhow!(
                "receiver `{}` is present in persisted state, but not \
                 in workflow interface",
                channel_name
            )
        })?;
        Ok(())
    }
}

impl SenderState {
    fn check_on_restore(interface: &Interface, channel_name: &str) -> anyhow::Result<()> {
        interface.sender(channel_name).ok_or_else(|| {
            anyhow!(
                "sender `{}` is present in persisted state, but not \
                 in workflow interface",
                channel_name
            )
        })?;
        Ok(())
    }
}

impl ChannelStates {
    fn check_on_restore(&self, interface: &Interface) -> anyhow::Result<()> {
        for name in self.mapping.receiver_names() {
            ReceiverState::check_on_restore(interface, name)?;
        }
        for name in self.mapping.sender_names() {
            SenderState::check_on_restore(interface, name)?;
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

    pub fn restore<'a>(
        self,
        interface: &Interface,
        services: Services<'a>,
    ) -> anyhow::Result<WorkflowData<'a>> {
        self.channels.check_on_restore(interface)?;
        Ok(WorkflowData {
            exports: None,
            persisted: self,
            services,
            current_execution: None,
            task_queue: TaskQueue::default(),
            current_wakeup_cause: None,
        })
    }
}

impl WorkflowData<'_> {
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
    pub(crate) fn persist(self) -> PersistedWorkflowData {
        self.persisted
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct Refs {
    inner: HashMap<u32, HostResource>,
}

impl Refs {
    pub fn new(store: &mut Store<WorkflowData>) -> Self {
        let ref_table = store.data().exports().ref_table;
        let ref_count = ref_table.size(&mut *store);
        let refs = (0..ref_count).filter_map(|idx| {
            let val = ref_table.get(&mut *store, idx).unwrap();
            // ^ `unwrap()` is safe: we know that the index is in bounds
            val.externref().and_then(|reference| {
                HostResource::from_ref(reference.as_ref())
                    .ok()
                    .cloned()
                    .map(|res| (idx, res))
            })
        });

        Self {
            inner: refs.collect(),
        }
    }

    pub fn restore(self, store: &mut Store<WorkflowData>) -> anyhow::Result<()> {
        let ref_table = store.data().exports().ref_table;
        let expected_size = self.inner.keys().copied().max().map_or(0, |idx| idx + 1);
        let current_size = ref_table.size(&mut *store);
        if current_size < expected_size {
            ref_table.grow(
                &mut *store,
                expected_size - current_size,
                Val::ExternRef(None),
            )?;
        }
        for (idx, resource) in self.inner {
            ref_table.set(&mut *store, idx, resource.into_ref().into())?;
        }
        Ok(())
    }
}
