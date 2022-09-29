//! Persistence for `State`.

use anyhow::{anyhow, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use std::{collections::HashMap, error, fmt};
use wasmtime::{Store, Val};

use super::{
    channel::{ChannelStates, InboundChannelState, OutboundChannelState},
    helpers::HostResource,
    task::TaskQueue,
    time::Timers,
    PersistedWorkflowData, WorkflowData,
};
use crate::{module::Services, workflow::ChannelIds, WorkflowId};
use tardigrade::interface::{ChannelKind, Interface};

/// Error persisting a workflow.
#[derive(Debug)]
pub(crate) enum PersistError {
    /// There is a pending task.
    PendingTask,
    /// There is an non-flushed / non-consumed message.
    PendingMessage {
        /// Kind of the channel involved.
        channel_kind: ChannelKind,
        /// Name of the channel with the message.
        channel_name: String,
        /// ID of the remote workflow that the channel is attached to, or `None` if the channel
        /// is local.
        workflow_id: Option<WorkflowId>,
    },
}

impl fmt::Display for PersistError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("workflow cannot be persisted at this point: ")?;
        match self {
            Self::PendingTask => formatter.write_str("there is a pending task"),

            Self::PendingMessage {
                channel_kind,
                channel_name,
                workflow_id,
            } => {
                write!(
                    formatter,
                    "there is an non-flushed {} message on channel `{}`",
                    channel_kind, channel_name
                )?;
                if let Some(id) = workflow_id {
                    write!(formatter, " for workflow {}", id)?;
                }
                Ok(())
            }
        }
    }
}

impl error::Error for PersistError {}

impl InboundChannelState {
    fn check_on_restore(interface: &Interface<()>, channel_name: &str) -> anyhow::Result<()> {
        interface.inbound_channel(channel_name).ok_or_else(|| {
            anyhow!(
                "inbound channel `{}` is present in persisted state, but not \
                 in workflow interface",
                channel_name
            )
        })?;
        Ok(())
    }
}

impl OutboundChannelState {
    fn check_on_restore(interface: &Interface<()>, channel_name: &str) -> anyhow::Result<()> {
        interface.outbound_channel(channel_name).ok_or_else(|| {
            anyhow!(
                "outbound channel `{}` is present in persisted state, but not \
                 in workflow interface",
                channel_name
            )
        })?;
        Ok(())
    }
}

impl ChannelStates {
    fn check_on_restore(&self, interface: &Interface<()>) -> anyhow::Result<()> {
        let inbound_channels_len = interface.inbound_channels().len();
        ensure!(
            inbound_channels_len == self.inbound.len(),
            "mismatch between number of inbound channels in workflow interface ({}) \
             and in persisted state ({})",
            inbound_channels_len,
            self.inbound.len()
        );
        for name in self.inbound.keys() {
            InboundChannelState::check_on_restore(interface, name)?;
        }

        let outbound_channels_len = interface.outbound_channels().len();
        ensure!(
            outbound_channels_len == self.outbound.len(),
            "mismatch between number of outbound channels in workflow interface ({}) \
             and in persisted state ({})",
            outbound_channels_len,
            self.outbound.len()
        );
        for name in self.outbound.keys() {
            OutboundChannelState::check_on_restore(interface, name)?;
        }

        Ok(())
    }
}

impl PersistedWorkflowData {
    pub(super) fn new(
        interface: &Interface<()>,
        channel_ids: &ChannelIds,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            channels: ChannelStates::new(channel_ids, |name| {
                interface.outbound_channel(name).unwrap().capacity
            }),
            timers: Timers::new(now),
            tasks: HashMap::new(),
            child_workflows: HashMap::new(),
            waker_queue: Vec::new(),
        }
    }

    pub fn restore<'a>(
        self,
        interface: &Interface<()>,
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

        for (workflow_id, name, state) in self.persisted.inbound_channels() {
            if state.pending_message.is_some() {
                return Err(PersistError::PendingMessage {
                    channel_kind: ChannelKind::Inbound,
                    channel_name: name.to_owned(),
                    workflow_id,
                });
            }
        }
        for (workflow_id, name, state) in self.persisted.outbound_channels() {
            if !state.messages.is_empty() {
                return Err(PersistError::PendingMessage {
                    channel_kind: ChannelKind::Outbound,
                    channel_name: name.to_owned(),
                    workflow_id,
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
