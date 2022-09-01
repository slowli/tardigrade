//! Persistence for `State`.

use anyhow::{anyhow, ensure};
use serde::{Deserialize, Serialize};

use std::{
    collections::{HashMap, HashSet},
    error, fmt,
};
use wasmtime::{Store, Val};

use super::{
    helpers::HostResource,
    task::{TaskQueue, TaskState},
    time::Timers,
    WorkflowData,
};
use crate::{services::Services, ChannelId, TaskId, WakerId, WorkflowId};
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
            Self::PendingOutboundMessage {
                channel_name,
                workflow_id,
            } => {
                write!(
                    formatter,
                    "there is an non-flushed outbound message on channel `{}`",
                    channel_name
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

#[allow(clippy::trivially_copy_pass_by_ref)] // required by serde
fn flip_bool(&flag: &bool) -> bool {
    !flag
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InboundChannelState {
    channel_id: ChannelId,
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
            channel_id: state.channel_id,
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
            channel_id: persisted.channel_id,
            is_closed: persisted.is_closed,
            is_acquired: persisted.is_acquired,
            received_messages: persisted.received_messages,
            pending_message: None,
            wakes_on_next_element: persisted.wakes_on_next_element,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutboundChannelState {
    channel_id: ChannelId,
    capacity: Option<usize>,
    is_acquired: bool,
    is_closed: bool,
    flushed_messages: usize,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    wakes_on_flush: HashSet<WakerId>,
}

impl OutboundChannelState {
    fn new(state: &super::OutboundChannelState) -> Self {
        debug_assert!(state.messages.is_empty());
        Self {
            channel_id: state.channel_id,
            capacity: state.capacity,
            is_acquired: state.is_acquired,
            is_closed: state.is_closed,
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
            channel_id: persisted.channel_id,
            capacity: persisted.capacity,
            is_acquired: persisted.is_acquired,
            is_closed: persisted.is_closed,
            flushed_messages: persisted.flushed_messages,
            messages: Vec::new(),
            wakes_on_flush: persisted.wakes_on_flush,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChannelStates {
    inbound: HashMap<String, InboundChannelState>,
    outbound: HashMap<String, OutboundChannelState>,
}

impl ChannelStates {
    fn new(channels: &super::ChannelStates) -> Self {
        let outbound = channels
            .outbound
            .iter()
            .map(|(name, state)| {
                let persisted_state = OutboundChannelState::new(state);
                (name.clone(), persisted_state)
            })
            .collect();
        let inbound = channels
            .inbound
            .iter()
            .map(|(name, state)| (name.clone(), InboundChannelState::new(state)))
            .collect();
        Self { inbound, outbound }
    }

    fn restore(self, interface: &Interface<()>) -> anyhow::Result<super::ChannelStates> {
        let inbound_channels_len = interface.inbound_channels().len();
        ensure!(
            inbound_channels_len == self.inbound.len(),
            "mismatch between number of inbound channels in workflow interface ({}) \
             and in persisted state ({})",
            inbound_channels_len,
            self.inbound.len()
        );
        let inbound = self.inbound.into_iter().map(|(name, state)| {
            let restored = state.restore(interface, &name)?;
            Ok((name, restored))
        });
        let inbound = inbound.collect::<anyhow::Result<_>>()?;

        let outbound_channels_len = interface.outbound_channels().len();
        ensure!(
            outbound_channels_len == self.outbound.len(),
            "mismatch between number of outbound channels in workflow interface ({}) \
             and in persisted state ({})",
            outbound_channels_len,
            self.outbound.len()
        );
        let outbound = self.outbound.into_iter().map(|(name, state)| {
            let restored = state.restore(interface, &name)?;
            Ok((name, restored))
        });
        let outbound = outbound.collect::<anyhow::Result<_>>()?;

        Ok(super::ChannelStates { inbound, outbound })
    }
}

impl From<ChannelStates> for super::ChannelStates {
    fn from(persisted: ChannelStates) -> Self {
        let inbound = persisted
            .inbound
            .into_iter()
            .map(|(name, state)| (name, state.into()))
            .collect();
        let outbound = persisted
            .outbound
            .into_iter()
            .map(|(name, state)| (name, state.into()))
            .collect();
        Self { inbound, outbound }
    }
}

/// `Workflow` state that can be persisted between workflow invocations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkflowState {
    channels: ChannelStates,
    timers: Timers,
    tasks: HashMap<TaskId, TaskState>,
}

impl WorkflowState {
    pub fn restore(
        self,
        interface: Interface<()>,
        services: Services,
    ) -> anyhow::Result<WorkflowData> {
        let channels = self.channels.restore(&interface)?;
        Ok(WorkflowData {
            exports: None,
            interface,
            channels,
            timers: self.timers,
            services,
            tasks: self.tasks,
            child_workflows: HashMap::default(), // FIXME
            current_execution: None,
            task_queue: TaskQueue::default(),
            waker_queue: Vec::new(),
            current_wakeup_cause: None,
        })
    }

    // NB. Should agree with logic in `Self::restore()`.
    pub fn restore_in_place(self, data: &mut WorkflowData) {
        data.channels = self.channels.into();
        data.timers = self.timers;
        data.tasks = self.tasks;
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

        for (workflow_id, name, state) in self.outbound_channels() {
            if !state.messages.is_empty() {
                return Err(PersistError::PendingOutboundMessage {
                    channel_name: name.to_owned(),
                    workflow_id,
                });
            }
        }
        Ok(())
    }

    // Must be preceded with `Self::check_persistence()`.
    pub(crate) fn persist(&self) -> WorkflowState {
        WorkflowState {
            channels: ChannelStates::new(&self.channels),
            timers: self.timers.clone(),
            tasks: self.tasks.clone(),
        }
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
