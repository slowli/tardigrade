//! Functionality to manage channel state.

use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use tracing::field;
use wasmtime::{AsContextMut, ExternRef, StoreContextMut};

use std::{
    collections::{HashMap, HashSet},
    error, fmt, mem,
    task::Poll,
};

use super::{
    helpers::{HostResource, WakeIfPending, WakerPlacement, Wakers, WasmContext, WasmContextPtr},
    PersistedWorkflowData, WorkflowData, WorkflowFunctions,
};
use crate::{
    receipt::WakeUpCause,
    utils::{self, Message, WasmAllocator},
    workflow::ChannelIds,
};
use tardigrade::{
    abi::{IntoWasm, PollMessage},
    channel::SendError,
    interface::{AccessErrorKind, ChannelKind},
    spawn::{ChannelSpawnConfig, ChannelsConfig},
    ChannelId, WakerId, WorkflowId,
};

/// Errors for channels that cannot be acquired by the workflow.
#[derive(Debug)]
struct AlreadyAcquired;

/// Errors that can occur when consuming messages in a workflow.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum ConsumeError {
    /// No tasks listen to the channel.
    NotListened,
    /// The channel is closed.
    Closed,
}

impl fmt::Display for ConsumeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotListened => "channel is not currently listened to",
            Self::Closed => "channel is closed",
        })
    }
}

impl error::Error for ConsumeError {}

#[allow(clippy::trivially_copy_pass_by_ref)] // required by serde
fn flip_bool(&flag: &bool) -> bool {
    !flag
}

/// State of an inbound workflow channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundChannelState {
    pub(super) channel_id: ChannelId,
    pub(super) is_acquired: bool,
    #[serde(default, skip_serializing_if = "flip_bool")]
    pub(super) is_closed: bool,
    pub(super) received_messages: usize,
    #[serde(default, skip)]
    pub(super) pending_message: Option<Message>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub(super) wakes_on_next_element: HashSet<WakerId>,
}

impl InboundChannelState {
    pub(crate) fn new(channel_id: ChannelId) -> Self {
        Self {
            channel_id,
            is_acquired: false,
            is_closed: false,
            received_messages: 0,
            pending_message: None,
            wakes_on_next_element: HashSet::new(),
        }
    }

    /// Returns the ID of the channel.
    pub fn id(&self) -> ChannelId {
        self.channel_id
    }

    /// Returns the number of messages received over the channel.
    pub fn received_message_count(&self) -> usize {
        self.received_messages
    }

    /// Checks whether the channel is closed.
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub(crate) fn waits_for_message(&self) -> bool {
        !self.is_closed && !self.wakes_on_next_element.is_empty()
    }

    fn acquire(&mut self) -> Result<(), AlreadyAcquired> {
        if mem::replace(&mut self.is_acquired, true) {
            Err(AlreadyAcquired)
        } else {
            Ok(())
        }
    }

    fn poll_next(&mut self) -> Poll<Option<Vec<u8>>> {
        if self.is_closed {
            Poll::Ready(None)
        } else if let Some(message) = self.pending_message.take() {
            Poll::Ready(Some(message.into()))
        } else {
            Poll::Pending
        }
    }
}

/// State of an outbound workflow channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboundChannelState {
    pub(super) channel_id: ChannelId,
    pub(super) capacity: Option<usize>,
    pub(super) is_acquired: bool,
    pub(super) is_closed: bool,
    pub(super) flushed_messages: usize,
    #[serde(default, skip)]
    pub(super) messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub(super) wakes_on_flush: HashSet<WakerId>,
}

impl OutboundChannelState {
    pub(crate) fn new(channel_id: ChannelId, capacity: Option<usize>) -> Self {
        Self {
            channel_id,
            capacity,
            is_acquired: false,
            is_closed: false,
            flushed_messages: 0,
            messages: Vec::new(),
            wakes_on_flush: HashSet::new(),
        }
    }

    /// Returns ID of the channel.
    pub fn id(&self) -> ChannelId {
        self.channel_id
    }

    /// Is this channel ready to receive another message?
    fn is_ready(&self) -> bool {
        !self.is_closed && self.capacity.map_or(true, |cap| self.messages.len() < cap)
    }

    fn acquire(&mut self) -> Result<(), AlreadyAcquired> {
        if mem::replace(&mut self.is_acquired, true) {
            Err(AlreadyAcquired)
        } else {
            Ok(())
        }
    }

    fn take_messages(&mut self) -> (Vec<Message>, Option<Wakers>) {
        let start_message_idx = self.flushed_messages;
        let messages = mem::take(&mut self.messages);
        self.flushed_messages += messages.len();

        let mut waker_set = None;
        if !messages.is_empty() {
            let wakers = mem::take(&mut self.wakes_on_flush);
            if !wakers.is_empty() {
                waker_set = Some(Wakers::new(
                    wakers,
                    WakeUpCause::Flush {
                        channel_id: self.channel_id,
                        message_indexes: start_message_idx..(start_message_idx + messages.len()),
                    },
                ));
            }
        }
        (messages, waker_set)
    }

    pub(crate) fn close(&mut self) {
        debug_assert!(self.wakes_on_flush.is_empty());
        self.is_closed = true;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ChannelStates {
    pub inbound: HashMap<ChannelId, InboundChannelState>,
    pub outbound: HashMap<ChannelId, OutboundChannelState>,
    pub mapping: ChannelIds,
}

impl ChannelStates {
    pub fn new<F>(channel_ids: ChannelIds, outbound_cap_fn: F) -> Self
    where
        F: Fn(&str) -> Option<usize>,
    {
        let inbound = channel_ids
            .inbound
            .values()
            .map(|&id| (id, InboundChannelState::new(id)))
            .collect();
        let outbound = channel_ids
            .outbound
            .iter()
            .map(|(name, &id)| (id, OutboundChannelState::new(id, outbound_cap_fn(name))))
            .collect();
        Self {
            inbound,
            outbound,
            mapping: channel_ids,
        }
    }

    pub fn insert_child_channels(
        &mut self,
        channel_ids: &ChannelIds,
        config: &ChannelsConfig<ChannelId>,
    ) {
        // This is needed to prevent the workflow from capturing non-captured channel handles.
        for (name, &id) in &channel_ids.inbound {
            let state = self
                .inbound
                .entry(id)
                .or_insert_with(|| InboundChannelState::new(id));
            if matches!(config.inbound[name], ChannelSpawnConfig::Existing(_)) {
                state.is_acquired = true;
            }
        }

        // TODO: what is the appropriate capacity?
        for (name, &id) in &channel_ids.outbound {
            let state = self
                .outbound
                .entry(id)
                .or_insert_with(|| OutboundChannelState::new(id, Some(1)));
            if matches!(config.outbound[name], ChannelSpawnConfig::Existing(_)) {
                state.is_acquired = true;
            }
        }
    }
}

impl PersistedWorkflowData {
    pub(crate) fn push_inbound_message(
        &mut self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), ConsumeError> {
        let channel_state = self.channels.inbound.get_mut(&channel_id).unwrap();
        if channel_state.is_closed {
            return Err(ConsumeError::Closed);
        }
        if channel_state.wakes_on_next_element.is_empty() {
            return Err(ConsumeError::NotListened);
        }

        debug_assert!(
            channel_state.pending_message.is_none(),
            "Multiple messages inserted for inbound channel {channel_id}"
        );
        let message_index = channel_state.received_messages;
        channel_state.pending_message = Some(message.into());
        channel_state.received_messages += 1;

        let wakers = mem::take(&mut channel_state.wakes_on_next_element);
        self.schedule_wakers(
            wakers,
            WakeUpCause::InboundMessage {
                channel_id,
                message_index,
            },
        );
        Ok(())
    }

    pub(crate) fn drop_inbound_message(&mut self, channel_id: ChannelId) {
        let channel_state = self.channels.inbound.get_mut(&channel_id).unwrap();
        channel_state.received_messages += 1;
    }

    pub(crate) fn close_inbound_channel(&mut self, channel_id: ChannelId) {
        let channel_state = self.channels.inbound.get_mut(&channel_id).unwrap();
        if channel_state.is_closed {
            return; // no further actions required
        }

        debug_assert!(
            channel_state.pending_message.is_none(),
            "Inbound channel {channel_id} closed while it has a pending message"
        );
        channel_state.is_closed = true;
        channel_state.received_messages += 1; // EOF is a considered a message as well

        let wakers = mem::take(&mut channel_state.wakes_on_next_element);
        self.schedule_wakers(wakers, WakeUpCause::ChannelClosed { channel_id });
    }

    // FIXME: is this correct?
    pub(crate) fn close_outbound_channel(&mut self, channel_id: ChannelId) {
        let channel_state = self.channels.outbound.get_mut(&channel_id).unwrap();
        channel_state.close();
    }

    pub fn channel_ids(&self) -> &ChannelIds {
        &self.channels.mapping
    }

    pub(crate) fn inbound_channel(&self, channel_id: ChannelId) -> Option<&InboundChannelState> {
        self.channels.inbound.get(&channel_id)
    }

    pub(crate) fn inbound_channels(
        &self,
    ) -> impl Iterator<Item = (ChannelId, &InboundChannelState)> + '_ {
        self.channels.inbound.iter().map(|(&id, state)| (id, state))
    }

    pub(super) fn inbound_channel_mut(
        &mut self,
        channel_id: ChannelId,
    ) -> Option<&mut InboundChannelState> {
        self.channels.inbound.get_mut(&channel_id)
    }

    pub(super) fn inbound_channels_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut InboundChannelState> + '_ {
        self.channels.inbound.values_mut()
    }

    pub(super) fn outbound_channel(&self, channel_id: ChannelId) -> Option<&OutboundChannelState> {
        self.channels.outbound.get(&channel_id)
    }

    pub(super) fn outbound_channel_mut(
        &mut self,
        channel_id: ChannelId,
    ) -> Option<&mut OutboundChannelState> {
        self.channels.outbound.get_mut(&channel_id)
    }

    pub(crate) fn outbound_channels(
        &self,
    ) -> impl Iterator<Item = (ChannelId, &OutboundChannelState)> + '_ {
        self.channels
            .outbound
            .iter()
            .map(|(&id, state)| (id, state))
    }

    pub(crate) fn outbound_channels_mut(
        &mut self,
    ) -> impl Iterator<Item = (ChannelId, &mut OutboundChannelState)> + '_ {
        self.channels
            .outbound
            .iter_mut()
            .map(|(&id, state)| (id, state))
    }

    fn drain_messages(&mut self) -> HashMap<ChannelId, Vec<Message>> {
        let mut new_wakers = vec![];
        let messages_by_channel = self
            .outbound_channels_mut()
            .map(|(id, state)| {
                let (messages, maybe_wakers) = state.take_messages();
                if let Some(wakers) = maybe_wakers {
                    new_wakers.push(wakers);
                }
                (id, messages)
            })
            .collect();

        self.waker_queue.extend(new_wakers);
        messages_by_channel
    }
}

impl WorkflowData<'_> {
    pub(super) fn handle_inbound_channel_drop(
        &mut self,
        channel_id: ChannelId,
    ) -> HashSet<WakerId> {
        let channel_state = self
            .persisted
            .channels
            .inbound
            .get_mut(&channel_id)
            .unwrap();
        let wakers = mem::take(&mut channel_state.wakes_on_next_element);
        if !channel_state.is_closed {
            channel_state.is_closed = true;
            self.current_execution().push_channel_closure(
                ChannelKind::Inbound,
                channel_id,
                0, // inbound channels cannot be aliased
            );
        }
        wakers
    }

    pub(super) fn handle_outbound_channel_drop(
        &mut self,
        channel_id: ChannelId,
    ) -> HashSet<WakerId> {
        let channel_state = self
            .persisted
            .channels
            .outbound
            .get_mut(&channel_id)
            .unwrap();
        let wakers = mem::take(&mut channel_state.wakes_on_flush);
        if !channel_state.is_closed {
            channel_state.is_closed = true;
            let remaining_alias_count = 0; // FIXME
            self.current_execution().push_channel_closure(
                ChannelKind::Outbound,
                channel_id,
                remaining_alias_count,
            );
        }
        wakers
    }

    pub(crate) fn take_pending_inbound_message(&mut self, channel_id: ChannelId) -> bool {
        let state = self
            .persisted
            .channels
            .inbound
            .get_mut(&channel_id)
            .unwrap();
        let has_message = state.pending_message.take().is_some();
        if has_message {
            state.received_messages -= 1;
        }
        has_message
    }

    fn poll_inbound_channel(&mut self, channel_id: ChannelId, cx: &mut WasmContext) -> PollMessage {
        let channel_state = self
            .persisted
            .channels
            .inbound
            .get_mut(&channel_id)
            .unwrap();
        // ^ `unwrap()` safety is guaranteed by resource handling

        let poll_result = channel_state.poll_next();
        self.current_execution()
            .push_inbound_channel_event(channel_id, &poll_result);
        poll_result.wake_if_pending(cx, || WakerPlacement::InboundChannel(channel_id))
    }

    fn poll_outbound_channel(
        &mut self,
        channel_id: ChannelId,
        flush: bool,
        cx: &mut WasmContext,
    ) -> Poll<Result<(), SendError>> {
        let channel_state = &self.persisted.channels.outbound[&channel_id];

        let should_block =
            !channel_state.is_ready() || (flush && !channel_state.messages.is_empty());
        let poll_result = if channel_state.is_closed {
            Poll::Ready(if flush {
                // In the current implementation, flushing a closed channel always succeeds.
                Ok(())
            } else {
                Err(SendError::Closed)
            })
        } else if should_block {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        };

        self.current_execution()
            .push_outbound_poll_event(channel_id, flush, poll_result.clone());
        poll_result.wake_if_pending(cx, || WakerPlacement::OutboundChannel(channel_id))
    }

    fn push_outbound_message(
        &mut self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), SendError> {
        let channel_state = self
            .persisted
            .channels
            .outbound
            .get_mut(&channel_id)
            .unwrap();
        if channel_state.is_closed {
            return Err(SendError::Closed);
        } else if !channel_state.is_ready() {
            return Err(SendError::Full);
        }

        let message_len = message.len();
        channel_state.messages.push(message.into());
        self.current_execution()
            .push_outbound_message_event(channel_id, message_len);
        Ok(())
    }

    pub(crate) fn drain_messages(&mut self) -> HashMap<ChannelId, Vec<Message>> {
        self.persisted.drain_messages()
    }
}

/// Channel-related functions exported to WASM.
#[allow(clippy::needless_pass_by_value)] // required for WASM function wrappers
impl WorkflowFunctions {
    fn write_access_result(
        ctx: &mut StoreContextMut<'_, WorkflowData>,
        result: Result<(), AccessErrorKind>,
        error_ptr: u32,
    ) -> anyhow::Result<()> {
        let memory = ctx.data().exports().memory;
        let result_abi = result.into_wasm(&mut WasmAllocator::new(ctx.as_context_mut()))?;
        memory
            .write(ctx, error_ptr as usize, &result_abi.to_le_bytes())
            .context("cannot write to WASM memory")
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(workflow_id, channel_name))]
    pub fn get_receiver(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        child: Option<ExternRef>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        error_ptr: u32,
    ) -> anyhow::Result<Option<ExternRef>> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            utils::copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;
        let child_id = if let Some(child) = &child {
            Some(HostResource::from_ref(Some(child))?.as_workflow()?)
        } else {
            None
        };

        tracing::Span::current()
            .record("workflow_id", child_id)
            .record("channel_name", &channel_name);
        let data = &mut ctx.data_mut().persisted;
        let channel_ids = Self::channel_ids(data, child_id)?;

        let channel_id = channel_ids
            .inbound
            .get(&channel_name)
            .copied()
            .ok_or(AccessErrorKind::Unknown);
        let result = channel_id.map(|id| {
            let state = data.channels.inbound.get_mut(&id).unwrap();
            state.acquire().ok().map(|()| id)
        });
        utils::debug_result(&result);

        let mut channel_ref = None;
        let result = result.map(|acquire_result| {
            channel_ref = acquire_result.map(|id| HostResource::InboundChannel(id).into_ref());
        });
        Self::write_access_result(&mut ctx, result, error_ptr)?;
        Ok(channel_ref)
    }

    fn channel_ids(
        data: &PersistedWorkflowData,
        child_id: Option<WorkflowId>,
    ) -> anyhow::Result<&ChannelIds> {
        if let Some(child_id) = child_id {
            let workflow = data
                .child_workflows
                .get(&child_id)
                .ok_or_else(|| anyhow!("unknown child workflow ID {child_id}"))?;
            Ok(&workflow.channels)
        } else {
            Ok(&data.channels.mapping)
        }
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(channel, message.len))]
    pub fn poll_next_for_receiver(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> anyhow::Result<i64> {
        let channel_id = HostResource::from_ref(channel_ref.as_ref())?.as_inbound_channel()?;
        tracing::Span::current().record("channel", field::debug(channel_ref));

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx
            .data_mut()
            .poll_inbound_channel(channel_id, &mut poll_cx);
        tracing::debug!(result = ?utils::drop_value(&poll_result));

        if let Poll::Ready(maybe_message) = &poll_result {
            let message_len = maybe_message.as_ref().map(Vec::len);
            tracing::Span::current().record("message.len", message_len);
        }
        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(workflow_id, channel_name))]
    pub fn get_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        child: Option<ExternRef>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        error_ptr: u32,
    ) -> anyhow::Result<Option<ExternRef>> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            utils::copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;
        let child_id = if let Some(child) = &child {
            Some(HostResource::from_ref(Some(child))?.as_workflow()?)
        } else {
            None
        };

        tracing::Span::current()
            .record("workflow_id", child_id)
            .record("channel_name", &channel_name);
        let data = &mut ctx.data_mut().persisted;
        let channel_ids = Self::channel_ids(data, child_id)?;

        let channel_id = channel_ids
            .outbound
            .get(&channel_name)
            .copied()
            .ok_or(AccessErrorKind::Unknown);
        let result = channel_id.map(|id| {
            let state = data.channels.outbound.get_mut(&id).unwrap();
            state.acquire().ok().map(|()| id)
        });
        utils::debug_result(&result);

        let mut channel_ref = None;
        let result = result.map(|acquire_result| {
            channel_ref = acquire_result.map(|id| HostResource::OutboundChannel(id).into_ref());
        });
        Self::write_access_result(&mut ctx, result, error_ptr)?;
        Ok(channel_ref)
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(channel))]
    pub fn poll_ready_for_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        cx: WasmContextPtr,
    ) -> anyhow::Result<i32> {
        let channel_id = HostResource::from_ref(channel_ref.as_ref())?.as_outbound_channel()?;
        tracing::Span::current().record("channel", field::debug(channel_ref));

        let mut cx = WasmContext::new(cx);
        let poll_result = ctx
            .data_mut()
            .poll_outbound_channel(channel_id, false, &mut cx);
        tracing::debug!(result = ?poll_result);

        cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        err,
        fields(channel, message.len = message_len)
    )]
    pub fn start_send(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        message_ptr: u32,
        message_len: u32,
    ) -> anyhow::Result<i32> {
        let channel_id = HostResource::from_ref(channel_ref.as_ref())?.as_outbound_channel()?;
        let memory = ctx.data().exports().memory;
        let message = utils::copy_bytes_from_wasm(&ctx, &memory, message_ptr, message_len)?;
        tracing::Span::current().record("channel", field::debug(channel_id));

        let result = ctx.data_mut().push_outbound_message(channel_id, message);
        utils::debug_result(&result);
        result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(channel))]
    pub fn poll_flush_for_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> anyhow::Result<i32> {
        let channel_id = HostResource::from_ref(channel_ref.as_ref())?.as_outbound_channel()?;
        tracing::Span::current().record("channel", field::debug(channel_id));

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx
            .data_mut()
            .poll_outbound_channel(channel_id, true, &mut poll_cx);
        tracing::debug!(result = ?poll_result);

        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }
}
