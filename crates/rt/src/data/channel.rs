//! Functionality to manage channel state.

use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use tracing::field;
use wasmtime::{AsContextMut, ExternRef, StoreContextMut};

use std::{
    cmp,
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
    interface::{AccessErrorKind, ChannelHalf, Interface},
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct ChannelMappingState {
    id: ChannelId,
    is_acquired: bool,
}

impl ChannelMappingState {
    fn new(id: ChannelId) -> Self {
        Self {
            id,
            is_acquired: false,
        }
    }

    fn acquire(&mut self) -> Result<ChannelId, AlreadyAcquired> {
        if mem::replace(&mut self.is_acquired, true) {
            Err(AlreadyAcquired)
        } else {
            Ok(self.id)
        }
    }
}

/// Mapping of channels (from names to IDs) for a workflow.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct ChannelMapping {
    receivers: HashMap<String, ChannelMappingState>,
    senders: HashMap<String, ChannelMappingState>,
}

impl ChannelMapping {
    pub fn new(ids: ChannelIds) -> Self {
        let receivers = ids
            .receivers
            .into_iter()
            .map(|(name, id)| (name, ChannelMappingState::new(id)))
            .collect();
        let senders = ids
            .senders
            .into_iter()
            .map(|(name, id)| (name, ChannelMappingState::new(id)))
            .collect();
        Self { receivers, senders }
    }

    pub fn receiver_names(&self) -> impl Iterator<Item = &str> + '_ {
        self.receivers.keys().map(String::as_str)
    }

    pub fn sender_names(&self) -> impl Iterator<Item = &str> + '_ {
        self.senders.keys().map(String::as_str)
    }

    pub fn acquire_non_captured_channels(&mut self, config: &ChannelsConfig<ChannelId>) {
        for (name, channel_config) in &config.receivers {
            if matches!(channel_config, ChannelSpawnConfig::Existing(_)) {
                let state = self.senders.get_mut(name).unwrap();
                state.is_acquired = true;
            }
        }
        for (name, channel_config) in &config.senders {
            if matches!(channel_config, ChannelSpawnConfig::Existing(_)) {
                let state = self.receivers.get_mut(name).unwrap();
                state.is_acquired = true;
            }
        }
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)] // required by serde
fn flip_bool(&flag: &bool) -> bool {
    !flag
}

/// State of a workflow channel receiver.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReceiverState {
    #[serde(default, skip_serializing_if = "flip_bool")]
    pub(super) is_closed: bool,
    pub(super) received_messages: usize,
    #[serde(default, skip)]
    pub(super) pending_message: Option<Message>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub(super) wakes_on_next_element: HashSet<WakerId>,
}

impl ReceiverState {
    pub(crate) fn new() -> Self {
        Self::default()
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

/// State of a workflow channel sender.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SenderState {
    pub(super) capacity: Option<usize>,
    /// Number of references to the channel (including non-acquired ones).
    pub(super) ref_count: usize,
    pub(super) is_closed: bool,
    pub(super) flushed_messages: usize,
    #[serde(default, skip)]
    pub(super) messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub(super) wakes_on_flush: HashSet<WakerId>,
}

impl SenderState {
    pub(crate) fn new(capacity: Option<usize>) -> Self {
        Self {
            capacity,
            ref_count: 0,
            is_closed: false,
            flushed_messages: 0,
            messages: Vec::new(),
            wakes_on_flush: HashSet::new(),
        }
    }

    /// Checks whether the channel is closed.
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /// Is this channel ready to receive another message?
    fn is_ready(&self) -> bool {
        !self.is_closed && self.capacity.map_or(true, |cap| self.messages.len() < cap)
    }

    fn ensure_capacity(&mut self, capacity: Option<usize>) {
        self.capacity = match (self.capacity, capacity) {
            (None, _) | (_, None) => None,
            (Some(current), Some(necessary)) => Some(cmp::max(current, necessary)),
        };
    }

    fn take_messages(&mut self, channel_id: ChannelId) -> (Vec<Message>, Option<Wakers>) {
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
                        channel_id,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct ChannelStates {
    pub receivers: HashMap<ChannelId, ReceiverState>,
    pub senders: HashMap<ChannelId, SenderState>,
    pub mapping: ChannelMapping,
}

impl ChannelStates {
    pub fn new(channel_ids: ChannelIds, interface: &Interface) -> Self {
        let mut this = Self::default();
        this.insert_channels(&channel_ids, |name| {
            interface.sender(name).unwrap().capacity
        });
        this.mapping = ChannelMapping::new(channel_ids);
        this
    }

    pub fn insert_channels<F>(&mut self, channel_ids: &ChannelIds, sender_cap_fn: F)
    where
        F: Fn(&str) -> Option<usize>,
    {
        for &id in channel_ids.receivers.values() {
            self.receivers.entry(id).or_insert_with(ReceiverState::new);
        }
        for (name, &id) in &channel_ids.senders {
            let capacity = sender_cap_fn(name);
            let state = self
                .senders
                .entry(id)
                .or_insert_with(|| SenderState::new(capacity));
            state.ensure_capacity(capacity);
            state.ref_count += 1;
        }
    }
}

/// Information about channels for a particular workflow interface.
#[derive(Debug, Clone, Copy)]
pub struct Channels<'a> {
    states: &'a ChannelStates,
    mapping: &'a ChannelMapping,
}

impl<'a> Channels<'a> {
    pub(super) fn new(states: &'a ChannelStates, mapping: &'a ChannelMapping) -> Self {
        Self { states, mapping }
    }

    /// Returns the channel ID of the receiver with the specified name, or `None` if the receiver
    /// is not present.
    pub fn receiver_id(&self, name: &str) -> Option<ChannelId> {
        self.mapping.receivers.get(name).map(|state| state.id)
    }

    /// Returns information about the receiver with the specified name.
    pub fn receiver(&self, name: &str) -> Option<&'a ReceiverState> {
        let channel_id = self.mapping.receivers.get(name)?.id;
        self.states.receivers.get(&channel_id)
    }

    /// Iterates over all receivers.
    pub fn receivers(&self) -> impl Iterator<Item = (&'a str, &'a ReceiverState)> + '_ {
        self.mapping.receivers.iter().filter_map(|(name, state)| {
            let state = self.states.receivers.get(&state.id)?;
            Some((name.as_str(), state))
        })
    }

    /// Returns the channel ID of the sender with the specified name, or `None` if the sender
    /// is not present.
    pub fn sender_id(&self, name: &str) -> Option<ChannelId> {
        self.mapping.senders.get(name).map(|state| state.id)
    }

    /// Returns information about the sender with the specified name.
    pub fn sender(&self, name: &str) -> Option<&'a SenderState> {
        let channel_id = self.mapping.senders.get(name)?.id;
        self.states.senders.get(&channel_id)
    }

    /// Iterates over all senders.
    pub fn senders(&self) -> impl Iterator<Item = (&'a str, &'a SenderState)> + '_ {
        self.mapping.senders.iter().filter_map(|(name, state)| {
            let state = self.states.senders.get(&state.id)?;
            Some((name.as_str(), state))
        })
    }

    pub(crate) fn to_ids(self) -> ChannelIds {
        let receivers = self.mapping.receivers.iter();
        let receivers = receivers
            .map(|(name, state)| (name.clone(), state.id))
            .collect();
        let senders = self.mapping.senders.iter();
        let senders = senders
            .map(|(name, state)| (name.clone(), state.id))
            .collect();
        ChannelIds { receivers, senders }
    }
}

impl PersistedWorkflowData {
    pub(crate) fn push_message_for_receiver(
        &mut self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), ConsumeError> {
        let channel_state = self.channels.receivers.get_mut(&channel_id).unwrap();
        if channel_state.is_closed {
            return Err(ConsumeError::Closed);
        }
        if channel_state.wakes_on_next_element.is_empty() {
            return Err(ConsumeError::NotListened);
        }

        debug_assert!(
            channel_state.pending_message.is_none(),
            "Multiple messages inserted for channel receiver {channel_id}"
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

    pub(crate) fn drop_message_for_receiver(&mut self, channel_id: ChannelId) {
        let channel_state = self.channels.receivers.get_mut(&channel_id).unwrap();
        channel_state.received_messages += 1;
    }

    pub(crate) fn close_receiver(&mut self, channel_id: ChannelId) {
        let channel_state = self.channels.receivers.get_mut(&channel_id).unwrap();
        if channel_state.is_closed {
            return; // no further actions required
        }

        debug_assert!(
            channel_state.pending_message.is_none(),
            "Receiver {channel_id} closed while it has a pending message"
        );
        channel_state.is_closed = true;
        channel_state.received_messages += 1; // EOF is a considered a message as well

        let wakers = mem::take(&mut channel_state.wakes_on_next_element);
        self.schedule_wakers(wakers, WakeUpCause::ChannelClosed { channel_id });
    }

    pub(crate) fn close_sender(&mut self, channel_id: ChannelId) {
        let channel_state = self.channels.senders.get_mut(&channel_id).unwrap();
        channel_state.close();
    }

    pub(crate) fn channels(&self) -> Channels<'_> {
        Channels::new(&self.channels, &self.channels.mapping)
    }

    pub(crate) fn receiver(&self, channel_id: ChannelId) -> Option<&ReceiverState> {
        self.channels.receivers.get(&channel_id)
    }

    pub(crate) fn receivers(&self) -> impl Iterator<Item = (ChannelId, &ReceiverState)> + '_ {
        self.channels
            .receivers
            .iter()
            .map(|(&id, state)| (id, state))
    }

    pub(super) fn receiver_mut(&mut self, channel_id: ChannelId) -> Option<&mut ReceiverState> {
        self.channels.receivers.get_mut(&channel_id)
    }

    pub(super) fn receivers_mut(&mut self) -> impl Iterator<Item = &mut ReceiverState> + '_ {
        self.channels.receivers.values_mut()
    }

    pub(super) fn sender_mut(&mut self, channel_id: ChannelId) -> Option<&mut SenderState> {
        self.channels.senders.get_mut(&channel_id)
    }

    pub(crate) fn senders(&self) -> impl Iterator<Item = (ChannelId, &SenderState)> + '_ {
        self.channels.senders.iter().map(|(&id, state)| (id, state))
    }

    pub(crate) fn senders_mut(
        &mut self,
    ) -> impl Iterator<Item = (ChannelId, &mut SenderState)> + '_ {
        self.channels
            .senders
            .iter_mut()
            .map(|(&id, state)| (id, state))
    }

    fn drain_messages(&mut self) -> HashMap<ChannelId, Vec<Message>> {
        let mut new_wakers = vec![];
        let messages_by_channel = self
            .senders_mut()
            .filter_map(|(id, state)| {
                let (messages, maybe_wakers) = state.take_messages(id);
                if let Some(wakers) = maybe_wakers {
                    new_wakers.push(wakers);
                }

                if messages.is_empty() {
                    None
                } else {
                    Some((id, messages))
                }
            })
            .collect();

        self.waker_queue.extend(new_wakers);
        messages_by_channel
    }
}

impl WorkflowData<'_> {
    pub(super) fn handle_receiver_drop(&mut self, channel_id: ChannelId) -> HashSet<WakerId> {
        let channel_state = self
            .persisted
            .channels
            .receivers
            .get_mut(&channel_id)
            .unwrap();
        let wakers = mem::take(&mut channel_state.wakes_on_next_element);
        if !channel_state.is_closed {
            channel_state.is_closed = true;
            self.current_execution()
                .push_channel_closure(ChannelHalf::Receiver, channel_id);
        }
        wakers
    }

    pub(super) fn handle_sender_drop(&mut self, channel_id: ChannelId) -> HashSet<WakerId> {
        let channels = &mut self.persisted.channels;
        let state = channels.senders.get_mut(&channel_id).unwrap();
        state.ref_count -= 1;

        if state.ref_count == 0 {
            let wakers = mem::take(&mut state.wakes_on_flush);
            self.current_execution()
                .push_channel_closure(ChannelHalf::Sender, channel_id);
            wakers
        } else {
            HashSet::new()
        }
    }

    pub(crate) fn take_pending_inbound_message(&mut self, channel_id: ChannelId) -> bool {
        let channels = &mut self.persisted.channels;
        let state = channels.receivers.get_mut(&channel_id).unwrap();
        let has_message = state.pending_message.take().is_some();
        if has_message {
            state.received_messages -= 1;
        }
        has_message
    }

    fn poll_receiver(&mut self, channel_id: ChannelId, cx: &mut WasmContext) -> PollMessage {
        let channels = &mut self.persisted.channels;
        let state = channels.receivers.get_mut(&channel_id).unwrap();
        // ^ `unwrap()` safety is guaranteed by resource handling

        let poll_result = state.poll_next();
        self.current_execution()
            .push_receiver_event(channel_id, &poll_result);
        poll_result.wake_if_pending(cx, || WakerPlacement::Receiver(channel_id))
    }

    fn poll_sender(
        &mut self,
        channel_id: ChannelId,
        flush: bool,
        cx: &mut WasmContext,
    ) -> Poll<Result<(), SendError>> {
        let channel_state = &self.persisted.channels.senders[&channel_id];

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
            .push_sender_poll_event(channel_id, flush, poll_result.clone());
        poll_result.wake_if_pending(cx, || WakerPlacement::Sender(channel_id))
    }

    fn push_outbound_message(
        &mut self,
        channel_id: ChannelId,
        message: Vec<u8>,
    ) -> Result<(), SendError> {
        let channel_state = self
            .persisted
            .channels
            .senders
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
        let mapping = Self::channel_mapping(data, child_id)?;

        let mapping = mapping
            .receivers
            .get_mut(&channel_name)
            .ok_or(AccessErrorKind::Unknown);
        let result = mapping.map(|mapping_state| mapping_state.acquire().ok());
        utils::debug_result(&result);

        let mut channel_ref = None;
        let result = result.map(|acquire_result| {
            channel_ref = acquire_result.map(|id| HostResource::Receiver(id).into_ref());
        });
        Self::write_access_result(&mut ctx, result, error_ptr)?;
        Ok(channel_ref)
    }

    fn channel_mapping(
        data: &mut PersistedWorkflowData,
        child_id: Option<WorkflowId>,
    ) -> anyhow::Result<&mut ChannelMapping> {
        if let Some(child_id) = child_id {
            let workflow = data
                .child_workflows
                .get_mut(&child_id)
                .ok_or_else(|| anyhow!("unknown child workflow ID {child_id}"))?;
            Ok(&mut workflow.channels)
        } else {
            Ok(&mut data.channels.mapping)
        }
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(channel, message.len))]
    pub fn poll_next_for_receiver(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> anyhow::Result<i64> {
        let channel_id = HostResource::from_ref(channel_ref.as_ref())?.as_receiver()?;
        tracing::Span::current().record("channel", field::debug(channel_ref));

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx.data_mut().poll_receiver(channel_id, &mut poll_cx);
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
        let mapping = Self::channel_mapping(data, child_id)?;

        let mapping = mapping
            .senders
            .get_mut(&channel_name)
            .ok_or(AccessErrorKind::Unknown);
        let result = mapping.map(|mapping_state| mapping_state.acquire().ok());
        utils::debug_result(&result);

        let mut channel_ref = None;
        let result = result.map(|acquire_result| {
            channel_ref = acquire_result.map(|id| HostResource::Sender(id).into_ref());
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
        let channel_id = HostResource::from_ref(channel_ref.as_ref())?.as_sender()?;
        tracing::Span::current().record("channel", field::debug(channel_ref));

        let mut cx = WasmContext::new(cx);
        let poll_result = ctx.data_mut().poll_sender(channel_id, false, &mut cx);
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
        let channel_id = HostResource::from_ref(channel_ref.as_ref())?.as_sender()?;
        tracing::Span::current().record("channel", field::debug(channel_id));

        let memory = ctx.data().exports().memory;
        let message = utils::copy_bytes_from_wasm(&ctx, &memory, message_ptr, message_len)?;
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
        let channel_id = HostResource::from_ref(channel_ref.as_ref())?.as_sender()?;
        tracing::Span::current().record("channel", field::debug(channel_id));

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx.data_mut().poll_sender(channel_id, true, &mut poll_cx);
        tracing::debug!(result = ?poll_result);

        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }
}
