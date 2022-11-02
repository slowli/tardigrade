//! Functionality to manage channel state.

use serde::{Deserialize, Serialize};
use tracing::field;
use wasmtime::{AsContextMut, ExternRef, StoreContextMut, Trap};

use std::{
    cmp,
    collections::{HashMap, HashSet},
    error, fmt, mem,
    task::Poll,
};

use super::{
    helpers::{
        ChannelRef, HostResource, WakeIfPending, WakerPlacement, Wakers, WasmContext,
        WasmContextPtr,
    },
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
    ChannelId, WakerId, WorkflowId,
};

/// Wrapper around `Message` that contains message index within all messages emitted
/// by the workflow during its execution. This allows correct ordering of messages
/// for aliased channels.
#[derive(Debug, Clone)]
pub(super) struct OrderedMessage {
    inner: Message,
    index: usize,
}

impl PartialEq for OrderedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl Eq for OrderedMessage {}

impl PartialOrd for OrderedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedMessage {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.index.cmp(&other.index)
    }
}

impl From<OrderedMessage> for Message {
    fn from(message: OrderedMessage) -> Self {
        message.inner
    }
}

impl OrderedMessage {
    fn new(message: Vec<u8>, index: usize) -> Self {
        Self {
            inner: message.into(),
            index,
        }
    }
}

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

    /// Returns ID of the channel.
    pub fn id(&self) -> ChannelId {
        self.channel_id
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
    pub(super) messages: Vec<OrderedMessage>,
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

    fn take_messages(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
    ) -> (Vec<OrderedMessage>, Option<Wakers>) {
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
                        workflow_id,
                        channel_name: channel_name.to_owned(),
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
    pub inbound: HashMap<String, InboundChannelState>,
    pub outbound: HashMap<String, OutboundChannelState>,
}

impl ChannelStates {
    pub fn new<F>(channel_ids: &ChannelIds, outbound_cap_fn: F) -> Self
    where
        F: Fn(&str) -> Option<usize>,
    {
        let inbound = channel_ids
            .inbound
            .iter()
            .map(|(name, channel_id)| (name.clone(), InboundChannelState::new(*channel_id)))
            .collect();
        let outbound = channel_ids
            .outbound
            .iter()
            .map(|(name, channel_id)| {
                (
                    name.clone(),
                    OutboundChannelState::new(*channel_id, outbound_cap_fn(name)),
                )
            })
            .collect();
        Self { inbound, outbound }
    }
}

impl PersistedWorkflowData {
    fn channels(&self, workflow_id: Option<WorkflowId>) -> &ChannelStates {
        if let Some(workflow_id) = workflow_id {
            &self.child_workflows.get(&workflow_id).unwrap().channels
        } else {
            &self.channels
        }
    }

    fn channels_mut(&mut self, workflow_id: Option<WorkflowId>) -> &mut ChannelStates {
        if let Some(workflow_id) = workflow_id {
            &mut self.child_workflows.get_mut(&workflow_id).unwrap().channels
        } else {
            &mut self.channels
        }
    }

    fn push_inbound_message(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<(), ConsumeError> {
        let channel_state = self
            .channels_mut(workflow_id)
            .inbound
            .get_mut(channel_name)
            .unwrap();
        // ^ `unwrap()` safety is guaranteed by previous checks.

        if channel_state.is_closed {
            return Err(ConsumeError::Closed);
        }
        if channel_state.wakes_on_next_element.is_empty() {
            return Err(ConsumeError::NotListened);
        }

        debug_assert!(
            channel_state.pending_message.is_none(),
            "Multiple messages inserted for inbound channel `{}`",
            channel_name
        );
        let message_index = channel_state.received_messages;
        channel_state.pending_message = Some(message.into());
        channel_state.received_messages += 1;

        let wakers = mem::take(&mut channel_state.wakes_on_next_element);
        self.schedule_wakers(
            wakers,
            WakeUpCause::InboundMessage {
                workflow_id,
                channel_name: channel_name.to_owned(),
                message_index,
            },
        );
        Ok(())
    }

    pub(crate) fn close_inbound_channel(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
    ) {
        let channel_state = self
            .channels_mut(workflow_id)
            .inbound
            .get_mut(channel_name)
            .unwrap();
        // ^ `unwrap()` safety is guaranteed by previous checks.
        if channel_state.is_closed {
            return; // no further actions required
        }

        debug_assert!(
            channel_state.pending_message.is_none(),
            "Inbound channel `{}` closed while it has a pending message",
            channel_name
        );
        channel_state.is_closed = true;

        let wakers = mem::take(&mut channel_state.wakes_on_next_element);
        self.schedule_wakers(
            wakers,
            WakeUpCause::ChannelClosed {
                workflow_id,
                channel_name: channel_name.to_owned(),
            },
        );
    }

    pub(crate) fn close_outbound_channel(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
    ) {
        let channel_state = self
            .channels_mut(workflow_id)
            .outbound
            .get_mut(channel_name)
            .unwrap();
        channel_state.close();
    }

    pub fn channel_ids(&self) -> ChannelIds {
        let inbound = self.channels.inbound.iter();
        let inbound = inbound.map(|(name, state)| (name.clone(), state.id()));
        let outbound = self.channels.outbound.iter();
        let outbound = outbound.map(|(name, state)| (name.clone(), state.id()));

        ChannelIds {
            inbound: inbound.collect(),
            outbound: outbound.collect(),
        }
    }

    pub fn inbound_channels(
        &self,
    ) -> impl Iterator<Item = (Option<WorkflowId>, &str, &InboundChannelState)> + '_ {
        let local_channels = self.channels.inbound.iter();
        let local_channels = local_channels.map(|(name, state)| (None, name.as_str(), state));
        let workflow_channels = self
            .child_workflows
            .iter()
            .flat_map(|(&workflow_id, workflow)| {
                let channels = workflow.channels.inbound.iter();
                channels.map(move |(name, state)| (Some(workflow_id), name.as_str(), state))
            });
        local_channels.chain(workflow_channels)
    }

    pub(super) fn inbound_channel_mut(
        &mut self,
        channel_ref: &ChannelRef,
    ) -> Option<&mut InboundChannelState> {
        self.channels_mut(channel_ref.workflow_id)
            .inbound
            .get_mut(&channel_ref.name)
    }

    pub(super) fn inbound_channels_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut InboundChannelState> + '_ {
        let workflow_channels = self
            .child_workflows
            .values_mut()
            .flat_map(|workflow| workflow.channels.inbound.values_mut());
        self.channels.inbound.values_mut().chain(workflow_channels)
    }

    pub(super) fn outbound_channel(
        &self,
        channel_ref: &ChannelRef,
    ) -> Option<&OutboundChannelState> {
        self.channels(channel_ref.workflow_id)
            .outbound
            .get(&channel_ref.name)
    }

    pub(super) fn outbound_channel_mut(
        &mut self,
        channel_ref: &ChannelRef,
    ) -> Option<&mut OutboundChannelState> {
        self.channels_mut(channel_ref.workflow_id)
            .outbound
            .get_mut(&channel_ref.name)
    }

    pub(crate) fn outbound_channels(
        &self,
    ) -> impl Iterator<Item = (Option<WorkflowId>, &str, &OutboundChannelState)> + '_ {
        let local_channels = self.channels.outbound.iter();
        let local_channels = local_channels.map(|(name, state)| (None, name.as_str(), state));
        let workflow_channels = self
            .child_workflows
            .iter()
            .flat_map(|(&workflow_id, workflow)| {
                let channels = workflow.channels.outbound.iter();
                channels.map(move |(name, state)| (Some(workflow_id), name.as_str(), state))
            });
        local_channels.chain(workflow_channels)
    }

    pub(crate) fn outbound_channels_mut(
        &mut self,
    ) -> impl Iterator<Item = (Option<WorkflowId>, &str, &mut OutboundChannelState)> + '_ {
        let local_channels = self.channels.outbound.iter_mut();
        let local_channels = local_channels.map(|(name, state)| (None, name.as_str(), state));
        let workflow_channels =
            self.child_workflows
                .iter_mut()
                .flat_map(|(&workflow_id, workflow)| {
                    let channels = workflow.channels.outbound.iter_mut();
                    channels.map(move |(name, state)| (Some(workflow_id), name.as_str(), state))
                });
        local_channels.chain(workflow_channels)
    }

    fn drain_messages(&mut self) -> HashMap<ChannelId, Vec<Message>> {
        let mut new_wakers = vec![];
        let messages_by_channel = self.outbound_channels_mut().fold(
            HashMap::<_, Vec<OrderedMessage>>::new(),
            |mut acc, (child_id, name, state)| {
                let (messages, maybe_wakers) = state.take_messages(child_id, name);
                if let Some(wakers) = maybe_wakers {
                    new_wakers.push(wakers);
                }
                if !messages.is_empty() {
                    utils::merge_vec(acc.entry(state.id()).or_default(), messages);
                }
                acc
            },
        );

        self.waker_queue.extend(new_wakers);
        messages_by_channel
            .into_iter()
            .map(|(id, messages)| (id, messages.into_iter().map(Message::from).collect()))
            .collect()
    }
}

impl WorkflowData<'_> {
    pub(super) fn handle_inbound_channel_drop(
        &mut self,
        channel_ref: &ChannelRef,
    ) -> HashSet<WakerId> {
        let channels = self.persisted.channels_mut(channel_ref.workflow_id);
        let channel_state = channels.inbound.get_mut(&channel_ref.name).unwrap();
        let wakers = mem::take(&mut channel_state.wakes_on_next_element);
        if !channel_state.is_closed {
            channel_state.is_closed = true;
            let channel_id = channel_state.channel_id;
            self.current_execution().push_channel_closure(
                ChannelKind::Inbound,
                channel_ref,
                channel_id,
                0, // inbound channels cannot be aliased
            );
        }
        wakers
    }

    pub(super) fn handle_outbound_channel_drop(
        &mut self,
        channel_ref: &ChannelRef,
    ) -> HashSet<WakerId> {
        let channels = self.persisted.channels_mut(channel_ref.workflow_id);
        let channel_state = channels.outbound.get_mut(&channel_ref.name).unwrap();
        let wakers = mem::take(&mut channel_state.wakes_on_flush);
        if !channel_state.is_closed {
            channel_state.is_closed = true;
            let channel_id = channel_state.channel_id;
            let remaining_alias_count = self
                .persisted
                .outbound_channels()
                .filter(|(_, _, state)| state.id() == channel_id && !state.is_closed)
                .count();

            self.current_execution().push_channel_closure(
                ChannelKind::Outbound,
                channel_ref,
                channel_id,
                remaining_alias_count,
            );
        }
        wakers
    }

    pub(crate) fn push_inbound_message(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<(), ConsumeError> {
        self.persisted
            .push_inbound_message(workflow_id, channel_name, message)
    }

    pub(crate) fn take_pending_inbound_message(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
    ) -> bool {
        let state = self
            .persisted
            .channels_mut(workflow_id)
            .inbound
            .get_mut(channel_name)
            .unwrap();
        state.pending_message.take().is_some()
    }

    fn poll_inbound_channel(
        &mut self,
        channel_ref: &ChannelRef,
        cx: &mut WasmContext,
    ) -> PollMessage {
        let channels = self.persisted.channels_mut(channel_ref.workflow_id);
        let channel_state = channels.inbound.get_mut(&channel_ref.name).unwrap();
        // ^ `unwrap()` safety is guaranteed by resource handling

        let poll_result = channel_state.poll_next();
        self.current_execution()
            .push_inbound_channel_event(channel_ref, &poll_result);
        poll_result.wake_if_pending(cx, || WakerPlacement::InboundChannel(channel_ref.clone()))
    }

    fn poll_outbound_channel(
        &mut self,
        channel_ref: &ChannelRef,
        flush: bool,
        cx: &mut WasmContext,
    ) -> Poll<Result<(), SendError>> {
        let channels = if let Some(workflow_id) = channel_ref.workflow_id {
            &self.persisted.child_workflows[&workflow_id].channels
        } else {
            &self.persisted.channels
        };
        let channel_state = &channels.outbound[&channel_ref.name];

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
            .push_outbound_poll_event(channel_ref, flush, poll_result.clone());
        poll_result.wake_if_pending(cx, || WakerPlacement::OutboundChannel(channel_ref.clone()))
    }

    fn push_outbound_message(
        &mut self,
        channel_ref: &ChannelRef,
        message: Vec<u8>,
    ) -> Result<(), SendError> {
        let channels = self.persisted.channels_mut(channel_ref.workflow_id);
        let channel_state = channels.outbound.get_mut(&channel_ref.name).unwrap();

        if channel_state.is_closed {
            return Err(SendError::Closed);
        } else if !channel_state.is_ready() {
            return Err(SendError::Full);
        }

        let message_len = message.len();
        let index = self.counters.new_outbound_message_index();
        channel_state
            .messages
            .push(OrderedMessage::new(message, index));
        self.current_execution()
            .push_outbound_message_event(channel_ref, message_len);
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
    ) -> Result<(), Trap> {
        let memory = ctx.data().exports().memory;
        let result_abi = result.into_wasm(&mut WasmAllocator::new(ctx.as_context_mut()))?;
        memory
            .write(ctx, error_ptr as usize, &result_abi.to_le_bytes())
            .map_err(|err| Trap::new(format!("cannot write to WASM memory: {}", err)))
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(workflow_id, channel_name))]
    pub fn get_receiver(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        workflow: Option<ExternRef>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        error_ptr: u32,
    ) -> Result<Option<ExternRef>, Trap> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            utils::copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;
        let data = &mut ctx.data_mut().persisted;
        let (workflow_id, channels) = Self::channels(data, workflow.as_ref())?;

        tracing::Span::current()
            .record("workflow_id", workflow_id)
            .record("channel_name", &channel_name);

        let result = channels
            .inbound
            .get_mut(&channel_name)
            .ok_or(AccessErrorKind::Unknown)
            .map(|state| state.acquire().ok());
        utils::debug_result(&result);

        let mut channel_ref = None;
        let result = result.map(|acquire_result| {
            channel_ref = acquire_result
                .map(|()| HostResource::inbound_channel(workflow_id, channel_name).into_ref());
        });
        Self::write_access_result(&mut ctx, result, error_ptr)?;
        Ok(channel_ref)
    }

    fn channels<'a>(
        data: &'a mut PersistedWorkflowData,
        workflow: Option<&ExternRef>,
    ) -> Result<(Option<WorkflowId>, &'a mut ChannelStates), Trap> {
        if let Some(workflow) = workflow {
            let id = HostResource::from_ref(Some(workflow))?.as_workflow()?;
            let workflow = data.child_workflows.get_mut(&id).ok_or_else(|| {
                let message = format!("unknown workflow ID {}", id);
                Trap::new(message)
            })?;
            Ok((Some(id), &mut workflow.channels))
        } else {
            Ok((None, &mut data.channels))
        }
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(channel, message.len))]
    pub fn poll_next_for_receiver(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> Result<i64, Trap> {
        let channel_ref = HostResource::from_ref(channel_ref.as_ref())?.as_inbound_channel()?;
        tracing::Span::current().record("channel", field::debug(channel_ref));

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx
            .data_mut()
            .poll_inbound_channel(channel_ref, &mut poll_cx);
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
        workflow: Option<ExternRef>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        error_ptr: u32,
    ) -> Result<Option<ExternRef>, Trap> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            utils::copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;
        let data = &mut ctx.data_mut().persisted;
        let (workflow_id, channels) = Self::channels(data, workflow.as_ref())?;

        tracing::Span::current()
            .record("workflow_id", workflow_id)
            .record("channel_name", &channel_name);

        let result = channels
            .outbound
            .get_mut(&channel_name)
            .ok_or(AccessErrorKind::Unknown)
            .map(|state| state.acquire().ok());
        utils::debug_result(&result);

        let mut channel_ref = None;
        let result = result.map(|acquire_result| {
            channel_ref = acquire_result
                .map(|()| HostResource::outbound_channel(workflow_id, channel_name).into_ref());
        });
        Self::write_access_result(&mut ctx, result, error_ptr)?;
        Ok(channel_ref)
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(channel))]
    pub fn poll_ready_for_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        cx: WasmContextPtr,
    ) -> Result<i32, Trap> {
        let channel_ref = HostResource::from_ref(channel_ref.as_ref())?.as_outbound_channel()?;
        tracing::Span::current().record("channel", field::debug(channel_ref));

        let mut cx = WasmContext::new(cx);
        let poll_result = ctx
            .data_mut()
            .poll_outbound_channel(channel_ref, false, &mut cx);
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
    ) -> Result<i32, Trap> {
        let channel_ref = HostResource::from_ref(channel_ref.as_ref())?.as_outbound_channel()?;
        let memory = ctx.data().exports().memory;
        let message = utils::copy_bytes_from_wasm(&ctx, &memory, message_ptr, message_len)?;
        tracing::Span::current().record("channel", field::debug(channel_ref));

        let result = ctx.data_mut().push_outbound_message(channel_ref, message);
        utils::debug_result(&result);
        result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(channel))]
    pub fn poll_flush_for_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> Result<i32, Trap> {
        let channel_ref = HostResource::from_ref(channel_ref.as_ref())?.as_outbound_channel()?;
        tracing::Span::current().record("channel", field::debug(channel_ref));

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx
            .data_mut()
            .poll_outbound_channel(channel_ref, true, &mut poll_cx);
        tracing::debug!(result = ?poll_result);

        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }
}
