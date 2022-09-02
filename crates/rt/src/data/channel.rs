//! Functionality to manage channel state.

use wasmtime::{AsContextMut, ExternRef, StoreContextMut, Trap};

use std::{
    collections::{HashMap, HashSet},
    error, fmt, mem,
    task::Poll,
};

use super::{
    helpers::{
        ChannelRef, HostResource, WakeIfPending, WakerPlacement, WasmContext, WasmContextPtr,
    },
    WorkflowData, WorkflowFunctions,
};
use crate::{
    receipt::WakeUpCause,
    utils::{copy_bytes_from_wasm, copy_string_from_wasm, Message, WasmAllocator},
    workflow::ChannelIds,
    ChannelId, WakerId, WorkflowId,
};
use tardigrade::interface::AccessErrorKind;
use tardigrade_shared::{abi::IntoWasm, PollMessage, SendError};

/// Kind of a [`ConsumeError`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ConsumeErrorKind {
    /// No tasks listen to the channel.
    NotListened,
    /// The channel is closed.
    Closed,
}

impl ConsumeErrorKind {
    pub(crate) fn for_message(self, channel_name: &str, message: Vec<u8>) -> ConsumeError {
        ConsumeError {
            kind: self,
            channel_name: channel_name.to_owned(),
            message: Some(message),
        }
    }

    pub(crate) fn for_closure(self, channel_name: &str) -> ConsumeError {
        ConsumeError {
            kind: self,
            channel_name: channel_name.to_owned(),
            message: None,
        }
    }
}

impl fmt::Display for ConsumeErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotListened => "channel is not currently listened to",
            Self::Closed => "channel is closed",
        })
    }
}

/// Errors that can occur when feeding a message to an inbound
/// [`Workflow`](crate::Workflow) channel.
#[derive(Debug)]
pub struct ConsumeError {
    channel_name: String,
    message: Option<Vec<u8>>,
    kind: ConsumeErrorKind,
}

impl fmt::Display for ConsumeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(message) = &self.message {
            write!(
                formatter,
                "cannot push message ({} bytes) into inbound channel `{}`: {}",
                message.len(),
                self.channel_name,
                self.kind
            )
        } else {
            write!(
                formatter,
                "cannot close inbound channel `{}`: {}",
                self.channel_name, self.kind
            )
        }
    }
}

impl error::Error for ConsumeError {}

impl ConsumeError {
    /// Returns the kind of this error.
    pub fn kind(&self) -> &ConsumeErrorKind {
        &self.kind
    }

    /// Returns the channel name.
    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }

    /// Retrieves message bytes from this error, if the error was caused by sending a message.
    /// Otherwise, returns `None`.
    pub fn into_message(self) -> Option<Vec<u8>> {
        self.message
    }
}

/// State of an inbound workflow channel.
#[derive(Debug, Clone)]
pub struct InboundChannelState {
    pub(super) channel_id: ChannelId,
    pub(super) is_acquired: bool,
    pub(super) is_closed: bool,
    pub(super) received_messages: usize,
    pub(super) pending_message: Option<Message>,
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

    /// Checks whether this channel is closed.
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /// Returns the total number of messages received by this channel. Only processed messages
    /// are counted, the pending message (if any) is not.
    pub fn received_messages(&self) -> usize {
        self.received_messages
    }

    fn acquire(&mut self) -> Result<(), AccessErrorKind> {
        if mem::replace(&mut self.is_acquired, true) {
            Err(AccessErrorKind::AlreadyAcquired)
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
#[derive(Debug, Clone)]
pub struct OutboundChannelState {
    pub(super) channel_id: ChannelId,
    pub(super) capacity: Option<usize>,
    pub(super) is_acquired: bool,
    pub(super) is_closed: bool,
    pub(super) flushed_messages: usize,
    pub(super) messages: Vec<Message>,
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

    /// Returns the number of messages flushed to this channel.
    pub fn flushed_messages(&self) -> usize {
        self.flushed_messages
    }

    fn acquire(&mut self) -> Result<(), AccessErrorKind> {
        if mem::replace(&mut self.is_acquired, true) {
            Err(AccessErrorKind::AlreadyAcquired)
        } else {
            Ok(())
        }
    }

    /// Is this channel ready to receive another message?
    fn is_ready(&self) -> bool {
        !self.is_closed && self.capacity.map_or(true, |cap| self.messages.len() < cap)
    }
}

#[derive(Debug)]
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

impl WorkflowData {
    fn channels_mut(&mut self, workflow_id: Option<WorkflowId>) -> &mut ChannelStates {
        if let Some(workflow_id) = workflow_id {
            &mut self.child_workflows.get_mut(&workflow_id).unwrap().channels
        } else {
            &mut self.channels
        }
    }

    pub(crate) fn push_inbound_message(
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
            return Err(ConsumeErrorKind::Closed.for_message(channel_name, message));
        }
        if channel_state.wakes_on_next_element.is_empty() {
            return Err(ConsumeErrorKind::NotListened.for_message(channel_name, message));
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
    ) -> Result<(), ConsumeError> {
        let channel_state = self
            .channels_mut(workflow_id)
            .inbound
            .get_mut(channel_name)
            .unwrap();
        // ^ `unwrap()` safety is guaranteed by previous checks.
        if channel_state.is_closed {
            return Ok(()); // no further actions required
        }
        if channel_state.wakes_on_next_element.is_empty() {
            return Err(ConsumeErrorKind::NotListened.for_closure(channel_name));
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
        Ok(())
    }

    pub(super) fn handle_inbound_channel_closure(
        &mut self,
        channel_ref: &ChannelRef,
    ) -> HashSet<WakerId> {
        let channels = self.channels_mut(channel_ref.workflow_id);
        let channel_state = channels.inbound.get_mut(&channel_ref.name).unwrap();
        let wakers = mem::take(&mut channel_state.wakes_on_next_element);
        if !channel_state.is_closed {
            channel_state.is_closed = true;
            let channel_id = channel_state.channel_id;
            self.current_execution()
                .push_inbound_channel_closure(channel_ref, channel_id);
        }
        wakers
    }

    #[cfg(feature = "async")]
    pub(crate) fn listened_inbound_channels(&self) -> impl Iterator<Item = &str> + '_ {
        // FIXME: include workflow channels as well
        self.channels.inbound.iter().filter_map(|(name, state)| {
            if state.wakes_on_next_element.is_empty() {
                None
            } else {
                Some(name.as_str())
            }
        })
    }

    pub(crate) fn inbound_channel(&self, channel_name: &str) -> Option<&InboundChannelState> {
        self.channels.inbound.get(channel_name)
    }

    pub(super) fn inbound_channel_mut(
        &mut self,
        channel_ref: &ChannelRef,
    ) -> Option<&mut InboundChannelState> {
        self.channels_mut(channel_ref.workflow_id)
            .inbound
            .get_mut(&channel_ref.name)
    }

    pub(crate) fn inbound_channels(
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

    pub(super) fn inbound_channels_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut InboundChannelState> + '_ {
        let workflow_channels = self
            .child_workflows
            .values_mut()
            .flat_map(|workflow| workflow.channels.inbound.values_mut());
        self.channels.inbound.values_mut().chain(workflow_channels)
    }

    pub(crate) fn outbound_channel(&self, channel_name: &str) -> Option<&OutboundChannelState> {
        self.channels.outbound.get(channel_name)
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

    pub(super) fn outbound_channels_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut OutboundChannelState> + '_ {
        let workflow_channels = self
            .child_workflows
            .values_mut()
            .flat_map(|workflow| workflow.channels.outbound.values_mut());
        self.channels.outbound.values_mut().chain(workflow_channels)
    }

    fn push_outbound_message(
        &mut self,
        channel_ref: &ChannelRef,
        message: Vec<u8>,
    ) -> Result<(), SendError> {
        let channels = self.channels_mut(channel_ref.workflow_id);
        let channel_state = channels.outbound.get_mut(&channel_ref.name).unwrap();

        if channel_state.is_closed {
            return Err(SendError::Closed);
        } else if !channel_state.is_ready() {
            return Err(SendError::Full);
        }

        let message_len = message.len();
        channel_state.messages.push(message.into());
        self.current_execution()
            .push_outbound_message_event(channel_ref, message_len);
        Ok(())
    }

    fn poll_inbound_channel(
        &mut self,
        channel_ref: &ChannelRef,
        cx: &mut WasmContext,
    ) -> PollMessage {
        let channels = self.channels_mut(channel_ref.workflow_id);
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
            &self.child_workflows[&workflow_id].channels
        } else {
            &self.channels
        };
        let channel_state = &channels.outbound[&channel_ref.name];

        let should_block =
            !channel_state.is_ready() || (flush && !channel_state.messages.is_empty());
        let poll_result = if channel_state.is_closed {
            Poll::Ready(Err(SendError::Closed))
        } else if should_block {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        };

        self.current_execution()
            .push_outbound_poll_event(channel_ref, flush, poll_result.clone());
        poll_result.wake_if_pending(cx, || WakerPlacement::OutboundChannel(channel_ref.clone()))
    }

    pub(crate) fn take_outbound_messages(
        &mut self,
        workflow_id: Option<WorkflowId>,
        channel_name: &str,
    ) -> (usize, Vec<Message>) {
        let channel_state = self
            .channels_mut(workflow_id)
            .outbound
            .get_mut(channel_name)
            .unwrap();
        let start_message_idx = channel_state.flushed_messages;
        let messages = mem::take(&mut channel_state.messages);
        channel_state.flushed_messages += messages.len();

        if !messages.is_empty() {
            let wakers = mem::take(&mut channel_state.wakes_on_flush);
            self.schedule_wakers(
                wakers,
                WakeUpCause::Flush {
                    workflow_id,
                    channel_name: channel_name.to_owned(),
                    message_indexes: start_message_idx..(start_message_idx + messages.len()),
                },
            );
        }
        let messages = messages.into_iter().collect();
        (start_message_idx, messages)
    }

    #[allow(clippy::needless_collect)] // false positive
    pub(crate) fn drain_messages(&mut self) -> Vec<(ChannelId, Vec<Message>)> {
        let channels = self.channels.outbound.iter();
        let channel_ids: Vec<_> = channels
            .map(|(name, state)| (name.clone(), state.id()))
            .collect();

        let messages = channel_ids.into_iter().filter_map(|(name, channel_id)| {
            let (_, messages) = self.take_outbound_messages(None, &name);
            if messages.is_empty() {
                None
            } else {
                Some((channel_id, messages))
            }
        });
        messages.collect()
    }

    #[cfg(test)]
    pub(crate) fn outbound_channel_wakers(&self) -> impl Iterator<Item = WakerId> + '_ {
        self.channels
            .outbound
            .values()
            .flat_map(|state| &state.wakes_on_flush)
            .copied()
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

    pub fn get_receiver(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        workflow: Option<ExternRef>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        error_ptr: u32,
    ) -> Result<Option<ExternRef>, Trap> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;
        let data = ctx.data_mut();
        let (workflow_id, channels) = Self::channels(data, workflow.as_ref())?;

        let result = channels
            .inbound
            .get_mut(&channel_name)
            .ok_or(AccessErrorKind::Unknown)
            .and_then(InboundChannelState::acquire);
        let result = crate::log_result!(result, "Acquired inbound channel `{}`", channel_name);

        let channel_ref = result
            .as_ref()
            .ok()
            .map(|()| HostResource::inbound_channel(workflow_id, channel_name).into_ref());
        Self::write_access_result(&mut ctx, result, error_ptr)?;
        Ok(channel_ref)
    }

    fn channels<'a>(
        data: &'a mut WorkflowData,
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

    pub fn poll_next_for_receiver(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> Result<i64, Trap> {
        let channel_ref = HostResource::from_ref(channel_ref.as_ref())?.as_inbound_channel()?;

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx
            .data_mut()
            .poll_inbound_channel(channel_ref, &mut poll_cx);
        crate::trace!(
            "Polled inbound channel {:?} with context {:?}: {:?}",
            channel_ref,
            poll_cx,
            poll_result,
        );

        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn get_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        workflow: Option<ExternRef>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        error_ptr: u32,
    ) -> Result<Option<ExternRef>, Trap> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;
        let data = ctx.data_mut();
        let (workflow_id, channels) = Self::channels(data, workflow.as_ref())?;

        let result = channels
            .outbound
            .get_mut(&channel_name)
            .ok_or(AccessErrorKind::Unknown)
            .and_then(OutboundChannelState::acquire);
        let result = crate::log_result!(result, "Acquired outbound channel `{}`", channel_name);

        let channel_ref = result
            .as_ref()
            .ok()
            .map(|()| HostResource::outbound_channel(workflow_id, channel_name).into_ref());
        Self::write_access_result(&mut ctx, result, error_ptr)?;
        Ok(channel_ref)
    }

    pub fn poll_ready_for_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        cx: WasmContextPtr,
    ) -> Result<i32, Trap> {
        let channel_ref = HostResource::from_ref(channel_ref.as_ref())?.as_outbound_channel()?;

        let mut cx = WasmContext::new(cx);
        let poll_result = ctx
            .data_mut()
            .poll_outbound_channel(channel_ref, false, &mut cx);
        crate::trace!(
            "Polled outbound channel {:?} for readiness: {:?}",
            channel_ref,
            poll_result
        );

        cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn start_send(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        message_ptr: u32,
        message_len: u32,
    ) -> Result<i32, Trap> {
        let channel_ref = HostResource::from_ref(channel_ref.as_ref())?.as_outbound_channel()?;
        let memory = ctx.data().exports().memory;
        let message = copy_bytes_from_wasm(&ctx, &memory, message_ptr, message_len)?;

        let message_len = message.len();
        let result = ctx.data_mut().push_outbound_message(channel_ref, message);
        crate::trace!(
            "Sent message ({} bytes) over outbound channel {:?}: {:?}",
            message_len,
            channel_ref,
            result
        );
        result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn poll_flush_for_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> Result<i32, Trap> {
        let channel_ref = HostResource::from_ref(channel_ref.as_ref())?.as_outbound_channel()?;

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx
            .data_mut()
            .poll_outbound_channel(channel_ref, true, &mut poll_cx);
        crate::trace!(
            "Polled outbound channel {:?} for flush: {:?}",
            channel_ref,
            poll_result
        );

        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }
}
