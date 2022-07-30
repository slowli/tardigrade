//! Functionality to manage channel state.

use wasmtime::{AsContextMut, ExternRef, StoreContextMut, Trap};

use std::{collections::HashSet, error, fmt, mem, task::Poll};

use super::{
    helpers::{HostResource, Message, WakeIfPending, WakerPlacement, WasmContext, WasmContextPtr},
    WorkflowData, WorkflowFunctions,
};
use crate::{
    receipt::WakeUpCause,
    utils::{copy_bytes_from_wasm, copy_string_from_wasm, WasmAllocator},
    WakerId,
};
use tardigrade::interface::{AccessErrorKind, OutboundChannelSpec};
use tardigrade_shared::{abi::IntoWasm, PollMessage};

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
#[derive(Debug, Clone, Default)]
pub struct InboundChannelState {
    pub(super) is_acquired: bool,
    pub(super) is_closed: bool,
    pub(super) received_messages: usize,
    pub(super) pending_message: Option<Message>,
    pub(super) wakes_on_next_element: HashSet<WakerId>,
}

impl InboundChannelState {
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
#[derive(Debug, Clone, Default)]
pub struct OutboundChannelState {
    pub(super) is_acquired: bool,
    pub(super) flushed_messages: usize,
    pub(super) messages: Vec<Message>,
    pub(super) wakes_on_flush: HashSet<WakerId>,
}

impl OutboundChannelState {
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
    fn is_ready(&self, spec: &OutboundChannelSpec) -> bool {
        spec.capacity.map_or(true, |cap| self.messages.len() < cap)
    }
}

impl WorkflowData {
    pub(crate) fn push_inbound_message(
        &mut self,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<(), ConsumeError> {
        let channel_state = self.inbound_channels.get_mut(channel_name).unwrap();
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
                channel_name: channel_name.to_owned(),
                message_index,
            },
        );
        Ok(())
    }

    pub(crate) fn close_inbound_channel(&mut self, channel_name: &str) -> Result<(), ConsumeError> {
        let channel_state = self.inbound_channels.get_mut(channel_name).unwrap();
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
                channel_name: channel_name.to_owned(),
            },
        );
        Ok(())
    }

    pub(crate) fn handle_inbound_channel_closure(
        &mut self,
        channel_name: &str,
    ) -> HashSet<WakerId> {
        let channel_state = self.inbound_channels.get_mut(channel_name).unwrap();
        // ^ `unwrap()` safety is guaranteed by previous checks.
        mem::take(&mut channel_state.wakes_on_next_element)
    }

    #[cfg(feature = "async")]
    pub(crate) fn listened_inbound_channels(&self) -> impl Iterator<Item = &str> + '_ {
        self.inbound_channels.iter().filter_map(|(name, state)| {
            if state.wakes_on_next_element.is_empty() {
                None
            } else {
                Some(name.as_str())
            }
        })
    }

    pub(crate) fn inbound_channel(&self, channel_name: &str) -> Option<&InboundChannelState> {
        self.inbound_channels.get(channel_name)
    }

    fn inbound_channel_mut(&mut self, channel_name: &str) -> Option<&mut InboundChannelState> {
        self.inbound_channels.get_mut(channel_name)
    }

    pub(crate) fn outbound_channel(&self, channel_name: &str) -> Option<&OutboundChannelState> {
        self.outbound_channels.get(channel_name)
    }

    fn outbound_channel_mut(&mut self, channel_name: &str) -> Option<&mut OutboundChannelState> {
        self.outbound_channels.get_mut(channel_name)
    }

    fn push_outbound_message(&mut self, channel_name: &str, message: Vec<u8>) {
        let channel_state = self.outbound_channel_mut(channel_name).unwrap();
        // ^ `unwrap()` safety is guaranteed by resource handling
        let message_len = message.len();
        channel_state.messages.push(message.into());
        self.current_execution()
            .push_outbound_message_event(channel_name, message_len);
    }

    fn poll_inbound_channel(&mut self, channel_name: &str, cx: &mut WasmContext) -> PollMessage {
        let channel_state = self.inbound_channel_mut(channel_name).unwrap();
        // ^ `unwrap()` safety is guaranteed by resource handling
        let poll_result = channel_state.poll_next();
        self.current_execution()
            .push_inbound_channel_event(channel_name, &poll_result);
        poll_result.wake_if_pending(cx, || {
            WakerPlacement::InboundChannel(channel_name.to_owned())
        })
    }

    fn poll_outbound_channel(
        &mut self,
        channel_name: &str,
        flush: bool,
        cx: &mut WasmContext,
    ) -> Poll<()> {
        let channel_state = &self.outbound_channels[channel_name];
        let spec = self.interface.outbound_channel(channel_name).unwrap();

        let should_block =
            !channel_state.is_ready(spec) || (flush && !channel_state.messages.is_empty());
        let poll_result = if should_block {
            Poll::Pending
        } else {
            Poll::Ready(())
        };

        self.current_execution()
            .push_outbound_poll_event(channel_name, flush, poll_result);
        poll_result.wake_if_pending(cx, || {
            WakerPlacement::OutboundChannel(channel_name.to_owned())
        })
    }

    pub(crate) fn take_outbound_messages(&mut self, channel_name: &str) -> (usize, Vec<Vec<u8>>) {
        let channel_state = self.outbound_channels.get_mut(channel_name).unwrap();
        let start_message_idx = channel_state.flushed_messages;
        let messages = mem::take(&mut channel_state.messages);
        channel_state.flushed_messages += messages.len();

        if !messages.is_empty() {
            let wakers = mem::take(&mut channel_state.wakes_on_flush);
            self.schedule_wakers(
                wakers,
                WakeUpCause::Flush {
                    channel_name: channel_name.to_owned(),
                    message_indexes: start_message_idx..(start_message_idx + messages.len()),
                },
            );
        }
        let messages = messages.into_iter().map(Into::into).collect();
        (start_message_idx, messages)
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
        channel_name_ptr: u32,
        channel_name_len: u32,
        error_ptr: u32,
    ) -> Result<Option<ExternRef>, Trap> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;
        let result = ctx
            .data_mut()
            .inbound_channel_mut(&channel_name)
            .ok_or(AccessErrorKind::Unknown)
            .and_then(InboundChannelState::acquire);
        let result = crate::log_result!(result, "Acquired inbound channel `{}`", channel_name);

        let channel_ref = result
            .as_ref()
            .ok()
            .map(|()| HostResource::InboundChannel(channel_name).into_ref());
        Self::write_access_result(&mut ctx, result, error_ptr)?;
        Ok(channel_ref)
    }

    pub fn poll_next_for_receiver(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> Result<i64, Trap> {
        let channel_name = HostResource::from_ref(channel_ref.as_ref())?.as_inbound_channel()?;

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx
            .data_mut()
            .poll_inbound_channel(channel_name, &mut poll_cx);
        crate::trace!(
            "Polled inbound channel `{}` with context {:?}: {:?}",
            channel_name,
            poll_cx,
            poll_result,
        );

        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn get_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        error_ptr: u32,
    ) -> Result<Option<ExternRef>, Trap> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;
        let result = ctx
            .data_mut()
            .outbound_channel_mut(&channel_name)
            .ok_or(AccessErrorKind::Unknown)
            .and_then(OutboundChannelState::acquire);
        let result = crate::log_result!(result, "Acquired outbound channel `{}`", channel_name);

        let channel_ref = result
            .as_ref()
            .ok()
            .map(|()| HostResource::OutboundChannel(channel_name).into_ref());
        Self::write_access_result(&mut ctx, result, error_ptr)?;
        Ok(channel_ref)
    }

    pub fn poll_ready_for_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        cx: WasmContextPtr,
    ) -> Result<i32, Trap> {
        let channel_name = HostResource::from_ref(channel_ref.as_ref())?.as_outbound_channel()?;

        let mut cx = WasmContext::new(cx);
        let poll_result = ctx
            .data_mut()
            .poll_outbound_channel(channel_name, false, &mut cx);
        crate::trace!(
            "Polled outbound channel `{}` for readiness: {:?}",
            channel_name,
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
    ) -> Result<(), Trap> {
        let channel_name = HostResource::from_ref(channel_ref.as_ref())?.as_outbound_channel()?;
        let memory = ctx.data().exports().memory;
        let message = copy_bytes_from_wasm(&ctx, &memory, message_ptr, message_len)?;

        ctx.data_mut().push_outbound_message(channel_name, message);
        Ok(())
    }

    pub fn poll_flush_for_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_ref: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> Result<i32, Trap> {
        let channel_name = HostResource::from_ref(channel_ref.as_ref())?.as_outbound_channel()?;

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx
            .data_mut()
            .poll_outbound_channel(channel_name, true, &mut poll_cx);
        crate::trace!(
            "Polled outbound channel `{}` for flush: {:?}",
            channel_name,
            poll_result
        );

        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }
}
