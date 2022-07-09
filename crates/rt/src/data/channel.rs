//! Functionality to manage channel state.

use wasmtime::{AsContextMut, StoreContextMut, Trap};

use std::{collections::HashSet, error, fmt, mem, task::Poll};

use super::{
    helpers::{Message, WakeIfPending, WakerPlacement, WasmContext, WasmContextPtr},
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

#[derive(Debug, Default)]
pub(super) struct InboundChannelState {
    pub is_acquired: bool,
    pub is_closed: bool,
    pub received_messages: usize,
    pub pending_message: Option<Message>,
    pub wakes_on_next_element: HashSet<WakerId>,
}

impl InboundChannelState {
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

#[derive(Debug)]
pub(super) struct OutboundChannelState {
    pub capacity: Option<usize>,
    pub flushed_messages: usize,
    pub messages: Vec<Message>,
    pub wakes_on_flush: HashSet<WakerId>,
}

impl OutboundChannelState {
    pub(super) fn new(spec: &OutboundChannelSpec) -> Self {
        Self {
            capacity: spec.capacity,
            flushed_messages: 0,
            messages: Vec::new(),
            wakes_on_flush: HashSet::new(),
        }
    }

    /// Is this channel ready to receive another message?
    fn is_ready(&self) -> bool {
        self.capacity.map_or(true, |cap| self.messages.len() < cap)
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

    pub(crate) fn listened_inbound_channels(&self) -> impl Iterator<Item = &str> + '_ {
        self.inbound_channels.iter().filter_map(|(name, state)| {
            if state.wakes_on_next_element.is_empty() {
                None
            } else {
                Some(name.as_str())
            }
        })
    }

    fn acquire_inbound_channel(&mut self, channel_name: &str) -> Result<(), AccessErrorKind> {
        let channel_state = self
            .inbound_channels
            .get_mut(channel_name)
            .ok_or(AccessErrorKind::Unknown)?;
        if mem::replace(&mut channel_state.is_acquired, true) {
            Err(AccessErrorKind::AlreadyAcquired)
        } else {
            Ok(())
        }
    }

    fn inbound_channel_mut(
        &mut self,
        channel_name: &str,
    ) -> Result<&mut InboundChannelState, Trap> {
        self.inbound_channels.get_mut(channel_name).ok_or_else(|| {
            let message = format!("no inbound channel `{}`", channel_name);
            Trap::new(message)
        })
    }

    fn outbound_channel_mut(
        &mut self,
        channel_name: &str,
    ) -> Result<&mut OutboundChannelState, Trap> {
        self.outbound_channels.get_mut(channel_name).ok_or_else(|| {
            let message = format!("no outbound channel `{}`", channel_name);
            Trap::new(message)
        })
    }

    pub(crate) fn push_outbound_message(
        &mut self,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<(), Trap> {
        let channel_state = self.outbound_channel_mut(channel_name)?;
        channel_state.messages.push(message.into());
        Ok(())
    }

    fn poll_inbound_channel(
        &mut self,
        channel_name: &str,
        cx: &mut WasmContext,
    ) -> Result<PollMessage, Trap> {
        let channel_state = self.inbound_channel_mut(channel_name)?;
        let poll_result = channel_state.poll_next();
        self.current_execution()
            .push_inbound_channel_event(channel_name, &poll_result);
        Ok(poll_result.wake_if_pending(cx, || {
            WakerPlacement::InboundChannel(channel_name.to_owned())
        }))
    }

    fn poll_outbound_channel(
        &mut self,
        channel_name: &str,
        flush: bool,
        cx: &mut WasmContext,
    ) -> Result<Poll<()>, Trap> {
        let channel_state = self.outbound_channel_mut(channel_name)?;
        let should_block =
            !channel_state.is_ready() || (flush && !channel_state.messages.is_empty());
        let poll_result = if should_block {
            Poll::Pending
        } else {
            Poll::Ready(())
        };

        self.current_execution()
            .push_outbound_channel_event(channel_name, flush, poll_result);
        Ok(poll_result.wake_if_pending(cx, || {
            WakerPlacement::OutboundChannel(channel_name.to_owned())
        }))
    }

    pub(crate) fn take_outbound_messages(&mut self, channel_name: &str) -> (usize, Vec<Vec<u8>>) {
        let channel_state = self
            .outbound_channels
            .get_mut(channel_name)
            .unwrap_or_else(|| panic!("No outbound channel `{}`", channel_name));

        let wakers = mem::take(&mut channel_state.wakes_on_flush);
        let start_message_idx = channel_state.flushed_messages;
        let messages = mem::take(&mut channel_state.messages);
        channel_state.flushed_messages += messages.len();

        self.schedule_wakers(
            wakers,
            WakeUpCause::Flush {
                channel_name: channel_name.to_owned(),
                message_indexes: start_message_idx..(start_message_idx + messages.len()),
            },
        );
        let messages = messages.into_iter().map(Into::into).collect();
        (start_message_idx, messages)
    }
}

/// Channel-related functions exported to WASM.
impl WorkflowFunctions {
    pub fn get_receiver(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_name_ptr: u32,
        channel_name_len: u32,
    ) -> Result<i64, Trap> {
        let mut ctx = ctx.as_context_mut();
        let memory = ctx.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;
        let result = ctx.data_mut().acquire_inbound_channel(&channel_name);

        crate::log_result!(result, "Acquired inbound channel `{}`", channel_name)
            .into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn poll_next_for_receiver(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        poll_cx: WasmContextPtr,
    ) -> Result<i64, Trap> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx
            .data_mut()
            .poll_inbound_channel(&channel_name, &mut poll_cx);
        let poll_result = crate::log_result!(
            poll_result,
            "Polled inbound channel `{}` with context {:?}",
            channel_name,
            poll_cx
        )?;

        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn get_sender(
        ctx: StoreContextMut<'_, WorkflowData>,
        channel_name_ptr: u32,
        channel_name_len: u32,
    ) -> Result<i64, Trap> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;
        let result = if ctx.data().outbound_channels.contains_key(&channel_name) {
            Ok(())
        } else {
            Err(AccessErrorKind::Unknown)
        };
        crate::log_result!(result, "Acquired outbound channel `{}`", channel_name)
            .into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn poll_ready_for_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        cx: WasmContextPtr,
    ) -> Result<i32, Trap> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;

        let mut cx = WasmContext::new(cx);
        let poll_result = ctx
            .data_mut()
            .poll_outbound_channel(&channel_name, false, &mut cx);
        let poll_result = crate::log_result!(
            poll_result,
            "Polled outbound channel `{}` for readiness",
            channel_name
        )?;

        cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn start_send(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        message_ptr: u32,
        message_len: u32,
    ) -> Result<(), Trap> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;
        let message = copy_bytes_from_wasm(&ctx, &memory, message_ptr, message_len)?;

        let result = ctx.data_mut().push_outbound_message(&channel_name, message);
        crate::log_result!(
            result,
            "Started sending message ({} bytes) over outbound channel `{}`",
            message_len,
            channel_name
        )
    }

    pub fn poll_flush_for_sender(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        channel_name_ptr: u32,
        channel_name_len: u32,
        poll_cx: WasmContextPtr,
    ) -> Result<i32, Trap> {
        let memory = ctx.data().exports().memory;
        let channel_name =
            copy_string_from_wasm(&ctx, &memory, channel_name_ptr, channel_name_len)?;

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx
            .data_mut()
            .poll_outbound_channel(&channel_name, true, &mut poll_cx);
        let poll_result = crate::log_result!(
            poll_result,
            "Polled outbound channel `{}` for flush",
            channel_name
        )?;

        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }
}
