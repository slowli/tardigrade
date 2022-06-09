//! Functionality to manage channel state.

use wasmtime::Trap;

use std::{collections::HashSet, error, fmt, mem, ops::Range, task::Poll};

use super::{
    helpers::{Message, WakeIfPending, WakerPlacement, WasmContext},
    State,
};
use crate::{WakeUpCause, WakerId};
use tardigrade_shared::{ChannelErrorKind, PollMessage};

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ConsumeErrorKind {
    /// Channel is not registered in the workflow interface.
    UnknownChannel,
    /// No tasks listen to the channel.
    NotListened,
}

impl ConsumeErrorKind {
    pub(crate) fn for_message(self, channel_name: &str, message: Vec<u8>) -> ConsumeError {
        ConsumeError {
            kind: self,
            channel_name: channel_name.to_owned(),
            message,
        }
    }
}

impl fmt::Display for ConsumeErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnknownChannel => "channel is not registered in the workflow interface",
            Self::NotListened => "channel not currently listened to",
        })
    }
}

#[derive(Debug)]
pub struct ConsumeError {
    channel_name: String,
    message: Vec<u8>,
    kind: ConsumeErrorKind,
}

impl fmt::Display for ConsumeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot push message ({} bytes) into channel `{}`: {}",
            self.message.len(),
            self.channel_name,
            self.kind
        )
    }
}

impl error::Error for ConsumeError {}

impl ConsumeError {
    pub fn kind(&self) -> &ConsumeErrorKind {
        &self.kind
    }

    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }

    pub fn into_message(self) -> Vec<u8> {
        self.message
    }
}

#[derive(Debug, Default)]
pub(super) struct InboundChannelState {
    pub is_acquired: bool,
    pub received_messages: usize,
    pub pending_message: Option<Message>,
    pub wakes_on_next_element: HashSet<WakerId>,
}

impl InboundChannelState {
    fn poll_next(&mut self) -> Poll<Option<Vec<u8>>> {
        if let Some(message) = self.pending_message.take() {
            Poll::Ready(Some(message.into()))
        } else {
            Poll::Pending
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct OutboundChannelState {
    pub flushed_messages: usize,
    pub messages: Vec<Message>,
    pub wakes_on_flush: HashSet<WakerId>,
}

impl State {
    pub fn push_inbound_message(
        &mut self,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<(), ConsumeError> {
        let channel_state = match self.inbound_channels.get_mut(channel_name) {
            Some(state) => state,
            None => {
                return Err(ConsumeErrorKind::UnknownChannel.for_message(channel_name, message));
            }
        };
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

    pub fn acquire_inbound_channel(&mut self, channel_name: &str) -> Result<(), ChannelErrorKind> {
        let channel_state = self
            .inbound_channels
            .get_mut(channel_name)
            .ok_or(ChannelErrorKind::Unknown)?;
        if mem::replace(&mut channel_state.is_acquired, true) {
            Err(ChannelErrorKind::AlreadyAcquired)
        } else {
            Ok(())
        }
    }

    fn inbound_channel(&mut self, channel_name: &str) -> Result<&mut InboundChannelState, Trap> {
        self.inbound_channels.get_mut(channel_name).ok_or_else(|| {
            let message = format!("No inbound channel `{}`", channel_name);
            Trap::new(message)
        })
    }

    pub fn has_outbound_channel(&self, channel_name: &str) -> bool {
        self.outbound_channels.contains_key(channel_name)
    }

    fn outbound_channel(&mut self, channel_name: &str) -> Result<&mut OutboundChannelState, Trap> {
        self.outbound_channels.get_mut(channel_name).ok_or_else(|| {
            let message = format!("No outbound channel `{}`", channel_name);
            Trap::new(message)
        })
    }

    pub fn outbound_message_indices(&self, channel_name: &str) -> Range<usize> {
        let channel_state = &self.outbound_channels[channel_name];
        let start = channel_state.flushed_messages;
        let end = start + channel_state.messages.len();
        start..end
    }

    pub fn push_outbound_message(
        &mut self,
        channel_name: &str,
        message: Vec<u8>,
    ) -> Result<(), Trap> {
        let channel_state = self.outbound_channel(channel_name)?;
        channel_state.messages.push(message.into());
        Ok(())
    }

    pub fn poll_inbound_channel(
        &mut self,
        channel_name: &str,
        cx: &mut WasmContext,
    ) -> Result<PollMessage, Trap> {
        let channel_state = self.inbound_channel(channel_name)?;
        Ok(channel_state.poll_next().wake_if_pending(cx, || {
            WakerPlacement::InboundChannel(channel_name.to_owned())
        }))
    }

    pub fn poll_outbound_channel(
        &mut self,
        channel_name: &str,
        flush: bool,
        cx: &mut WasmContext,
    ) -> Result<Poll<()>, Trap> {
        let needs_flushing = self.needs_flushing();
        let channel_state = self.outbound_channel(channel_name)?;

        let poll_result = if needs_flushing || (flush && !channel_state.messages.is_empty()) {
            Poll::Pending
        } else {
            Poll::Ready(())
        };
        Ok(poll_result.wake_if_pending(cx, || {
            WakerPlacement::OutboundChannel(channel_name.to_owned())
        }))
    }

    pub fn needs_flushing(&self) -> bool {
        self.outbound_channels
            .values()
            .any(|channel_state| !channel_state.wakes_on_flush.is_empty())
    }

    pub fn take_outbound_messages(&mut self, channel_name: &str) -> Vec<Vec<u8>> {
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
        messages.into_iter().map(Into::into).collect()
    }
}
