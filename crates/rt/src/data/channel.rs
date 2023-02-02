//! Functionality to manage channel state.

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use std::{
    cmp,
    collections::{HashMap, HashSet},
    error, fmt, mem,
    task::Poll,
};

use super::{
    helpers::{WakerPlacement, Wakers, WorkflowPoll},
    PersistedWorkflowData, WorkflowData,
};
use crate::{
    receipt::{StubEventKind, StubId, WakeUpCause},
    utils::Message,
    workflow::ChannelIds,
};
use tardigrade::{
    channel::SendError,
    handle::{AccessError, AccessErrorKind, Handle, HandleMapKey, HandlePath, SenderAt},
    interface::Interface,
    ChannelId, WakerId,
};

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

/// State of a workflow channel receiver.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReceiverState {
    #[serde(default, skip_serializing_if = "flip_bool")]
    pub(super) is_closed: bool,
    pub(super) received_messages: u64,
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
    pub fn received_message_count(&self) -> u64 {
        self.received_messages
    }

    /// Checks whether the channel is closed.
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /// Is this receiver currently waiting for a message?
    pub fn waits_for_message(&self) -> bool {
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

    /// Returns the number of messages flushed over the sender.
    pub fn flushed_message_count(&self) -> usize {
        self.flushed_messages
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
    pub mapping: ChannelIds,
}

impl ChannelStates {
    pub fn new(channel_ids: ChannelIds, interface: &Interface) -> Self {
        let mut this = Self::default();
        for (path, id_handle) in &channel_ids {
            match id_handle {
                Handle::Receiver(id) => {
                    this.insert_receiver(*id);
                }
                Handle::Sender(id) => {
                    let capacity = interface.handle(SenderAt(path)).unwrap().capacity;
                    this.insert_sender(*id, capacity);
                }
            }
        }
        this.mapping = channel_ids;
        this
    }

    fn insert_receiver(&mut self, id: ChannelId) {
        self.receivers.entry(id).or_insert_with(ReceiverState::new);
    }

    fn insert_sender(&mut self, id: ChannelId, capacity: Option<usize>) {
        let state = self
            .senders
            .entry(id)
            .or_insert_with(|| SenderState::new(capacity));
        state.ensure_capacity(capacity);
        state.ref_count += 1;
    }
}

type ChannelState<'a> = Handle<&'a ReceiverState, &'a SenderState>;

/// Information about channels for a particular workflow interface.
#[derive(Debug, Clone, Copy)]
pub struct Channels<'a> {
    states: &'a ChannelStates,
    mapping: &'a ChannelIds,
}

impl<'a> Channels<'a> {
    pub(super) fn new(states: &'a ChannelStates, mapping: &'a ChannelIds) -> Self {
        Self { states, mapping }
    }

    /// Returns the channel ID of the channel half with the specified name, or `None` if the receiver
    /// is not present.
    pub fn channel_id<'p, P: Into<HandlePath<'p>>>(&self, path: P) -> Option<ChannelId> {
        self.mapping
            .get(&path.into())
            .map(|state| *state.as_ref().factor())
    }

    /// Returns information about a channel handle with the specified path.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel does not exist or has an unexpected type.
    pub fn handle<K: HandleMapKey>(
        &self,
        key: K,
    ) -> Result<K::Output<&'a ReceiverState, &'a SenderState>, AccessError> {
        const ERROR_MSG: &str = "handle is not captured by the workflow";

        let mapping = key
            .with_path(|path| self.mapping.get(&path))
            .ok_or_else(|| AccessErrorKind::Missing.with_location(key))?;
        let state = match mapping {
            Handle::Receiver(id) => {
                let state = self
                    .states
                    .receivers
                    .get(id)
                    .ok_or_else(|| AccessErrorKind::custom(ERROR_MSG).with_location(key))?;
                Handle::Receiver(state)
            }
            Handle::Sender(id) => {
                let state = self
                    .states
                    .senders
                    .get(id)
                    .ok_or_else(|| AccessErrorKind::custom(ERROR_MSG).with_location(key))?;
                Handle::Sender(state)
            }
        };
        K::from_handle(state).ok_or_else(|| AccessErrorKind::KindMismatch.with_location(key))
    }

    /// Iterates over all channel handles.
    pub fn handles(&self) -> impl Iterator<Item = (HandlePath<'a>, ChannelState<'a>)> + '_ {
        self.mapping.iter().map(|(path, mapping)| {
            let state = mapping
                .as_ref()
                .map_receiver(|id| &self.states.receivers[id])
                .map_sender(|id| &self.states.senders[id]);
            (path.as_ref(), state)
        })
    }

    pub(crate) fn to_ids(self) -> ChannelIds {
        self.mapping.clone()
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

    /// Returns information about channels defined in this workflow interface.
    pub fn channels(&self) -> Channels<'_> {
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

/// Handle allowing to manipulate a workflow channel receiver.
pub struct ReceiverActions<'a> {
    data: &'a mut WorkflowData,
    id: ChannelId,
}

impl fmt::Debug for ReceiverActions<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReceiverActions")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl ReceiverActions<'_> {
    /// Drops this receiver. Returns IDs of the wakers that were created polling the receiver
    /// and should be dropped.
    #[tracing::instrument(level = "debug")]
    #[must_use = "Returned wakers must be dropped"]
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn drop(self) -> HashSet<WakerId> {
        let channels = &mut self.data.persisted.channels;
        let channel_state = channels.receivers.get_mut(&self.id).unwrap();
        let wakers = mem::take(&mut channel_state.wakes_on_next_element);
        if !channel_state.is_closed {
            channel_state.is_closed = true;
            self.data
                .current_execution()
                .push_channel_closure(Handle::Receiver(self.id));
        }
        wakers
    }

    /// Polls for the next message for this receiver.
    #[tracing::instrument(level = "debug")]
    pub fn poll_next(&mut self) -> WorkflowPoll<Option<Vec<u8>>> {
        let channels = &mut self.data.persisted.channels;
        let state = channels.receivers.get_mut(&self.id).unwrap();
        // ^ `unwrap()` safety is guaranteed by resource handling

        let poll_result = state.poll_next();
        if let Poll::Ready(maybe_message) = &poll_result {
            let message_len = maybe_message.as_ref().map(Vec::len);
            tracing::debug!(ret.len = message_len);
        }

        self.data
            .current_execution()
            .push_receiver_event(self.id, &poll_result);
        WorkflowPoll::new(poll_result, WakerPlacement::Receiver(self.id))
    }
}

/// Handle allowing to manipulate a workflow channel sender.
pub struct SenderActions<'a> {
    data: &'a mut WorkflowData,
    id: ChannelId,
}

impl fmt::Debug for SenderActions<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SenderActions")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl SenderActions<'_> {
    /// Drops the specified sender. Returns IDs of the wakers that were created polling the receiver
    /// and should be dropped.
    #[tracing::instrument(level = "debug")]
    #[must_use = "Returned wakers must be dropped"]
    #[allow(clippy::missing_panics_doc)] // false positive
    pub fn drop(self) -> HashSet<WakerId> {
        let channels = &mut self.data.persisted.channels;
        let state = channels.senders.get_mut(&self.id).unwrap();
        state.ref_count -= 1;

        if state.ref_count == 0 {
            let wakers = mem::take(&mut state.wakes_on_flush);
            self.data
                .current_execution()
                .push_channel_closure(Handle::Sender(self.id));
            wakers
        } else {
            HashSet::new()
        }
    }

    /// Polls the specified sender for readiness.
    #[tracing::instrument(level = "debug", ret)]
    pub fn poll_ready(&mut self) -> WorkflowPoll<Result<(), SendError>> {
        self.data.poll_sender(self.id, false)
    }

    /// Polls the specified sender for flush.
    #[tracing::instrument(level = "debug", ret)]
    pub fn poll_flush(&mut self) -> WorkflowPoll<Result<(), SendError>> {
        self.data.poll_sender(self.id, true)
    }

    /// Pushes an outbound message into the specified channel.
    #[tracing::instrument(level = "debug", skip(message), fields(message.len = message.len()))]
    pub fn start_send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        let channels = &mut self.data.persisted.channels;
        let channel_state = channels.senders.get_mut(&self.id).unwrap();
        if channel_state.is_closed {
            return Err(SendError::Closed);
        } else if !channel_state.is_ready() {
            return Err(SendError::Full);
        }

        let message_len = message.len();
        channel_state.messages.push(message.into());
        self.data
            .current_execution()
            .push_outbound_message_event(self.id, message_len);
        Ok(())
    }
}

/// Channel-related functionality.
impl WorkflowData {
    pub(crate) fn notify_on_channel_init(&mut self, local_id: ChannelId, id: ChannelId) {
        self.persisted.channels.insert_receiver(id);
        self.persisted.channels.insert_sender(id, Some(1));
        // TODO: what is the appropriate capacity?

        self.current_execution()
            .push_stub_event(StubId::Channel(local_id), StubEventKind::Mapped(Ok(id)));
    }

    /// Starts creating a channel. When the channel initialization is complete,
    /// it will be reported via [`initialize_channel()`].
    ///
    /// As of right now, channel initialization cannot fail, nor can it be cancelled
    /// by the workflow logic.
    ///
    /// [`initialize_channel()`]: crate::engine::RunWorkflow::initialize_channel()
    ///
    /// # Arguments
    ///
    /// - `stub_id` must be unique across all invocations of `create_channel_stub()`
    ///   for a single workflow. E.g., it can be implemented as an incrementing static.
    ///
    /// # Errors
    ///
    /// Returns an error if a channel cannot be created.
    pub fn create_channel_stub(&mut self, stub_id: ChannelId) -> anyhow::Result<()> {
        let workflows = self.services_mut().stubs.as_deref_mut();
        let workflows = workflows.ok_or_else(|| anyhow!("cannot create new channel"))?;
        workflows.stash_channel(stub_id);
        self.current_execution()
            .push_stub_event(StubId::Channel(stub_id), StubEventKind::Created);
        Ok(())
    }

    /// Returns an action handle for the receiver with the specified ID.
    ///
    /// # Panics
    ///
    /// Panics if the receiver with `id` is not owned by the workflow. Note that
    /// receivers for the channel with ID 0 (the "pre-closed" channel) are not considered to be owned
    /// by any workflow, despite a workflow potentially acquiring receiver(s) for this channel
    /// on initialization.
    pub fn receiver(&mut self, id: ChannelId) -> ReceiverActions<'_> {
        assert!(
            self.persisted.channels.receivers.contains_key(&id),
            "receiver not found"
        );
        ReceiverActions { data: self, id }
    }

    /// Returns an action handle for the sender with the specified ID.
    ///
    /// # Panics
    ///
    /// Panics if a sender with `id` is not owned by the workflow. Note that
    /// senders for the channel with ID 0 (the "pre-closed" channel) are not considered to be owned
    /// by any workflow, despite a workflow potentially acquiring sender(s) for this channel
    /// on initialization.
    pub fn sender(&mut self, id: ChannelId) -> SenderActions<'_> {
        assert!(
            self.persisted.channels.senders.contains_key(&id),
            "sender not found"
        );
        SenderActions { data: self, id }
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

    fn poll_sender(
        &mut self,
        channel_id: ChannelId,
        flush: bool,
    ) -> WorkflowPoll<Result<(), SendError>> {
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
        WorkflowPoll::new(poll_result, WakerPlacement::Sender(channel_id))
    }

    pub(crate) fn drain_messages(&mut self) -> HashMap<ChannelId, Vec<Message>> {
        self.persisted.drain_messages()
    }
}
