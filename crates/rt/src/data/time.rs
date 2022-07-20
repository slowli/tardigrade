//! Time utilities.

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use wasmtime::{StoreContextMut, Trap};

use std::{
    collections::{HashMap, HashSet},
    mem,
    task::Poll,
};

use super::{
    helpers::{WakeIfPending, WakerPlacement, Wakers, WasmContext, WasmContextPtr},
    WorkflowData, WorkflowFunctions,
};
use crate::{
    receipt::{ResourceEventKind, ResourceId, WakeUpCause},
    utils::WasmAllocator,
    TimerId, WakerId,
};
use tardigrade_shared::{abi::IntoWasm, TimerDefinition};

/// State of a [`Workflow`](crate::Workflow) timer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimerState {
    definition: TimerDefinition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    wakes_on_completion: HashSet<WakerId>,
}

impl TimerState {
    /// Returns a copy of the timer definition.
    pub fn definition(&self) -> TimerDefinition {
        self.definition
    }

    /// Returns timestamp when the timer was completed.
    pub fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }

    fn poll(&self) -> Poll<DateTime<Utc>> {
        if let Some(timestamp) = self.completed_at {
            Poll::Ready(timestamp)
        } else {
            Poll::Pending
        }
    }

    fn complete(&mut self, current_timestamp: DateTime<Utc>) -> HashSet<WakerId> {
        self.completed_at = Some(current_timestamp);
        mem::take(&mut self.wakes_on_completion)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Timers {
    current_time: DateTime<Utc>,
    timers: HashMap<TimerId, TimerState>,
    next_timer_id: TimerId,
}

impl Timers {
    pub(super) fn new() -> Self {
        Self {
            current_time: Utc::now(),
            timers: HashMap::new(),
            next_timer_id: 0,
        }
    }

    pub fn get(&self, id: TimerId) -> Option<&TimerState> {
        self.timers.get(&id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (TimerId, &TimerState)> + '_ {
        self.timers.iter().map(|(id, state)| (*id, state))
    }

    fn insert(&mut self, definition: TimerDefinition) -> TimerId {
        let id = self.next_timer_id;
        self.timers.insert(
            id,
            TimerState {
                definition,
                completed_at: None,
                wakes_on_completion: HashSet::new(),
            },
        );
        self.next_timer_id += 1;
        id
    }

    pub(super) fn remove(&mut self, timer_id: TimerId) {
        self.timers.remove(&timer_id);
    }

    fn poll(&mut self, id: TimerId) -> Result<Poll<DateTime<Utc>>, Trap> {
        let timer_state = self
            .timers
            .get_mut(&id)
            .ok_or_else(|| Trap::new(format!("Timeout with ID {} is not registered", id)))?;
        Ok(timer_state.poll())
    }

    pub(super) fn place_waker(&mut self, id: TimerId, waker: WakerId) {
        self.timers
            .get_mut(&id)
            .unwrap()
            .wakes_on_completion
            .insert(waker);
    }

    pub(super) fn remove_wakers(&mut self, wakers: &HashSet<WakerId>) {
        for state in self.timers.values_mut() {
            state
                .wakes_on_completion
                .retain(|waker_id| !wakers.contains(waker_id));
        }
    }

    pub fn current_time(&self) -> DateTime<Utc> {
        self.current_time
    }

    /// **NB.** The returned iterator must be completely consumed!
    fn set_current_time(
        &mut self,
        time: DateTime<Utc>,
    ) -> impl Iterator<Item = (TimerId, HashSet<WakerId>)> + '_ {
        self.current_time = time;
        self.timers.iter_mut().filter_map(move |(&id, state)| {
            if state.definition.expires_at <= time {
                Some((id, state.complete(time)))
            } else {
                None
            }
        })
    }
}

impl WorkflowData {
    fn timer_definition(timestamp_millis: i64) -> TimerDefinition {
        let expires_at = Utc.timestamp_millis(timestamp_millis);
        TimerDefinition { expires_at }
    }

    pub(crate) fn timers(&self) -> &Timers {
        &self.timers
    }

    fn create_timer(&mut self, definition: TimerDefinition) -> TimerId {
        let timer_id = self.timers.insert(definition);
        self.current_execution()
            .push_resource_event(ResourceId::Timer(timer_id), ResourceEventKind::Created);
        timer_id
    }

    fn drop_timer(&mut self, timer_id: TimerId) -> Result<(), Trap> {
        if self.timers.get(timer_id).is_none() {
            let message = format!("Timer ID {} is not defined", timer_id);
            return Err(Trap::new(message));
        }
        self.current_execution()
            .push_resource_event(ResourceId::Timer(timer_id), ResourceEventKind::Dropped);
        Ok(())
    }

    pub(crate) fn set_current_time(&mut self, time: DateTime<Utc>) {
        let wakers_by_timer = self.timers.set_current_time(time);
        for (id, wakers) in wakers_by_timer {
            let cause = WakeUpCause::Timer { id };
            crate::trace!("Scheduled wakers {:?} with cause {:?}", wakers, cause);
            self.waker_queue.push(Wakers::new(wakers, cause));
        }
    }

    fn poll_timer(
        &mut self,
        timer_id: TimerId,
        cx: &mut WasmContext,
    ) -> Result<Poll<DateTime<Utc>>, Trap> {
        let poll_result = self.timers.poll(timer_id)?;
        self.current_execution().push_resource_event(
            ResourceId::Timer(timer_id),
            ResourceEventKind::Polled(poll_result.map(drop)),
        );
        Ok(poll_result.wake_if_pending(cx, || WakerPlacement::Timer(timer_id)))
    }
}

/// Timer-related functions exported to WASM.
impl WorkflowFunctions {
    #[allow(clippy::needless_pass_by_value)] // for uniformity with other functions
    pub fn current_timestamp(ctx: StoreContextMut<'_, WorkflowData>) -> i64 {
        ctx.data().clock.now().timestamp_millis()
    }

    pub fn create_timer(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        timestamp_millis: i64,
    ) -> TimerId {
        let definition = WorkflowData::timer_definition(timestamp_millis);
        let timer_id = ctx.data_mut().create_timer(definition);
        crate::trace!(
            "Created timer {} with definition {:?}",
            timer_id,
            definition
        );
        timer_id
    }

    pub fn drop_timer(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        timer_id: TimerId,
    ) -> Result<(), Trap> {
        let result = ctx.data_mut().drop_timer(timer_id);
        crate::log_result!(result, "Dropped timer {}", timer_id)
    }

    pub fn poll_timer(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        timer_id: TimerId,
        poll_cx: WasmContextPtr,
    ) -> Result<i64, Trap> {
        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx.data_mut().poll_timer(timer_id, &mut poll_cx);
        let poll_result = crate::log_result!(
            poll_result,
            "Polled timer {} with context {:?}",
            timer_id,
            poll_cx
        )?;
        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }
}
