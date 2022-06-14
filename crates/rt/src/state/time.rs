//! Time utilities.

use chrono::{DateTime, Duration, TimeZone, Utc};
use wasmtime::{Caller, Trap};

use std::{
    collections::{HashMap, HashSet},
    mem,
    task::Poll,
};

use super::{
    helpers::{WakeIfPending, WakerPlacement, Wakers, WasmContext, WasmContextPtr},
    State, StateFunctions,
};
use crate::{
    receipt::{ResourceEventKind, ResourceId, WakeUpCause},
    utils::WasmAllocator,
    TimerId, WakerId,
};
use tardigrade_shared::{IntoWasm, TimerDefinition, TimerKind, TryFromWasm};

#[derive(Debug, Clone)]
pub struct TimerState {
    name: String,
    definition: TimerDefinition,
    is_completed: bool,
    wakes_on_completion: HashSet<WakerId>,
}

impl TimerState {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn definition(&self) -> TimerDefinition {
        self.definition
    }

    pub fn is_completed(&self) -> bool {
        self.is_completed
    }

    fn poll(&mut self) -> Poll<()> {
        if self.is_completed {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    fn complete(&mut self) -> HashSet<WakerId> {
        self.is_completed = true;
        mem::take(&mut self.wakes_on_completion)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Timers {
    current_time: DateTime<Utc>,
    timers: HashMap<TimerId, TimerState>,
    next_timer_id: TimerId,
}

impl Timers {
    pub(super) fn new() -> Self {
        Self {
            current_time: Utc::now(), // FIXME: generalize
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
                name: String::new(), // FIXME remove field
                definition,
                is_completed: false,
                wakes_on_completion: HashSet::new(),
            },
        );
        self.next_timer_id += 1;
        id
    }

    pub(super) fn remove(&mut self, timer_id: TimerId) {
        self.timers.remove(&timer_id);
    }

    fn poll(&mut self, id: TimerId) -> Result<Poll<()>, Trap> {
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
                Some((id, state.complete()))
            } else {
                None
            }
        })
    }
}

impl State {
    fn timer_definition(&self, kind: TimerKind, value: i64) -> TimerDefinition {
        let expires_at = match kind {
            TimerKind::Duration => self.timers.current_time + Duration::milliseconds(value),
            TimerKind::Instant => Utc.timestamp_millis(value),
        };
        TimerDefinition { expires_at }
    }

    pub fn timers(&self) -> &Timers {
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

    pub fn set_current_time(&mut self, time: DateTime<Utc>) {
        let wakers_by_timer = self.timers.set_current_time(time);
        for (id, wakers) in wakers_by_timer {
            let cause = WakeUpCause::Timer { id };
            crate::trace!("Scheduled wakers {:?} with cause {:?}", wakers, cause);
            self.waker_queue.push(Wakers::new(wakers, cause));
        }
    }

    fn poll_timer(&mut self, timer_id: TimerId, cx: &mut WasmContext) -> Result<Poll<()>, Trap> {
        let poll_result = self.timers.poll(timer_id)?;
        self.current_execution().push_resource_event(
            ResourceId::Timer(timer_id),
            ResourceEventKind::Polled(poll_result),
        );
        Ok(poll_result.wake_if_pending(cx, || WakerPlacement::Timer(timer_id)))
    }
}

/// Timer-related functions exported to WASM.
impl StateFunctions {
    pub fn create_timer(
        mut caller: Caller<'_, State>,
        timer_kind: i32,
        timer_value: i64,
    ) -> Result<TimerId, Trap> {
        let timer_kind =
            TimerKind::try_from_wasm(timer_kind).map_err(|err| Trap::new(err.to_string()));
        let timer_kind = crate::log_result!(timer_kind, "Parsed `TimerKind`")?;

        let definition = caller.data().timer_definition(timer_kind, timer_value);
        let timer_id = caller.data_mut().create_timer(definition);
        crate::trace!(
            "Created timer {} with definition {:?}",
            timer_id,
            definition
        );
        Ok(timer_id)
    }

    pub fn drop_timer(mut caller: Caller<'_, State>, timer_id: TimerId) -> Result<(), Trap> {
        let result = caller.data_mut().drop_timer(timer_id);
        crate::log_result!(result, "Dropped timer {}", timer_id)
    }

    pub fn poll_timer(
        mut caller: Caller<'_, State>,
        timer_id: TimerId,
        cx: WasmContextPtr,
    ) -> Result<i32, Trap> {
        let mut cx = WasmContext::new(cx);
        let poll_result = caller.data_mut().poll_timer(timer_id, &mut cx);
        let poll_result = crate::log_result!(
            poll_result,
            "Polled timer {} with context {:?}",
            timer_id,
            cx
        )?;
        cx.save_waker(&mut caller)?;
        poll_result.into_wasm(&mut WasmAllocator::new(caller))
    }
}
