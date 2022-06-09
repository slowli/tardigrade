//! Time utilities.

use chrono::{DateTime, Duration, TimeZone, Utc};
use wasmtime::Trap;

use std::{
    collections::{HashMap, HashSet},
    mem,
    task::Poll,
};

use super::{
    helpers::{WakeIfPending, WakerPlacement, Wakers, WasmContext},
    State,
};
use crate::{TimerId, WakeUpCause, WakerId};
use tardigrade_shared::{TimerDefinition, TimerKind};

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

    fn insert(&mut self, name: String, definition: TimerDefinition) -> TimerId {
        let id = self.next_timer_id;
        self.timers.insert(
            id,
            TimerState {
                name,
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
    pub fn timer_definition(&self, kind: TimerKind, value: i64) -> TimerDefinition {
        let expires_at = match kind {
            TimerKind::Duration => self.timers.current_time + Duration::milliseconds(value),
            TimerKind::Instant => Utc.timestamp_millis(value),
        };
        TimerDefinition { expires_at }
    }

    pub fn timers(&self) -> &Timers {
        &self.timers
    }

    pub fn create_timer(&mut self, name: String, definition: TimerDefinition) -> TimerId {
        let timer_id = self.timers.insert(name, definition);
        let current_task = self.current_execution.as_mut().unwrap();
        current_task.register_timer(timer_id);
        timer_id
    }

    pub fn drop_timer(&mut self, timer_id: TimerId) -> Result<(), Trap> {
        if self.timers.get(timer_id).is_none() {
            let message = format!("Timer ID {} is not defined", timer_id);
            return Err(Trap::new(message));
        }
        let current_task = self.current_execution.as_mut().unwrap();
        current_task.register_timer_drop(timer_id);
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

    pub fn poll_timer(
        &mut self,
        timer_id: TimerId,
        cx: &mut WasmContext,
    ) -> Result<Poll<()>, Trap> {
        let poll_result = self.timers.poll(timer_id)?;
        Ok(poll_result.wake_if_pending(cx, || WakerPlacement::Timer(timer_id)))
    }
}
