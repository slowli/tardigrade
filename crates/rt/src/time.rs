//! Time utilities.

use chrono::{DateTime, Duration, TimeZone, Utc};
use wasmtime::Trap;

use std::{
    collections::{HashMap, HashSet},
    mem,
    task::Poll,
};

use crate::state::WakerId;
use tardigrade_shared::TimerKind;

pub type TimerId = u64;

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct Timer {
    pub expires_at: DateTime<Utc>,
}

impl Timer {
    pub fn from_raw(kind: TimerKind, value: i64) -> Self {
        let expires_at = match kind {
            TimerKind::Duration => Utc::now() + Duration::milliseconds(value),
            TimerKind::Instant => Utc.timestamp_millis(value),
        };
        Self { expires_at }
    }
}

#[derive(Debug, Clone)]
pub struct TimerState {
    name: String,
    definition: Timer,
    is_completed: bool,
    wakes_on_completion: HashSet<WakerId>,
}

impl TimerState {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn definition(&self) -> Timer {
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
    pub fn new() -> Self {
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

    pub fn insert(&mut self, name: String, definition: Timer) -> TimerId {
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

    pub fn remove(&mut self, timer_id: TimerId) {
        self.timers.remove(&timer_id);
    }

    pub fn poll(&mut self, id: TimerId) -> Result<Poll<()>, Trap> {
        let timer_state = self
            .timers
            .get_mut(&id)
            .ok_or_else(|| Trap::new(format!("Timeout with ID {} is not registered", id)))?;
        Ok(timer_state.poll())
    }

    pub fn place_waker(&mut self, id: TimerId, waker: WakerId) {
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
    pub fn set_current_time(
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
