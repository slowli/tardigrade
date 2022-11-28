//! Time utilities.

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use std::{
    collections::{HashMap, HashSet},
    mem,
    task::Poll,
};

use super::{
    helpers::{WakerPlacement, Wakers, WorkflowPoll},
    PersistedWorkflowData, WorkflowData,
};
use crate::receipt::{ResourceEventKind, ResourceId, WakeUpCause};
use tardigrade::{TimerDefinition, TimerId, WakerId};

/// State of a workflow timer.
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
    last_known_time: DateTime<Utc>,
    timers: HashMap<TimerId, TimerState>,
    next_timer_id: TimerId,
}

impl Timers {
    pub(super) fn new(now: DateTime<Utc>) -> Self {
        Self {
            last_known_time: now,
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
                completed_at: if definition.expires_at > self.last_known_time {
                    None
                } else {
                    Some(self.last_known_time)
                },
                wakes_on_completion: HashSet::new(),
            },
        );
        self.next_timer_id += 1;
        id
    }

    pub(super) fn remove(&mut self, timer_id: TimerId) {
        self.timers.remove(&timer_id);
    }

    fn poll(&mut self, id: TimerId) -> anyhow::Result<Poll<DateTime<Utc>>> {
        let timer_state = self
            .timers
            .get_mut(&id)
            .ok_or_else(|| anyhow!("Timeout with ID {id} is not registered"))?;
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

    pub fn last_known_time(&self) -> DateTime<Utc> {
        self.last_known_time
    }

    /// **NB.** The returned iterator must be completely consumed!
    pub(super) fn set_current_time(
        &mut self,
        time: DateTime<Utc>,
    ) -> impl Iterator<Item = (TimerId, HashSet<WakerId>)> + '_ {
        self.last_known_time = time;
        self.timers.iter_mut().filter_map(move |(&id, state)| {
            if state.definition.expires_at <= time {
                Some((id, state.complete(time)))
            } else {
                None
            }
        })
    }
}

impl PersistedWorkflowData {
    pub(crate) fn set_current_time(&mut self, time: DateTime<Utc>) {
        let wakers_by_timer = self.timers.set_current_time(time);
        for (id, wakers) in wakers_by_timer {
            let cause = WakeUpCause::Timer { id };
            tracing::debug!(?wakers, ?cause, "scheduled wakers");
            self.waker_queue.push(Wakers::new(wakers, cause));
        }
    }
}

impl WorkflowData {
    /// Returns the current timestamp.
    #[tracing::instrument(level = "debug", skip(self), ret)]
    pub fn current_timestamp(&self) -> DateTime<Utc> {
        self.services().clock.now()
    }

    /// Creates a new timer.
    #[tracing::instrument(level = "debug", skip(self), ret)]
    pub fn create_timer(&mut self, definition: TimerDefinition) -> TimerId {
        let timer_id = self.persisted.timers.insert(definition);
        self.current_execution()
            .push_resource_event(ResourceId::Timer(timer_id), ResourceEventKind::Created);
        timer_id
    }

    /// Drops the specified timer.
    #[tracing::instrument(level = "debug", skip(self), err)]
    pub fn drop_timer(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
        if self.persisted.timers.get(timer_id).is_none() {
            let err = anyhow!("Timer ID {timer_id} is not defined");
            return Err(err);
        }
        self.current_execution()
            .push_resource_event(ResourceId::Timer(timer_id), ResourceEventKind::Dropped);
        Ok(())
    }

    /// Polls the specified timer.
    #[tracing::instrument(level = "debug", skip(self), ret, err)]
    pub fn poll_timer(&mut self, timer_id: TimerId) -> anyhow::Result<WorkflowPoll<DateTime<Utc>>> {
        let poll_result = self.persisted.timers.poll(timer_id)?;
        self.current_execution().push_resource_event(
            ResourceId::Timer(timer_id),
            ResourceEventKind::Polled(poll_result.map(drop)),
        );
        Ok(WorkflowPoll::new(
            poll_result,
            WakerPlacement::Timer(timer_id),
        ))
    }
}
