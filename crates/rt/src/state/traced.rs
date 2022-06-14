//! Traced futures.

use wasmtime::{Caller, Trap};

use std::{collections::HashMap, task::Poll};

use super::{State, StateFunctions};
use crate::{
    receipt::{ExecutedFunction, ResourceEventKind, ResourceId},
    utils::copy_string_from_wasm,
};
use tardigrade_shared::{FutureId, TracedFutureUpdate, TryFromWasm};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TracedFutureStage {
    /// Future is created, but was not yet polled.
    Created,
    /// Future was polled at least once, but did not complete.
    Polling,
    /// Future has been polled to completion.
    Completed,
    /// Future was dropped in WASM.
    Dropped,
    /// Future is dropped before completion.
    Abandoned,
}

#[derive(Debug, Clone)]
pub struct TracedFutureState {
    name: String,
    stage: TracedFutureStage,
    created_by: ExecutedFunction, // TODO: is this really useful?
}

impl TracedFutureState {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn stage(&self) -> TracedFutureStage {
        self.stage
    }

    pub fn created_by(&self) -> &ExecutedFunction {
        &self.created_by
    }

    fn update(&mut self, update: TracedFutureUpdate) -> Result<(), Trap> {
        use self::TracedFutureStage::*;

        let new_stage = match (self.stage, update) {
            (Created | Polling, TracedFutureUpdate::Polling) => Polling,
            (Polling, TracedFutureUpdate::Polled(Poll::Pending)) => Polling,
            (Polling, TracedFutureUpdate::Polled(Poll::Ready(_))) => Completed,
            (Created | Polling, TracedFutureUpdate::Dropped) => Abandoned,
            (Completed, TracedFutureUpdate::Dropped) => Dropped,

            _ => {
                let message = format!(
                    "unexpected future transition from stage {:?} with update {:?}",
                    self.stage, update
                );
                return Err(Trap::new(message));
            }
        };
        self.stage = new_stage;
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub(super) struct TracedFutures {
    inner: HashMap<FutureId, TracedFutureState>,
    next_future_id: FutureId,
}

impl TracedFutures {
    fn push(&mut self, name: String, created_by: ExecutedFunction) -> FutureId {
        let future_id = self.next_future_id;
        self.next_future_id += 1;
        self.inner.insert(
            future_id,
            TracedFutureState {
                name,
                stage: TracedFutureStage::Created,
                created_by,
            },
        );
        future_id
    }

    pub(super) fn remove(&mut self, future_id: FutureId) {
        self.inner.remove(&future_id);
    }
}

impl State {
    fn create_traced_future(&mut self, name: String) -> FutureId {
        let executed_function = self.current_execution().function.clone();
        let future_id = self.traced_futures.push(name, executed_function);
        self.current_execution().push_resource_event(
            ResourceId::TracedFuture(future_id),
            ResourceEventKind::Created,
        );
        future_id
    }

    fn update_traced_future(
        &mut self,
        future_id: FutureId,
        update: TracedFutureUpdate,
    ) -> Result<(), Trap> {
        let state = self
            .traced_futures
            .inner
            .get_mut(&future_id)
            .ok_or_else(|| {
                let message = format!("no traced future {}", future_id);
                Trap::new(message)
            })?;
        state.update(update)?;

        let event = match update {
            TracedFutureUpdate::Polled(res) => ResourceEventKind::Polled(res),
            TracedFutureUpdate::Dropped => ResourceEventKind::Dropped,
            _ => return Ok(()),
        };
        self.current_execution()
            .push_resource_event(ResourceId::TracedFuture(future_id), event);
        Ok(())
    }

    pub fn traced_futures(&self) -> impl Iterator<Item = (FutureId, &TracedFutureState)> + '_ {
        self.traced_futures
            .inner
            .iter()
            .map(|(id, state)| (*id, state))
    }
}

impl StateFunctions {
    pub fn create_traced_future(
        mut caller: Caller<'_, State>,
        future_name_ptr: u32,
        future_name_len: u32,
    ) -> Result<FutureId, Trap> {
        let memory = caller.data().exports().memory;
        let future_name =
            copy_string_from_wasm(&caller, &memory, future_name_ptr, future_name_len)?;
        Ok(caller.data_mut().create_traced_future(future_name))
    }

    pub fn update_traced_future(
        mut caller: Caller<'_, State>,
        future_id: FutureId,
        update: i32,
    ) -> Result<(), Trap> {
        let update = TracedFutureUpdate::try_from_wasm(update).map_err(Trap::new)?;
        caller.data_mut().update_traced_future(future_id, update)
    }
}
