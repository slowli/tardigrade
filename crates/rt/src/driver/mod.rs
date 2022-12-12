//! [`Driver`] for a [`WorkflowManager`](manager::WorkflowManager) that drives
//! workflows contained in the manager to completion.
//!
//! See `Driver` docs for examples of usage.

use chrono::{DateTime, Utc};
use futures::{channel::mpsc, future, stream::FusedStream, FutureExt, StreamExt};

#[cfg(test)]
mod tests;

use crate::{
    manager::{AsManager, TickResult},
    Schedule,
};

/// Terminal status of a [`WorkflowManager`].
///
/// [`WorkflowManager`]: manager::WorkflowManager
#[derive(Debug)]
#[non_exhaustive]
pub enum Termination {
    /// The workflow manager is finished: all workflows managed by it have run to completion.
    Finished,
    /// The manager has stalled: its progress does not depend on any external futures
    /// (inbound messages or timers).
    Stalled,
}

/// Environment for driving workflow execution in a [`WorkflowManager`].
///
/// [`WorkflowManager`]: manager::WorkflowManager
///
/// # Error handling
///
/// Erroneous workflow executions in [`Self::drive()`] lead to the corresponding workflow
/// getting the [errored state], so that it will not be executed again.
/// To drop incoming messages that may have led to an error,
/// call [`Self::drop_erroneous_messages()`] before [`Self::drive()`]. Dropping a message
/// means that from the workflow perspective, the message was never received in the first place,
/// and all progress resulting from receiving the message is lost (new tasks, timers, etc.).
/// Whether this makes sense, depends on a use case; e.g., it seems reasonable to roll back
/// deserialization errors for dynamically typed workflows.
///
/// [errored state]: manager::WorkflowManager#workflow-lifecycle
///
/// # Examples
///
/// ```
/// use async_std::task;
/// use futures::prelude::*;
/// use tardigrade::handle::{ReceiverAt, SenderAt, WithIndexing};
/// # use tardigrade::WorkflowId;
/// use tardigrade_rt::{driver::Driver, manager::WorkflowManager, AsyncIoScheduler};
/// # use tardigrade_rt::{engine::Wasmtime, storage::LocalStorage};
/// #
/// # async fn test_wrapper(
/// #     manager: WorkflowManager<Wasmtime, AsyncIoScheduler, LocalStorage>,
/// #     workflow_id: WorkflowId,
/// # ) -> anyhow::Result<()> {
/// // Assume we have a dynamically typed workflow:
/// let mut manager: WorkflowManager<_, AsyncIoScheduler, _> = // ...
/// #   manager;
/// let mut workflow = manager.workflow(workflow_id).await.unwrap();
///
/// // First, create a driver to execute the workflow in.
/// let mut driver = Driver::new();
/// // Take relevant channels from the workflow and convert them to async form.
/// let mut handle = workflow.handle().await.with_indexing();
/// let mut commands_sx = handle.remove(ReceiverAt("commands"))
///     .unwrap()
///     .into_sink(&mut driver);
/// let events_rx = handle.remove(SenderAt("events"))
///     .unwrap()
///     .into_stream(&mut driver);
/// drop(handle);
///
/// // Run the environment in a separate task.
/// task::spawn(async move { driver.drive(&mut manager).await });
/// // Let's send a message via an inbound channel...
/// let message = b"hello".to_vec();
/// commands_sx.send(message).await?;
///
/// // ...and wait for some outbound messages
/// let events: Vec<Vec<u8>> = events_rx.take(2).try_collect().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Driver {
    commits_rx: mpsc::Receiver<()>,
    results_sx: Option<mpsc::UnboundedSender<TickResult>>,
    drop_erroneous_messages: bool,
}

impl Driver {
    /// FIXME
    pub fn new(commits_rx: mpsc::Receiver<()>) -> Self {
        Self {
            commits_rx,
            results_sx: None,
            drop_erroneous_messages: false,
        }
    }

    /// Returns the receiver of [`TickResult`]s generated during workflow execution.
    pub fn tick_results(&mut self) -> mpsc::UnboundedReceiver<TickResult> {
        let (sx, rx) = mpsc::unbounded();
        self.results_sx = Some(sx);
        rx
    }

    /// Indicates that the environment should drop any inbound messages that lead
    /// to execution errors.
    pub fn drop_erroneous_messages(&mut self) {
        self.drop_erroneous_messages = true;
    }

    /// Executes the provided [`WorkflowManager`] until all workflows in it have completed
    /// or errored. As the workflows execute, outbound messages and
    /// [`TickResult`]s will be sent using respective channels.
    ///
    /// Note that it is possible to cancel this future (e.g., by [`select`]ing between it
    /// and a cancellation signal) and continue working with the provided workflow manager.
    ///
    /// [`WorkflowManager`]: manager::WorkflowManager
    /// [`select`]: futures::select
    pub async fn drive<M>(mut self, manager: &mut M) -> Termination
    where
        M: AsManager,
        M::Clock: Schedule,
    {
        // Technically, we don't need a mutable ref to a manager; we use it to ensure that
        // the manager isn't driven elsewhere, which can lead to unexpected behavior.
        loop {
            if let Some(termination) = self.tick(manager).await {
                return termination;
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all, ret)]
    async fn tick<M>(&mut self, manager: &M) -> Option<Termination>
    where
        M: AsManager,
        M::Clock: Schedule,
    {
        let manager_ref = manager.as_manager();
        let nearest_timer_expiration = self.tick_manager(manager).await;
        if nearest_timer_expiration.is_none() {
            let no_workflows = manager_ref.workflow_count().await == 0;
            if self.commits_rx.is_terminated() || no_workflows {
                let termination = if no_workflows {
                    Termination::Finished
                } else {
                    Termination::Stalled
                };
                return Some(termination);
            }
        }

        // TODO: cache `timer_event`?
        let mut commit_event = self.commits_rx.select_next_some();
        let timer_event = nearest_timer_expiration.map_or_else(
            || future::pending().left_future(),
            |timestamp| {
                let timer = manager_ref.clock.create_timer(timestamp);
                timer.right_future()
            },
        );

        futures::select! {
            _ = commit_event => {
               // We just need the fact that a commit has occurred.
            }
            timestamp = timer_event.fuse() => {
                manager_ref.set_current_time(timestamp).await;
            }
        }
        None
    }

    async fn tick_manager<M: AsManager>(&mut self, manager: &M) -> Option<DateTime<Utc>> {
        loop {
            let tick_result = match manager.as_manager().tick().await {
                Ok(result) => result,
                Err(blocked) => break blocked.nearest_timer_expiration(),
            };

            if self.drop_erroneous_messages {
                if let Err(handle) = tick_result.as_ref() {
                    // Concurrency errors should not occur if the driver is used properly
                    // (i.e., the workflows are not mutated externally). That's why the errors
                    // are ignored below.
                    let mut dropped_messages = false;
                    for message in handle.messages() {
                        message.drop_for_workflow().await.ok();
                        dropped_messages = true;
                    }
                    if dropped_messages {
                        handle.consider_repaired_by_ref().await.ok();
                    }
                }
            }
            let tick_result = tick_result.drop_handle();
            if let Some(sx) = &self.results_sx {
                sx.unbounded_send(tick_result).ok();
            }
        }
    }
}
