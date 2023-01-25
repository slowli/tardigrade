//! Driving workflows in a `Runtime` to completion.

use chrono::{DateTime, TimeZone, Utc};
use futures::{
    channel::mpsc,
    future::{self, Either},
    stream::FusedStream,
    FutureExt, StreamExt,
};

use std::mem;

use crate::{
    handle::{AnyWorkflowHandle, StorageRef},
    runtime::{traits::IntoRuntime, AsRuntime, TickResult},
    storage::{CommitStream, Storage, Streaming},
    Schedule, TimerFuture,
};
use tardigrade::WorkflowId;

/// Terminal status of [driving] a [`Runtime`].
///
/// [driving]: crate::runtime::Runtime::drive()
/// [`Runtime`]: crate::runtime::Runtime
#[derive(Debug)]
#[non_exhaustive]
pub enum Termination {
    /// The workflow runtime is finished: all workflows managed by it have run to completion.
    Finished,
    /// The runtime has stalled: its progress does not depend on any external futures
    /// (inbound messages or timers).
    Stalled,
}

struct CachedTimer {
    expires_at: DateTime<Utc>,
    timer: Option<TimerFuture>,
}

impl Default for CachedTimer {
    fn default() -> Self {
        Self {
            expires_at: Utc.timestamp_nanos(0),
            timer: None,
        }
    }
}

/// Configuration for driving workflow execution in a [`Runtime`].
///
/// [`Runtime`]: crate::runtime::Runtime
///
/// # Error handling
///
/// Erroneous workflow executions in [`Runtime::drive()`] lead to the corresponding workflow
/// getting the [errored state], so that it will not be executed again.
/// To drop incoming messages that may have led to an error,
/// call [`Self::drop_erroneous_messages()`]. Dropping a message
/// means that from the workflow perspective, the message was never received in the first place,
/// and all progress resulting from receiving the message is lost (new tasks, timers, etc.).
/// Whether this makes sense, depends on a use case; e.g., it seems reasonable to roll back
/// deserialization errors for dynamically typed workflows.
///
/// [`Runtime::drive()`]: crate::runtime::Runtime::drive()
/// [errored state]: crate::runtime::Runtime#workflow-lifecycle
///
/// # Examples
///
/// ```
/// use async_std::task;
/// use futures::prelude::*;
/// # use std::sync::Arc;
/// use tardigrade::handle::{ReceiverAt, SenderAt, WithIndexing};
/// # use tardigrade::WorkflowId;
/// use tardigrade_rt::{
///     runtime::{DriveConfig, Runtime},
///     storage::{LocalStorage, Streaming, CommitStream},
///     AsyncIoScheduler,
/// };
/// # use tardigrade_rt::engine::Wasmtime;
///
/// // We need a storage that can stream commit / message events;
/// // this one is based on `LocalStorage`.
/// type StreamingStorage = Streaming<Arc<LocalStorage>>;
///
/// # async fn test_wrapper(
/// #     runtime: Runtime<Wasmtime, AsyncIoScheduler, StreamingStorage>,
/// #     commits_rx: CommitStream,
/// #     workflow_id: WorkflowId,
/// # ) -> anyhow::Result<()> {
/// // Assume we have a dynamically typed workflow:
/// let runtime: Runtime<_, AsyncIoScheduler, StreamingStorage> = // ...
/// #   runtime;
/// let workflow = runtime.storage().workflow(workflow_id).await.unwrap();
/// // ...and a commits stream from the storage:
/// let mut commits_rx: CommitStream = // ...
/// #   commits_rx;
///
/// // Take relevant channels from the workflow and convert them to async form.
/// let mut handle = workflow.handle().await.with_indexing();
/// let commands_sx = handle.remove(ReceiverAt("commands")).unwrap();
/// let events_rx = handle.remove(SenderAt("events")).unwrap();
/// let events_rx = events_rx.stream_messages(0..);
///
/// // Run the runtime in a separate task.
/// let runtime = runtime.clone();
/// task::spawn(async move {
///     runtime.drive(&mut commits_rx, DriveConfig::new()).await
/// });
/// // Let's send a message via an inbound channel...
/// let message = b"hello".to_vec();
/// commands_sx.send(message).await?;
///
/// // ...and wait for some outbound messages
/// let events: Vec<_> = events_rx.take(2).collect().await;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default)]
pub struct DriveConfig {
    results_sx: Option<mpsc::UnboundedSender<TickResult>>,
    wait_for_workflows: bool,
    drop_erroneous_messages: bool,
}

impl DriveConfig {
    /// Creates a new driver configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the receiver of [`TickResult`]s generated during workflow execution.
    pub fn tick_results(&mut self) -> mpsc::UnboundedReceiver<TickResult> {
        let (sx, rx) = mpsc::unbounded();
        self.results_sx = Some(sx);
        rx
    }

    /// Indicates that the driver should drop any inbound messages that lead
    /// to execution errors.
    pub fn drop_erroneous_messages(&mut self) {
        self.drop_erroneous_messages = true;
    }

    /// Indicates that the driver should continue waiting for new workflows or workflow updates
    /// (e.g., repairing a workflow) when all existing workflows are terminated or errored.
    pub fn wait_for_workflows(&mut self) {
        self.wait_for_workflows = true;
    }

    /// Executes the provided [`Runtime`] until all workflows in it have completed
    /// or errored. As the workflows execute, outbound messages and
    /// [`TickResult`]s will be sent using respective channels.
    ///
    /// Note that it is possible to cancel this future (e.g., by [`select`]ing between it
    /// and a cancellation signal) and continue working with the provided workflow runtime.
    ///
    /// [`Runtime`]: runtime::Runtime
    /// [`select`]: futures::select
    pub(super) async fn run<S, R: IntoRuntime<Storage = Streaming<S>>>(
        mut self,
        runtime: R,
        commits_rx: &mut CommitStream,
    ) -> Termination
    where
        S: Storage + Clone,
        R::Clock: Schedule,
    {
        let mut runtime = runtime.into_runtime();
        // This is to prevent echoing commit events. Ugly, but seems the easiest solution.
        drop(runtime.storage.stream_commits());

        let mut cached_timer = CachedTimer::default();
        loop {
            if let Some(termination) = self.tick(&runtime, commits_rx, &mut cached_timer).await {
                return termination;
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all, ret)]
    async fn tick<R: AsRuntime>(
        &mut self,
        runtime: &R,
        commits_rx: &mut CommitStream,
        cached_timer: &mut CachedTimer,
    ) -> Option<Termination>
    where
        R::Clock: Schedule,
    {
        let nearest_timer_expiration = self.tick_runtime(runtime).await;
        let runtime_ref = runtime.as_runtime();
        if nearest_timer_expiration.is_none() {
            let no_workflows = if self.wait_for_workflows {
                false
            } else {
                runtime_ref.storage().workflow_count().await == 0
            };

            if commits_rx.is_terminated() || no_workflows {
                let termination = if no_workflows {
                    Termination::Finished
                } else {
                    Termination::Stalled
                };
                return Some(termination);
            }
        }

        let commit_event = if commits_rx.is_terminated() {
            future::pending().left_future()
        } else {
            commits_rx.next().right_future()
        };
        let timer_event = nearest_timer_expiration.map_or_else(
            || future::pending().left_future(),
            |timestamp| {
                if cached_timer.expires_at == timestamp {
                    if let Some(cached) = mem::take(&mut cached_timer.timer) {
                        return cached.right_future();
                    }
                }
                let timer = runtime_ref.inner.clock.create_timer(timestamp);
                timer.right_future()
            },
        );

        let selected = future::select(commit_event, timer_event).await;
        match selected {
            Either::Left((_, timer_event)) => {
                // We just need the fact that a commit has occurred.
                if let Either::Right(timer) = timer_event {
                    cached_timer.timer = Some(timer);
                }
            }
            Either::Right((timestamp, _)) => {
                runtime_ref.set_current_time(timestamp).await;
            }
        }
        None
    }

    async fn tick_runtime<R: AsRuntime>(&mut self, runtime: &R) -> Option<DateTime<Utc>> {
        let runtime = runtime.as_runtime();
        loop {
            let tick_result = match runtime.tick().await {
                Ok(result) => result,
                Err(blocked) => break blocked.nearest_timer_expiration(),
            };

            if self.drop_erroneous_messages && tick_result.as_ref().is_err() {
                Self::repair_workflow(runtime.storage(), tick_result.workflow_id()).await;
            }
            if let Some(sx) = &self.results_sx {
                sx.unbounded_send(tick_result).ok();
            }
        }
    }

    async fn repair_workflow<S: Storage>(storage: StorageRef<'_, S>, id: WorkflowId) {
        // Concurrency errors should not occur if the driver is used properly
        // (i.e., the workflows are not mutated externally). That's why the errors
        // are ignored below.
        let handle = storage.any_workflow(id).await;
        let Some(AnyWorkflowHandle::Errored(handle)) = handle else {
            return;
        };

        let mut dropped_messages = false;
        for message in handle.messages() {
            if message.drop_for_workflow().await.is_ok() {
                dropped_messages = true;
            }
        }
        if dropped_messages {
            handle.consider_repaired().await.ok();
        }
    }
}
