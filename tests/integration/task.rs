//! Tests for managing tasks in a workflow.

use assert_matches::assert_matches;
use async_trait::async_trait;
use futures::{future, FutureExt, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

use crate::TestHandle;
use tardigrade::{
    channel::SendError,
    spawn::{ManageWorkflows, Workflows},
    task::{self, JoinError, TaskError, TaskResult},
    test::Runtime,
    workflow::{GetInterface, SpawnWorkflow, Wasm, WithHandle, WorkflowFn},
    Json,
};

#[derive(Debug, Default, Serialize, Deserialize)]
struct Args {
    move_events_to_task: bool,
    fail_subtask: bool,
    abort_subtask: bool,
}

#[derive(Debug, GetInterface, WithHandle)]
#[tardigrade(handle = "TestHandle", auto_interface)]
struct WorkflowWithSubtask;

impl WorkflowFn for WorkflowWithSubtask {
    type Args = Args;
    type Codec = Json;
}

#[async_trait(?Send)]
impl SpawnWorkflow for WorkflowWithSubtask {
    async fn spawn(args: Args, mut handle: TestHandle<Wasm>) -> TaskResult {
        handle.events.send(42).await?;
        let task = if args.fail_subtask {
            future::err(TaskError::new("subtask failure!")).left_future()
        } else {
            future::pending::<TaskResult>().right_future()
        };

        let mut task_handle = if args.move_events_to_task {
            let task = task.inspect(|_| drop(handle.events));
            task::try_spawn("test", task)
        } else {
            task::try_spawn("test", task)
        };

        if args.fail_subtask {
            let err = task_handle.await.unwrap_err();
            let err = into_task_error(err);
            assert!(err.location().filename.ends_with("task.rs"));
            assert_eq!(err.cause().to_string(), "subtask failure!");
        } else if args.abort_subtask {
            task_handle.abort();
            let err = task_handle.await.unwrap_err();
            assert_matches!(err, JoinError::Aborted);
        }
        Ok(())
    }
}

fn test_workflow_termination(args: Args) {
    let mut runtime = Runtime::default();
    runtime
        .workflow_registry_mut()
        .insert::<WorkflowWithSubtask>("test");

    runtime.run(async {
        let builder = Workflows
            .new_workflow::<WorkflowWithSubtask>("test")
            .unwrap();
        let (child_handles, self_handles) = builder.handles(|_| ()).await;
        let child = builder.build(args, child_handles).await.unwrap();
        child.await.unwrap();

        let events: Vec<_> = self_handles.events.collect().await;
        assert_eq!(events, [42]);
    });
}

fn into_task_error(err: JoinError) -> TaskError {
    match err {
        JoinError::Err(err) => err,
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn workflow_terminates_on_main_task_completion() {
    test_workflow_termination(Args::default());
}

#[test]
fn all_workflow_tasks_are_dropped_on_main_task_completion() {
    test_workflow_termination(Args {
        move_events_to_task: true,
        ..Args::default()
    });
}

#[test]
fn aborting_subtask() {
    test_workflow_termination(Args {
        abort_subtask: true,
        ..Args::default()
    });
}

#[test]
fn aborting_subtask_with_moved_events() {
    test_workflow_termination(Args {
        abort_subtask: true,
        move_events_to_task: true,
        ..Args::default()
    });
}

#[test]
fn workflow_failure_in_main_task() {
    let mut runtime = Runtime::default();
    runtime
        .workflow_registry_mut()
        .insert::<WorkflowWithSubtask>("test");
    runtime.run(async {
        let builder = Workflows
            .new_workflow::<WorkflowWithSubtask>("test")
            .unwrap();
        let (child_handles, _) = builder
            .handles(|config| {
                config.events.close(); // makes the workflow fail on sending the "42" event
            })
            .await;
        let child = builder.build(Args::default(), child_handles).await.unwrap();

        let err = child.await.unwrap_err();
        let err = into_task_error(err);
        assert!(err.location().filename.ends_with("task.rs"));
        let cause = err.cause().downcast_ref::<SendError>().unwrap();
        assert_matches!(cause, SendError::Closed);
    });
}

#[test]
fn workflow_failure_in_subtask() {
    test_workflow_termination(Args {
        fail_subtask: true,
        ..Args::default()
    });
}

#[test]
fn workflow_failure_in_subtask_with_moved_events() {
    test_workflow_termination(Args {
        fail_subtask: true,
        move_events_to_task: true,
        ..Args::default()
    });
}
