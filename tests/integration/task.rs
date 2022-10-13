//! Tests for managing tasks in a workflow.

// FIXME: test error propagation

use futures::{future, FutureExt, SinkExt, StreamExt};

use crate::TestHandle;
use tardigrade::{
    spawn::{ManageWorkflowsExt, Workflows},
    task,
    test::Runtime,
    workflow::{GetInterface, SpawnWorkflow, TakeHandle, TaskHandle, Wasm, WorkflowFn},
    Json,
};

#[derive(Debug, GetInterface, TakeHandle)]
#[tardigrade(handle = "TestHandle", auto_interface)]
struct WorkflowWithSubtask;

impl WorkflowFn for WorkflowWithSubtask {
    type Args = bool;
    type Codec = Json;
}

impl SpawnWorkflow for WorkflowWithSubtask {
    fn spawn(move_events_to_task: bool, mut handle: TestHandle<Wasm>) -> TaskHandle {
        TaskHandle::new(async move {
            handle.events.send(42).await?;
            if move_events_to_task {
                let task = future::pending::<()>().map(|()| drop(handle.events));
                task::spawn("test", task);
            } else {
                task::spawn("test", future::pending::<()>());
            }
            Ok(())
        })
    }
}

fn test_workflow_termination(move_events_to_task: bool) {
    let mut runtime = Runtime::default();
    runtime
        .workflow_registry_mut()
        .insert::<WorkflowWithSubtask, _>("test");
    runtime.run(async {
        let builder = Workflows
            .new_workflow::<WorkflowWithSubtask>("test", move_events_to_task)
            .unwrap();
        let handle = builder.build().unwrap();
        handle.workflow.await.unwrap();

        let events: Vec<_> = handle.api.events.collect().await;
        assert_eq!(events, [42]);
    });
}

#[test]
fn workflow_terminates_on_main_task_completion() {
    test_workflow_termination(false);
}

#[test]
fn all_workflow_tasks_are_dropped_on_main_task_completion() {
    test_workflow_termination(true);
}
