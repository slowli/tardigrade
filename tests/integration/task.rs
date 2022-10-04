//! Tests for managing tasks in a workflow.

use futures::{future, FutureExt, SinkExt, StreamExt};

use crate::TestHandle;
use tardigrade::{
    spawn,
    spawn::{ManageWorkflowsExt, WorkflowBuilder, Workflows},
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
            handle.events.send(42).await.unwrap();
            if move_events_to_task {
                let task = future::pending::<()>().map(|()| drop(handle.events));
                spawn("test", task);
            } else {
                spawn("test", future::pending::<()>());
            }
        })
    }
}

fn test_workflow_termination(move_events_to_task: bool) {
    let mut runtime = Runtime::default();
    runtime
        .workflow_registry_mut()
        .insert::<WorkflowWithSubtask, _>("test");
    runtime.run(async {
        let builder: WorkflowBuilder<_, WorkflowWithSubtask> =
            Workflows.new_workflow("test", move_events_to_task).unwrap();
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
