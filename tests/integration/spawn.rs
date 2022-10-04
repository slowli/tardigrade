//! Tests for spawning and managing child workflows.

use futures::{stream, SinkExt, StreamExt};

use std::collections::HashSet;

use crate::{TestHandle, TestedWorkflow};
use tardigrade::{
    spawn::{ManageWorkflowsExt, WorkflowBuilder, Workflows},
    test::Runtime,
    workflow::{GetInterface, SpawnWorkflow, TakeHandle, TaskHandle, Wasm, WorkflowFn},
    Json,
};

#[derive(Debug, GetInterface, TakeHandle)]
#[tardigrade(handle = "TestHandle", auto_interface)]
struct ParentWorkflow;

impl WorkflowFn for ParentWorkflow {
    type Args = u32;
    type Codec = Json;
}

impl SpawnWorkflow for ParentWorkflow {
    fn spawn(concurrency: u32, handle: TestHandle<Wasm>) -> TaskHandle {
        let concurrency = concurrency as usize;
        let commands = handle
            .commands
            .for_each_concurrent(concurrency, move |command| {
                let events = handle.events.clone();
                async move {
                    let builder: WorkflowBuilder<_, TestedWorkflow> =
                        Workflows.new_workflow("child", ()).unwrap();
                    builder.handle().events.copy_from(events);
                    let mut child = builder.build().unwrap();
                    child.api.commands.send(command).await.unwrap();
                    drop(child.api.commands); // Should terminate the child workflow
                    child.workflow.await.unwrap();
                }
            });
        TaskHandle::new(commands)
    }
}

#[test]
fn forwarding_outbound_channel_for_child_workflow() {
    let mut runtime = Runtime::default();
    runtime
        .workflow_registry_mut()
        .insert::<TestedWorkflow, _>("child");
    runtime.test::<ParentWorkflow, _, _>(1, |mut api| async {
        let mut items = stream::iter([Ok(23), Ok(42)]);
        api.commands.send_all(&mut items).await.unwrap();
        drop(api.commands);

        let echos: Vec<_> = api.events.collect().await;
        assert_eq!(echos, [23, 42]);
    });
}

#[test]
fn concurrent_child_workflows() {
    let mut runtime = Runtime::default();
    runtime
        .workflow_registry_mut()
        .insert::<TestedWorkflow, _>("child");
    runtime.test::<ParentWorkflow, _, _>(3, |mut api| async {
        let mut items = stream::iter((0..10).map(Ok));
        api.commands.send_all(&mut items).await.unwrap();
        drop(api.commands);

        let echos: HashSet<_> = api.events.collect().await;
        assert_eq!(echos, HashSet::from_iter(0..10));
    });
}
