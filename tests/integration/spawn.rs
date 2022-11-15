//! Tests for spawning and managing child workflows.

use async_trait::async_trait;
use futures::{stream, SinkExt, StreamExt, TryStreamExt};

use std::collections::HashSet;

use crate::{TestHandle, TestedWorkflow};
use tardigrade::{
    channel::Sender,
    spawn::{ManageWorkflowsExt, Workflows},
    task::TaskResult,
    test::Runtime,
    workflow::{GetInterface, SpawnWorkflow, TakeHandle, Wasm, WorkflowFn},
    Json,
};

#[derive(Debug, GetInterface, TakeHandle)]
#[tardigrade(handle = "TestHandle", auto_interface)]
struct ParentWorkflow;

impl ParentWorkflow {
    async fn spawn_child(command: i32, events: Sender<i32, Json>) -> TaskResult {
        let builder = Workflows.new_workflow::<TestedWorkflow>("child", ())?;
        builder.handle().events.copy_from(events);
        let mut child = builder.build().await?;
        child.api.commands.send(command).await?;
        drop(child.api.commands); // Should terminate the child workflow
        child.workflow.await?;
        Ok(())
    }
}

impl WorkflowFn for ParentWorkflow {
    type Args = u32;
    type Codec = Json;
}

#[async_trait(?Send)]
impl SpawnWorkflow for ParentWorkflow {
    async fn spawn(concurrency: u32, handle: TestHandle<Wasm>) -> TaskResult {
        let concurrency = concurrency as usize;
        handle
            .commands
            .map(Ok)
            .try_for_each_concurrent(concurrency, move |command| {
                let events = handle.events.clone();
                Self::spawn_child(command, events)
            })
            .await
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
