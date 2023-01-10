//! Tests for spawning and managing child workflows.

use async_trait::async_trait;
use futures::{stream, SinkExt, StreamExt, TryStreamExt};

use std::collections::HashSet;

use crate::TestedWorkflow;
use tardigrade::{
    channel::Sender,
    spawn::{CreateWorkflow, Workflows},
    task::TaskResult,
    test::Runtime,
    workflow::{DelegateHandle, GetInterface, SpawnWorkflow, WorkflowFn},
    Json,
};

#[derive(Debug, GetInterface)]
#[tardigrade(auto_interface)]
struct ParentWorkflow;

impl DelegateHandle for ParentWorkflow {
    type Delegate = TestedWorkflow;
}

impl ParentWorkflow {
    async fn spawn_child(command: i32, events: Sender<i32, Json>) -> TaskResult {
        let builder = Workflows.new_workflow::<TestedWorkflow>("child").await?;
        let (child_handles, mut self_handles) = builder
            .handles(|config| {
                config.events.copy_from(events);
            })
            .await;
        let child = builder.build((), child_handles).await?;
        self_handles.commands.send(command).await?;
        drop(self_handles.commands); // Should terminate the child workflow
        child.await?;
        Ok(())
    }
}

impl WorkflowFn for ParentWorkflow {
    type Args = u32;
    type Codec = Json;
}

#[async_trait(?Send)]
impl SpawnWorkflow for ParentWorkflow {
    async fn spawn(concurrency: u32, handle: TestedWorkflow) -> TaskResult {
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
        .insert::<TestedWorkflow>("child");
    runtime.test::<ParentWorkflow>(1).run(|mut api| async {
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
        .insert::<TestedWorkflow>("child");
    runtime.test::<ParentWorkflow>(3).run(|mut api| async {
        let mut items = stream::iter((0..10).map(Ok));
        api.commands.send_all(&mut items).await.unwrap();
        drop(api.commands);

        let echos: HashSet<_> = api.events.collect().await;
        assert_eq!(echos, HashSet::from_iter(0..10));
    });
}
