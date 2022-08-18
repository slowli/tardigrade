//! High-level integration tests.

use futures::{stream, SinkExt, StreamExt};

mod channel;
mod requests;
mod timers;

use tardigrade::{
    channel::{Receiver, Sender},
    test::TestWorkflow,
    workflow::{GetInterface, Handle, SpawnWorkflow, TaskHandle, Wasm, WorkflowFn},
    Json,
};
use tardigrade_shared::interface::Interface;

#[derive(Debug)]
struct TestedWorkflow;

impl GetInterface for TestedWorkflow {
    const WORKFLOW_NAME: &'static str = "TestedWorkflow";

    fn interface() -> Interface<Self> {
        Interface::from_bytes(br#"{ "v": 0, "in": { "commands": {} }, "out": { "events": {} } }"#)
            .downcast()
            .unwrap()
    }
}

#[tardigrade::handle(for = "TestedWorkflow")]
struct TestHandle<Env> {
    commands: Handle<Receiver<i32, Json>, Env>,
    events: Handle<Sender<i32, Json>, Env>,
}

impl WorkflowFn for TestedWorkflow {
    type Args = ();
    type Codec = Json;
}

impl SpawnWorkflow for TestedWorkflow {
    fn spawn(_data: (), handle: TestHandle<Wasm>) -> TaskHandle {
        TaskHandle::new(async move {
            let commands = handle.commands.map(Ok);
            commands.forward(handle.events).await.unwrap();
        })
    }
}

#[test]
fn dropping_inbound_channel_handle_in_test_code() {
    TestedWorkflow::test((), |mut handle| async move {
        let mut items = stream::iter([Ok(23), Ok(42)]);
        handle.api.commands.send_all(&mut items).await.unwrap();
        drop(handle.api.commands);
        let echos: Vec<_> = handle.api.events.collect().await;
        assert_eq!(echos, [23, 42]);
    });
}

#[test]
fn dropping_outbound_channel_handle_in_test_code() {
    TestedWorkflow::test((), |mut handle| async move {
        handle.api.commands.send(23).await;
        let echo = handle.api.events.next().await.unwrap();
        assert_eq!(echo, 23);

        drop(handle.api.events);
        handle.api.commands.send(42).await;
    });
}
