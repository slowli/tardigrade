//! High-level integration tests.

use async_trait::async_trait;
use futures::{stream, SinkExt, StreamExt};

mod channel;
mod requests;
mod spawn;
mod task;
mod timers;

use tardigrade::{
    channel::{Receiver, Sender},
    task::TaskResult,
    test::TestInstance,
    workflow::{GetInterface, InEnv, SpawnWorkflow, TakeHandle, Wasm, WorkflowEnv, WorkflowFn},
    Json,
};

#[tardigrade::handle]
#[derive(Debug)]
struct TestHandle<Env: WorkflowEnv> {
    commands: InEnv<Receiver<i32, Json>, Env>,
    events: InEnv<Sender<i32, Json>, Env>,
}

#[derive(Debug, GetInterface, TakeHandle)]
#[tardigrade(handle = "TestHandle", auto_interface)]
struct TestedWorkflow;

impl WorkflowFn for TestedWorkflow {
    type Args = ();
    type Codec = Json;
}

#[async_trait(?Send)]
impl SpawnWorkflow for TestedWorkflow {
    async fn spawn(_args: (), handle: TestHandle<Wasm>) -> TaskResult {
        let commands = handle.commands.map(Ok);
        commands.forward(handle.events).await?;
        Ok(())
    }
}

#[test]
fn dropping_inbound_channel_handle_in_test_code() {
    TestInstance::<TestedWorkflow>::new(()).run(|mut api| async {
        let mut items = stream::iter([Ok(23), Ok(42)]);
        api.commands.send_all(&mut items).await.unwrap();
        drop(api.commands);
        let echos: Vec<_> = api.events.collect().await;
        assert_eq!(echos, [23, 42]);
    });
}

#[test]
fn dropping_outbound_channel_handle_in_test_code() {
    TestInstance::<TestedWorkflow>::new(()).run(|mut api| async move {
        api.commands.send(23).await.unwrap();
        let echo = api.events.next().await.unwrap();
        assert_eq!(echo, 23);

        drop(api.events);
        api.commands.send(42).await.unwrap();
    });
}
