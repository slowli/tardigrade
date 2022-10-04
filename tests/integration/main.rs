//! High-level integration tests.

use futures::{stream, SinkExt, StreamExt};

mod channel;
mod requests;
mod spawn;
mod task;
mod timers;

use tardigrade::{
    channel::{Receiver, Sender},
    test::Runtime,
    workflow::{GetInterface, Handle, SpawnWorkflow, TakeHandle, TaskHandle, Wasm, WorkflowFn},
    Json,
};

#[tardigrade::handle]
struct TestHandle<Env> {
    commands: Handle<Receiver<i32, Json>, Env>,
    events: Handle<Sender<i32, Json>, Env>,
}

#[derive(Debug, GetInterface, TakeHandle)]
#[tardigrade(handle = "TestHandle", auto_interface)]
struct TestedWorkflow;

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
    Runtime::default().test::<TestedWorkflow, _, _>((), |mut api| async {
        let mut items = stream::iter([Ok(23), Ok(42)]);
        api.commands.send_all(&mut items).await.unwrap();
        drop(api.commands);
        let echos: Vec<_> = api.events.collect().await;
        assert_eq!(echos, [23, 42]);
    });
}

#[test]
fn dropping_outbound_channel_handle_in_test_code() {
    Runtime::default().test::<TestedWorkflow, _, _>((), |mut api| async move {
        api.commands.send(23).await.unwrap();
        let echo = api.events.next().await.unwrap();
        assert_eq!(echo, 23);

        drop(api.events);
        api.commands.send(42).await.unwrap();
    });
}
