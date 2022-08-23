//! Timer-related tests.

use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use std::time::Duration;

use tardigrade::workflow::WorkflowFn;
use tardigrade::{
    channel::Sender,
    test::{Runtime, Timers},
    workflow::{GetInterface, Handle, SpawnWorkflow, TaskHandle, Wasm, WorkflowDefinition},
    Json, Timer,
};
use tardigrade_shared::interface::Interface;

#[derive(Debug)]
struct TimersWorkflow;

impl GetInterface for TimersWorkflow {
    const WORKFLOW_NAME: &'static str = "TimersWorkflow";

    fn interface() -> Interface<Self> {
        Interface::from_bytes(br#"{ "v": 0, "out": { "timestamps": {} } }"#)
            .downcast()
            .unwrap()
    }
}

#[tardigrade::handle(for = "TimersWorkflow")]
struct TestHandle<Env> {
    timestamps: Handle<Sender<DateTime<Utc>, Json>, Env>,
}

impl WorkflowFn for TimersWorkflow {
    type Args = ();
    type Codec = Json;
}

impl SpawnWorkflow for TimersWorkflow {
    fn spawn(_data: (), mut handle: TestHandle<Wasm>) -> TaskHandle {
        TaskHandle::new(async move {
            let now = tardigrade::now();
            let completion_time = Timer::at(now - chrono::Duration::milliseconds(100)).await;
            handle.timestamps.send(completion_time).await.unwrap();
            let completion_time = Timer::after(Duration::from_millis(100)).await;
            handle.timestamps.send(completion_time).await.unwrap();
        })
    }
}

#[test]
fn timers_basics() {
    let mut runtime = Runtime::default();
    runtime
        .workflow_registry_mut()
        .insert::<TimersWorkflow>("test");
    runtime.test(async move {
        let workflow_def = WorkflowDefinition::new("test")
            .unwrap()
            .downcast::<TimersWorkflow>()
            .unwrap();
        let api = workflow_def.spawn(()).build().unwrap().api;
        let mut timestamps = api.timestamps.unwrap();

        let now = Timers::now();
        let ts = timestamps.next().await.unwrap();
        assert_eq!(ts, now);

        Timers::set_now(now + chrono::Duration::seconds(1));
        let ts = timestamps.next().await.unwrap();
        assert_eq!(ts, Timers::now());
    });
}
