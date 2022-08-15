//! Timer-related tests.

use chrono::{DateTime, Utc};
use futures::StreamExt;
use std::time::Duration;

use tardigrade::workflow::WorkflowFn;
use tardigrade::{
    channel::Sender,
    test::TestWorkflow,
    workflow::{GetInterface, Handle, SpawnWorkflow, TaskHandle, Wasm},
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
struct TimersHandle<Env> {
    timestamps: Handle<Sender<DateTime<Utc>, Json>, Env>,
}

impl WorkflowFn for TimersWorkflow {
    type Args = ();
    type Codec = Json;
}

impl SpawnWorkflow for TimersWorkflow {
    fn spawn(_data: (), mut handle: TimersHandle<Wasm>) -> TaskHandle {
        TaskHandle::new(async move {
            let now = tardigrade::now();
            let completion_time = Timer::at(now - chrono::Duration::milliseconds(100)).await;
            handle.timestamps.send(completion_time).await;
            let completion_time = Timer::after(Duration::from_millis(100)).await;
            handle.timestamps.send(completion_time).await;
        })
    }
}

#[test]
fn timers_basics() {
    TimersWorkflow::test((), |mut handle| async move {
        let now = handle.timers.now();
        let ts = handle.api.timestamps.next().await.unwrap();
        assert_eq!(ts, now);

        handle.timers.set_now(now + chrono::Duration::seconds(1));
        let ts = handle.api.timestamps.next().await.unwrap();
        assert_eq!(ts, handle.timers.now());
    });
}
