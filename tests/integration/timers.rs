//! Timer-related tests.

use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use std::time::Duration;

use tardigrade::workflow::WorkflowFn;
use tardigrade::{
    channel::Sender,
    test::{Runtime, Timers},
    workflow::{GetInterface, Handle, SpawnWorkflow, TakeHandle, TaskHandle, Wasm},
    Json, Timer,
};

#[tardigrade::handle]
struct TestHandle<Env> {
    timestamps: Handle<Sender<DateTime<Utc>, Json>, Env>,
}

#[derive(Debug, GetInterface, TakeHandle)]
#[tardigrade(
    handle = "TestHandle",
    interface = r#"{ "v": 0, "out": { "timestamps": {} } }"#
)]
struct TimersWorkflow;

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
    Runtime::default().test::<TimersWorkflow, _, _>((), |mut api| async move {
        let now = Timers::now();
        let ts = api.timestamps.next().await.unwrap();
        assert_eq!(ts, now);

        Timers::set_now(now + chrono::Duration::seconds(1));
        let ts = api.timestamps.next().await.unwrap();
        assert_eq!(ts, Timers::now());
    });
}
