//! Timer-related tests.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};

use std::time::Duration;

use tardigrade::{
    channel::Sender,
    task::TaskResult,
    test::{TestInstance, Timers},
    workflow::{GetInterface, HandleFormat, InEnv, SpawnWorkflow, Wasm, WithHandle, WorkflowFn},
    Json, Timer,
};

#[derive(GetInterface, WithHandle)]
#[tardigrade(auto_interface)]
struct TimersWorkflow<Fmt: HandleFormat = Wasm> {
    timestamps: InEnv<Sender<DateTime<Utc>, Json>, Fmt>,
}

impl WorkflowFn for TimersWorkflow {
    type Args = ();
    type Codec = Json;
}

#[async_trait(?Send)]
impl SpawnWorkflow for TimersWorkflow {
    async fn spawn(_args: (), mut handle: Self) -> TaskResult {
        let now = tardigrade::now();
        let completion_time = Timer::at(now - chrono::Duration::milliseconds(100)).await;
        handle.timestamps.send(completion_time).await?;
        let completion_time = Timer::after(Duration::from_millis(100)).await;
        handle.timestamps.send(completion_time).await?;
        Ok(())
    }
}

#[test]
fn timers_basics() {
    TestInstance::<TimersWorkflow>::new(()).run(|mut api| async move {
        let now = Timers::now();
        let ts = api.timestamps.next().await.unwrap();
        assert_eq!(ts, now);

        Timers::set_now(now + chrono::Duration::seconds(1));
        let ts = api.timestamps.next().await.unwrap();
        assert_eq!(ts, Timers::now());
    });
}
