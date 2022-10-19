//! Pizza delivery using subtasks for each order.

use async_trait::async_trait;
use futures::{future, stream::FuturesUnordered, FutureExt, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use std::time::Duration;

use crate::PizzaDeliveryHandle;
use tardigrade::{
    sleep,
    task::{self, ErrorContextExt, JoinError, TaskError, TaskResult},
    workflow::{GetInterface, SpawnWorkflow, TakeHandle, Wasm, WorkflowFn},
    Json,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Args {
    pub oven_count: usize,
    pub fail_after: Duration,
    pub propagate_errors: bool,
}

#[derive(Debug, GetInterface, TakeHandle)]
#[tardigrade(handle = "PizzaDeliveryHandle")]
pub struct PizzaDeliveryWithTasks(());

#[test]
fn interface_agrees_between_declaration_and_handle() {
    PizzaDeliveryWithTasks::interface(); // Checks are performed internally
}

impl WorkflowFn for PizzaDeliveryWithTasks {
    type Args = Args;
    type Codec = Json;
}

#[async_trait(?Send)]
impl SpawnWorkflow for PizzaDeliveryWithTasks {
    async fn spawn(args: Args, mut handle: PizzaDeliveryHandle<Wasm>) -> TaskResult {
        let fail_after = args.fail_after;
        let mut order_index = 0;
        let mut tasks = FuturesUnordered::new();

        while let Some(order) = handle.orders.next().await {
            order_index += 1;
            let shared = handle.shared.clone();
            let task_name = format!("order #{}", order_index);
            let task_handle = task::try_spawn(&task_name, async move {
                futures::select_biased! {
                    _ = shared.bake(order_index, order).fuse() => Ok(()),
                    _ = sleep(fail_after).fuse() => Err(TaskError::new("baking interrupted")),
                }
            });
            tasks.push(task_handle);

            // This is quite an inefficient implementation of backpressure (instead of using
            // `try_for_each_concurrent`). It's used largely to test that different `futures`
            // primitives work properly.
            if tasks.len() == args.oven_count {
                if let Some(Err(JoinError::Err(err))) = tasks.next().await {
                    if args.propagate_errors {
                        return Err(err).context("propagating task error");
                    }
                }
            }
            assert!(tasks.len() < args.oven_count);
        }

        // Finish remaining tasks.
        if args.propagate_errors {
            tasks
                .try_for_each(future::ok)
                .await
                .map_err(JoinError::unwrap_task_error)
                .context("propagating task error")
        } else {
            tasks.for_each(|_| future::ready(())).await;
            Ok(())
        }
    }
}

tardigrade::workflow_entry!(PizzaDeliveryWithTasks);
