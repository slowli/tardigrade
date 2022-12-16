//! Pizza delivery using subtasks for each order.

use async_trait::async_trait;
use futures::{future, stream::FuturesUnordered, SinkExt, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use std::collections::HashSet;

use crate::{DomainEvent, PizzaDelivery, PizzaKind};
use tardigrade::{
    task::{self, ErrorContextExt, JoinError, TaskError, TaskResult},
    workflow::{DelegateHandle, GetInterface, SpawnWorkflow, WorkflowEntry, WorkflowFn},
    Json,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Args {
    pub oven_count: usize,
    pub fail_kinds: HashSet<PizzaKind>,
    pub propagate_errors: bool,
}

#[derive(Debug, GetInterface, WorkflowEntry)]
pub struct PizzaDeliveryWithTasks(());

impl DelegateHandle for PizzaDeliveryWithTasks {
    type Delegate = PizzaDelivery;
}

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
    async fn spawn(args: Args, mut handle: PizzaDelivery) -> TaskResult {
        let mut index = 0;
        let mut tasks = FuturesUnordered::new();

        while let Some(order) = handle.orders.next().await {
            index += 1;
            let shared = handle.shared.clone();
            let task_name = format!("order #{index}");
            let fail = args.fail_kinds.contains(&order.kind);
            let mut events = handle.shared.events.clone();

            let task_handle = task::try_spawn(&task_name, async move {
                if fail {
                    events
                        .send(DomainEvent::OrderTaken { index, order })
                        .await
                        .ok();
                    Err(TaskError::new("cannot bake"))
                } else {
                    shared.bake(index, order).await;
                    Ok(())
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
