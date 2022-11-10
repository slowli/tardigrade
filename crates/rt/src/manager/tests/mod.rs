//! Tests for `WorkflowManager`.

use assert_matches::assert_matches;
use mimicry::Answers;
use wasmtime::{AsContextMut, StoreContextMut, Trap};

use std::{collections::HashSet, task::Poll};

mod spawn;

use super::*;
use crate::{
    data::{WasmContextPtr, WorkflowData, WorkflowFunctions},
    module::{ExportsMock, MockPollFn},
    receipt::{
        ChannelEvent, ChannelEventKind, ExecutedFunction, ExecutionError, Receipt, WakeUpCause,
    },
    storage::LocalStorage,
    utils::WasmAllocator,
    WorkflowEngine, WorkflowModule,
};
use tardigrade::{
    abi::AllocateBytes,
    interface::{InboundChannel, OutboundChannel},
    spawn::ManageWorkflowsExt,
};

const POLL_CX: WasmContextPtr = 123;
const DEFINITION_ID: &str = "test@latest::TestWorkflow";

type LocalManager = WorkflowManager<(), LocalStorage>;

async fn create_test_manager() -> LocalManager {
    let mut manager = WorkflowManager::builder(LocalStorage::default())
        .build()
        .await
        .unwrap();
    let module =
        WorkflowModule::new(&WorkflowEngine::default(), ExportsMock::MOCK_MODULE_BYTES).unwrap();
    manager.insert_module("test@latest", module).await;
    manager
}

async fn create_test_workflow(manager: &LocalManager) -> WorkflowHandle<'_, (), LocalManager> {
    manager
        .new_workflow(DEFINITION_ID, b"test_input".to_vec())
        .unwrap()
        .build()
        .await
        .unwrap()
}

async fn tick_workflow(manager: &LocalManager, id: WorkflowId) -> Result<Receipt, ExecutionError> {
    let mut transaction = manager.storage.transaction().await;
    let record = transaction.workflow(id).await.unwrap();
    let result = manager.tick_workflow(&mut transaction, record).await;
    transaction.commit().await;
    result
}

async fn find_consumable_channel(
    manager: &WorkflowManager<(), LocalStorage>,
) -> Option<(ChannelId, usize, WorkflowId)> {
    let transaction = manager.storage.readonly_transaction().await;
    let (channel_id, idx, workflow) = transaction.find_consumable_channel().await?;
    Some((channel_id, idx, workflow.id))
}

async fn feed_message(
    manager: &WorkflowManager<(), LocalStorage>,
    channel_id: ChannelId,
    workflow_id: WorkflowId,
) -> Result<Receipt, ExecutionError> {
    let mut transaction = manager.storage.transaction().await;
    let workflow = transaction.workflow(workflow_id).await.unwrap();
    let (.., channel_state) = workflow.persisted.find_inbound_channel(channel_id);
    let message_idx = channel_state.received_message_count();
    let result = manager
        .feed_message_to_workflow(&mut transaction, channel_id, message_idx, workflow)
        .await;
    transaction.commit().await;
    result
}

fn initialize_task(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
    let poll_result =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
    assert_eq!(poll_result, -1); // Pending

    let traces = Some(WorkflowData::outbound_channel_ref(None, "traces"));
    let (trace_ptr, trace_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"trace #1")?;
    WorkflowFunctions::start_send(ctx.as_context_mut(), traces, trace_ptr, trace_len)?;
    // ignore send result

    Ok(Poll::Pending)
}

#[async_std::test]
async fn instantiating_workflow() {
    let poll_fns = Answers::from_value(initialize_task as MockPollFn);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager().await;
    let workflow = create_test_workflow(&manager).await;

    let storage = manager.storage.readonly_transaction().await;
    assert_eq!(
        storage
            .find_workflow_with_pending_tasks()
            .await
            .map(|record| record.id),
        Some(workflow.id())
    );
    let record = storage.workflow(workflow.id()).await.unwrap();
    assert_eq!(record.module_id, "test@latest");
    assert_eq!(record.name_in_module, "TestWorkflow");
    let orders_id = workflow.ids().channel_ids.inbound["orders"];
    let orders_record = storage.channel(orders_id).await.unwrap();
    assert_eq!(
        orders_record.state.receiver_workflow_id,
        Some(workflow.id())
    );
    assert_eq!(orders_record.state.sender_workflow_ids, HashSet::new());
    assert!(orders_record.state.has_external_sender);

    let traces_id = workflow.ids().channel_ids.outbound["traces"];
    let traces_record = storage.channel(traces_id).await.unwrap();
    assert_eq!(traces_record.state.receiver_workflow_id, None);
    assert_eq!(
        traces_record.state.sender_workflow_ids,
        HashSet::from_iter([workflow.id()])
    );
    assert!(!traces_record.state.has_external_sender);
    drop(storage);

    test_initializing_workflow(&manager, workflow.ids()).await;
}

async fn test_initializing_workflow(
    manager: &WorkflowManager<(), LocalStorage>,
    ids: &WorkflowAndChannelIds,
) {
    let mut transaction = manager.storage.transaction().await;
    let record = transaction.workflow(ids.workflow_id).await.unwrap();
    let receipt = manager
        .tick_workflow(&mut transaction, record)
        .await
        .unwrap();
    assert_eq!(receipt.executions().len(), 2);
    let main_execution = &receipt.executions()[0];
    assert_matches!(main_execution.function, ExecutedFunction::Entry { .. });

    let traces_id = ids.channel_ids.outbound["traces"];
    assert!(transaction
        .find_workflow_with_pending_tasks()
        .await
        .is_none());
    assert!(transaction.find_consumable_channel().await.is_none());
    let traces = transaction.channel(traces_id).await.unwrap();
    assert_eq!(traces.received_messages(), 1);
    let message = transaction.channel_message(traces_id, 0).await.unwrap();
    assert_eq!(message, b"trace #1");
}

#[async_std::test]
async fn initializing_workflow_with_closed_channels() {
    let test_channels: MockPollFn = |mut ctx| {
        let traces = Some(WorkflowData::outbound_channel_ref(None, "traces"));
        let poll_result =
            WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), traces, POLL_CX)?;
        assert_eq!(poll_result, 2); // Err(Closed)

        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        let poll_result =
            WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
        assert_eq!(poll_result, -2); // Ready(None)

        Ok(Poll::Pending)
    };

    let _guard = ExportsMock::prepare(Answers::from_value(test_channels));
    let manager = create_test_manager().await;
    let builder = manager
        .new_workflow::<()>(DEFINITION_ID, b"test_input".to_vec())
        .unwrap();
    builder.handle()[InboundChannel("orders")].close();
    builder.handle()[OutboundChannel("traces")].close();
    let workflow = builder.build().await.unwrap();
    let workflow_id = workflow.id();

    assert_eq!(workflow.ids().channel_ids.inbound["orders"], 0);
    assert_eq!(workflow.ids().channel_ids.outbound["traces"], 0);
    tick_workflow(&manager, workflow_id).await.unwrap();

    let mut handle = manager.workflow(workflow_id).await.unwrap().handle();
    let channel_info = handle[InboundChannel("orders")].channel_info().await;
    assert!(channel_info.is_closed());
    assert_eq!(channel_info.received_messages(), 0);
    let channel_info = handle[OutboundChannel("traces")].channel_info().await;
    assert!(channel_info.is_closed());

    let err = handle[InboundChannel("orders")]
        .send(b"test".to_vec())
        .await
        .unwrap_err();
    assert_matches!(err, SendError::Closed);
}

#[async_std::test]
async fn closing_workflow_channels() {
    let block_on_flush: MockPollFn = |mut ctx| {
        let events = Some(WorkflowData::outbound_channel_ref(None, "events"));
        let (ptr, len) = WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"event #1")?;
        WorkflowFunctions::start_send(ctx.as_context_mut(), events.clone(), ptr, len)?;
        let poll_result =
            WorkflowFunctions::poll_flush_for_sender(ctx.as_context_mut(), events, POLL_CX)?;
        assert_eq!(poll_result, -1); // Pending
        Ok(Poll::Pending)
    };
    let drop_inbound_channel: MockPollFn = |mut ctx| {
        // Check that the outbound channel is closed. Note that flushing the channel
        // must succeed (messages are flushed before the closure).
        let events = Some(WorkflowData::outbound_channel_ref(None, "events"));
        let poll_result = WorkflowFunctions::poll_flush_for_sender(
            ctx.as_context_mut(),
            events.clone(),
            POLL_CX,
        )?;
        assert_eq!(poll_result, 0); // Ok(())

        let poll_result =
            WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), events, POLL_CX)?;
        assert_eq!(poll_result, 2); // Err(Closed)

        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        WorkflowFunctions::drop_ref(ctx.as_context_mut(), orders)?;
        Ok(Poll::Pending)
    };

    let poll_fns = Answers::from_values([block_on_flush, drop_inbound_channel]);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager().await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let events_id = workflow.ids().channel_ids.outbound["events"];
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    tick_workflow(&manager, workflow_id).await.unwrap();
    manager.close_host_receiver(events_id).await;
    let channel_info = manager.channel(events_id).await.unwrap();
    assert!(channel_info.is_closed());

    workflow.update().await.unwrap();
    let events: Vec<_> = workflow.persisted().pending_events().collect();
    assert_matches!(
        events.as_slice(),
        [WakeUpCause::Flush { workflow_id: None, channel_name, .. }]
            if channel_name == "events"
    );

    tick_workflow(&manager, workflow_id).await.unwrap();
    let channel_info = manager.channel(orders_id).await.unwrap();
    assert!(channel_info.is_closed());
}

fn extract_channel_events<'a>(
    receipt: &'a Receipt,
    child_id: Option<WorkflowId>,
    name: &str,
) -> Vec<&'a ChannelEventKind> {
    let channel_events = receipt.events().filter_map(|event| {
        if let Some(ChannelEvent {
            kind,
            channel_name,
            workflow_id,
        }) = event.as_channel_event()
        {
            if channel_name == name && *workflow_id == child_id {
                return Some(kind);
            }
        }
        None
    });
    channel_events.collect()
}

async fn test_closing_inbound_channel_from_host_side(with_message: bool) {
    let poll_inbound_channel: MockPollFn = |mut ctx| {
        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        let poll_result = WorkflowFunctions::poll_next_for_receiver(
            ctx.as_context_mut(),
            orders.clone(),
            POLL_CX,
        )?;
        assert_ne!(poll_result, -1); // Poll::Ready(Some(_))

        let poll_result = WorkflowFunctions::poll_next_for_receiver(ctx, orders, POLL_CX)?;
        assert_eq!(poll_result, -1);
        // ^ Poll::Pending; we don't dispatch the closure signal immediately

        Ok(Poll::Pending)
    };
    let test_closed_channel: MockPollFn = |ctx| {
        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        let poll_result = WorkflowFunctions::poll_next_for_receiver(ctx, orders, POLL_CX)?;
        assert_eq!(poll_result, -2); // Poll::Ready(None)

        Ok(Poll::Pending)
    };

    let poll_fns: Vec<MockPollFn> = if with_message {
        vec![initialize_task, poll_inbound_channel, test_closed_channel]
    } else {
        vec![initialize_task, test_closed_channel]
    };
    let _guard = ExportsMock::prepare(Answers::from_values(poll_fns));
    let manager = create_test_manager().await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    tick_workflow(&manager, workflow_id).await.unwrap(); // initializes the workflow
    if with_message {
        manager
            .send_message(orders_id, b"order #1".to_vec())
            .await
            .unwrap();
    }
    manager.close_host_sender(orders_id).await;

    if with_message {
        let receipt = feed_message(&manager, orders_id, workflow_id)
            .await
            .unwrap();
        let order_events = extract_channel_events(&receipt, None, "orders");
        assert_matches!(
            order_events.as_slice(),
            [
                ChannelEventKind::InboundChannelPolled {
                    result: Poll::Ready(Some(8)),
                },
                ChannelEventKind::InboundChannelPolled {
                    result: Poll::Pending,
                },
            ]
        );

        // Feed the EOF as well.
        feed_message(&manager, orders_id, workflow_id)
            .await
            .unwrap();
    }

    workflow.update().await.unwrap();
    let pending_events = workflow.persisted().pending_events().collect::<Vec<_>>();
    assert_matches!(
        pending_events.as_slice(),
        [WakeUpCause::ChannelClosed { workflow_id: None, channel_name }]
            if channel_name == "orders"
    );

    let receipt = tick_workflow(&manager, workflow_id).await.unwrap();
    let order_events = extract_channel_events(&receipt, None, "orders");
    assert_matches!(
        order_events.as_slice(),
        [ChannelEventKind::InboundChannelPolled {
            result: Poll::Ready(None),
        }]
    );
}

#[async_std::test]
async fn closing_inbound_channel_from_host_side() {
    test_closing_inbound_channel_from_host_side(false).await;
}

#[async_std::test]
async fn closing_inbound_channel_with_message_from_host_side() {
    test_closing_inbound_channel_from_host_side(true).await;
}

#[async_std::test]
async fn error_initializing_workflow() {
    let error_on_initialization: MockPollFn = |_| Err(Trap::new("oops"));
    let poll_fns = Answers::from_value(error_on_initialization);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager().await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();

    let err = tick_workflow(&manager, workflow_id).await.unwrap_err();
    let err = err.trap().display_reason().to_string();
    assert!(err.contains("oops"), "{}", err);

    workflow.update().await.unwrap();
    assert!(!workflow.persisted().is_initialized());
}

fn poll_receiver(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
    let poll_result =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
    assert_eq!(poll_result, -1); // Poll::Pending

    Ok(Poll::Pending)
}

fn consume_message(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
    let poll_result =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
    assert_ne!(poll_result, -1); // Poll::Ready

    Ok(Poll::Pending)
}

#[async_std::test]
async fn sending_message_to_workflow() {
    let poll_fns = Answers::from_values([poll_receiver as MockPollFn, consume_message]);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager().await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    tick_workflow(&manager, workflow_id).await.unwrap();
    manager
        .send_message(orders_id, b"order #1".to_vec())
        .await
        .unwrap();

    {
        let transaction = manager.storage.readonly_transaction().await;
        let order_count = transaction
            .channel(orders_id)
            .await
            .unwrap()
            .received_messages();
        assert_eq!(order_count, 1);
        let order = transaction.channel_message(orders_id, 0).await.unwrap();
        assert_eq!(order, b"order #1");
    }
    assert_eq!(
        find_consumable_channel(&manager).await,
        Some((orders_id, 0, workflow_id))
    );

    let receipt = feed_message(&manager, orders_id, workflow_id)
        .await
        .unwrap();
    assert_eq!(receipt.executions().len(), 2); // waker + task
    let waker_execution = &receipt.executions()[1];
    assert_matches!(
        waker_execution.function,
        ExecutedFunction::Task {
            wake_up_cause: WakeUpCause::InboundMessage { .. },
            ..
        }
    );

    workflow.update().await.unwrap();
    let (.., orders_state) = workflow.persisted().find_inbound_channel(orders_id);
    assert_eq!(orders_state.received_message_count(), 1);
}

#[async_std::test]
async fn error_processing_inbound_message_in_workflow() {
    let error_after_consuming_message: MockPollFn = |mut ctx| {
        let orders = Some(WorkflowData::inbound_channel_ref(None, "orders"));
        let poll_result =
            WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), orders, POLL_CX)?;
        assert_ne!(poll_result, -1); // Ready(Some(_))

        Err(Trap::new("oops"))
    };

    let _guard = ExportsMock::prepare(Answers::from_values([
        initialize_task,
        error_after_consuming_message,
    ]));
    let manager = create_test_manager().await;
    let workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    tick_workflow(&manager, workflow_id).await.unwrap(); // initializes the workflow
    manager
        .send_message(orders_id, b"test".to_vec())
        .await
        .unwrap();
    let err = feed_message(&manager, orders_id, workflow_id)
        .await
        .unwrap_err();
    let err = err.trap().display_reason().to_string();
    assert!(err.contains("oops"), "{err}");

    let channel_info = manager.channel(orders_id).await.unwrap();
    assert!(!channel_info.is_closed());
    assert_eq!(channel_info.received_messages(), 1);
}

#[async_std::test]
async fn workflow_not_consuming_inbound_message() {
    let not_consume_message: MockPollFn = |_| Ok(Poll::Pending);
    let poll_fns = Answers::from_values([poll_receiver as MockPollFn, not_consume_message]);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager().await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    tick_workflow(&manager, workflow_id).await.unwrap();
    manager
        .send_message(orders_id, b"order #1".to_vec())
        .await
        .unwrap();
    feed_message(&manager, orders_id, workflow_id)
        .await
        .unwrap();

    let transaction = manager.storage.readonly_transaction().await;
    let orders_channel = transaction.channel(orders_id).await.unwrap();
    assert_eq!(orders_channel.received_messages(), 1);
    let order = transaction.channel_message(orders_id, 0).await.unwrap();
    assert_eq!(order, b"order #1");
    drop(transaction);

    // Workflow wakers should be consumed to not trigger the infinite loop.
    workflow.update().await.unwrap();
    let events: Vec<_> = workflow.persisted().pending_events().collect();
    assert!(events.is_empty(), "{events:?}");
}
