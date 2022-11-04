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
        ChannelEvent, ChannelEventKind, Event, ExecutedFunction, ExecutionError, Receipt,
        WakeUpCause,
    },
    storage::{LocalStorage, WorkflowWaker},
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

type LocalManager<C = ()> = WorkflowManager<C, LocalStorage>;

pub(crate) async fn create_test_manager<C: Clock>(clock: C) -> LocalManager<C> {
    let mut manager = WorkflowManager::builder(LocalStorage::default())
        .with_clock(clock)
        .build()
        .await
        .unwrap();
    let module =
        WorkflowModule::new(&WorkflowEngine::default(), ExportsMock::MOCK_MODULE_BYTES).unwrap();
    manager.insert_module("test@latest", module).await;
    manager
}

pub(crate) async fn create_test_workflow<C: Clock>(
    manager: &LocalManager<C>,
) -> WorkflowHandle<'_, (), LocalManager<C>> {
    manager
        .new_workflow(DEFINITION_ID, b"test_input".to_vec())
        .unwrap()
        .build()
        .await
        .unwrap()
}

async fn tick_workflow(manager: &LocalManager, id: WorkflowId) -> Result<Receipt, ExecutionError> {
    let mut transaction = manager.storage.transaction().await;
    transaction.prepare_wakers_for_workflow(id);
    transaction.commit().await;
    let result = manager.tick().await.unwrap();
    assert_eq!(result.workflow_id(), id);
    result.drop_handle().into_inner()
}

fn is_consumption(event: &Event, channel_name: &str) -> bool {
    if let Some(event) = event.as_channel_event() {
        event.channel_name == channel_name
            && matches!(
                event.kind,
                ChannelEventKind::InboundChannelPolled {
                    result: Poll::Ready(_)
                }
            )
    } else {
        false
    }
}

async fn feed_message(
    manager: &LocalManager,
    workflow_id: WorkflowId,
    channel_name: &str,
) -> Result<Receipt, ExecutionError> {
    let result = tick_workflow(manager, workflow_id).await;
    let receipt = result.as_ref().unwrap_or_else(ExecutionError::receipt);
    let consumed = receipt
        .events()
        .any(|event| is_consumption(event, channel_name));
    assert!(consumed, "{receipt:?}");
    result
}

async fn wakers_for_workflow(
    manager: &LocalManager,
    workflow_id: WorkflowId,
) -> Vec<WorkflowWaker> {
    let transaction = manager.storage.readonly_transaction().await;
    transaction
        .as_ref()
        .wakers_for_workflow(workflow_id)
        .cloned()
        .collect()
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
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let workflow = create_test_workflow(&manager).await;

    let storage = manager.storage.readonly_transaction().await;
    let record = storage.workflow(workflow.id()).await.unwrap();
    assert_eq!(record.module_id, "test@latest");
    assert_eq!(record.name_in_module, "TestWorkflow");
    let orders_id = workflow.ids().channel_ids.inbound["orders"];
    let orders_record = storage.channel(orders_id).await.unwrap();
    assert_eq!(orders_record.receiver_workflow_id, Some(workflow.id()));
    assert_eq!(orders_record.sender_workflow_ids, HashSet::new());
    assert!(orders_record.has_external_sender);

    let traces_id = workflow.ids().channel_ids.outbound["traces"];
    let traces_record = storage.channel(traces_id).await.unwrap();
    assert_eq!(traces_record.receiver_workflow_id, None);
    assert_eq!(
        traces_record.sender_workflow_ids,
        HashSet::from_iter([workflow.id()])
    );
    assert!(!traces_record.has_external_sender);
    drop(storage);

    poll_fn_sx
        .send(initialize_task)
        .async_scope(test_initializing_workflow(&manager, workflow.ids()))
        .await;
}

async fn test_initializing_workflow(
    manager: &WorkflowManager<(), LocalStorage>,
    ids: &WorkflowAndChannelIds,
) {
    let receipt = manager.tick().await.unwrap().into_inner().unwrap();
    assert_eq!(receipt.executions().len(), 2);
    let main_execution = &receipt.executions()[0];
    assert_matches!(main_execution.function, ExecutedFunction::Entry { .. });

    let traces_id = ids.channel_ids.outbound["traces"];
    let transaction = manager.storage.readonly_transaction().await;
    let traces = transaction.channel(traces_id).await.unwrap();
    assert_eq!(traces.received_messages, 1);
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

    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let builder = manager
        .new_workflow::<()>(DEFINITION_ID, b"test_input".to_vec())
        .unwrap();
    builder.handle()[InboundChannel("orders")].close();
    builder.handle()[OutboundChannel("traces")].close();
    let workflow = builder.build().await.unwrap();
    let workflow_id = workflow.id();

    assert_eq!(workflow.ids().channel_ids.inbound["orders"], 0);
    assert_eq!(workflow.ids().channel_ids.outbound["traces"], 0);

    poll_fn_sx
        .send(test_channels)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();

    let mut handle = manager.workflow(workflow_id).await.unwrap().handle();
    let channel_info = handle[InboundChannel("orders")].channel_info().await;
    assert!(channel_info.is_closed);
    assert_eq!(channel_info.received_messages, 0);
    let channel_info = handle[OutboundChannel("traces")].channel_info().await;
    assert!(channel_info.is_closed);

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

    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let events_id = workflow.ids().channel_ids.outbound["events"];
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    poll_fn_sx
        .send(block_on_flush)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    manager.close_host_receiver(events_id).await;
    let channel_info = manager.channel(events_id).await.unwrap();
    assert!(channel_info.is_closed);

    workflow.update().await.unwrap();
    let events: Vec<_> = workflow.persisted().pending_wakeup_causes().collect();
    assert_matches!(
        events.as_slice(),
        [WakeUpCause::Flush { workflow_id: None, channel_name, .. }]
            if channel_name == "events"
    );

    poll_fn_sx
        .send(drop_inbound_channel)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    let channel_info = manager.channel(orders_id).await.unwrap();
    assert!(channel_info.is_closed);
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

    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    poll_fn_sx
        .send(initialize_task)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    if with_message {
        manager
            .send_message(orders_id, b"order #1".to_vec())
            .await
            .unwrap();
    }
    manager.close_host_sender(orders_id).await;

    if with_message {
        let receipt = poll_fn_sx
            .send(poll_inbound_channel)
            .async_scope(feed_message(&manager, workflow_id, "orders"))
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
    }

    let receipt = poll_fn_sx
        .send(test_closed_channel)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
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
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();

    let err = poll_fn_sx
        .send(|_| Err(Trap::new("oops")))
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap_err();
    let err = err.trap().display_reason().to_string();
    assert!(err.contains("oops"), "{err}");

    workflow.update().await.unwrap_err();
    let block_err = manager.tick().await.unwrap_err();
    assert!(block_err.nearest_timer_expiration().is_none());

    {
        let workflow = manager.any_workflow(workflow_id).await.unwrap();
        assert!(workflow.is_errored());
        let workflow = workflow.unwrap_errored();
        assert_eq!(workflow.id(), workflow_id);
        let err = workflow.error().trap().display_reason().to_string();
        assert!(err.contains("oops"), "{err}");
        assert_eq!(workflow.messages().count(), 0);

        workflow.consider_repaired().await.unwrap();
    }

    let receipt = poll_fn_sx
        .send(initialize_task)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    assert!(!receipt.executions().is_empty());
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
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    poll_fn_sx
        .send(poll_receiver)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
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
            .received_messages;
        assert_eq!(order_count, 1);
        let order = transaction.channel_message(orders_id, 0).await.unwrap();
        assert_eq!(order, b"order #1");
    }

    let receipt = poll_fn_sx
        .send(consume_message)
        .async_scope(feed_message(&manager, workflow_id, "orders"))
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

    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    poll_fn_sx
        .send(initialize_task)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    manager
        .send_message(orders_id, b"test".to_vec())
        .await
        .unwrap();
    let err = poll_fn_sx
        .send(error_after_consuming_message)
        .async_scope(feed_message(&manager, workflow_id, "orders"))
        .await
        .unwrap_err();
    let err = err.trap().display_reason().to_string();
    assert!(err.contains("oops"), "{err}");

    let channel_info = manager.channel(orders_id).await.unwrap();
    assert!(!channel_info.is_closed);
    assert_eq!(channel_info.received_messages, 1);

    {
        let workflow = manager.any_workflow(workflow_id).await.unwrap();
        let workflow = workflow.unwrap_errored();
        let mut message_refs: Vec<_> = workflow.messages().collect();
        assert_eq!(message_refs.len(), 1);
        let message_ref = message_refs.pop().unwrap();
        let message = message_ref.receive().await.unwrap();
        assert_eq!(message.decode().unwrap(), b"test");
        message_ref.drop_for_workflow().await.unwrap();

        let message_ref = workflow.messages().next().unwrap();
        message_ref.drop_for_workflow().await.unwrap_err();
        workflow.consider_repaired().await.unwrap();
    }

    workflow.update().await.unwrap();
    let mut channels = workflow.persisted().inbound_channels();
    let orders_cursor = channels.find_map(|(_, name, state)| {
        if name == "orders" {
            Some(state.received_message_count())
        } else {
            None
        }
    });
    assert_eq!(orders_cursor, Some(1));
}

#[async_std::test]
async fn workflow_not_consuming_inbound_message() {
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = create_test_manager(()).await;
    let mut workflow = create_test_workflow(&manager).await;
    let workflow_id = workflow.id();
    let orders_id = workflow.ids().channel_ids.inbound["orders"];

    poll_fn_sx
        .send(poll_receiver)
        .async_scope(tick_workflow(&manager, workflow_id))
        .await
        .unwrap();
    manager
        .send_message(orders_id, b"order #1".to_vec())
        .await
        .unwrap();
    let tick_result = poll_fn_sx
        .send(|_| Ok(Poll::Pending))
        .async_scope(manager.tick())
        .await;
    tick_result.unwrap().into_inner().unwrap();

    let transaction = manager.storage.readonly_transaction().await;
    let orders_channel = transaction.channel(orders_id).await.unwrap();
    assert_eq!(orders_channel.received_messages, 1);
    let order = transaction.channel_message(orders_id, 0).await.unwrap();
    assert_eq!(order, b"order #1");
    drop(transaction);

    // Workflow wakers should be consumed to not trigger the infinite loop.
    workflow.update().await.unwrap();
    let events: Vec<_> = workflow.persisted().pending_wakeup_causes().collect();
    assert!(events.is_empty(), "{events:?}");
}
