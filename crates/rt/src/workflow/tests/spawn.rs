//! Workflow tests for spawning child workflows.

use std::sync::Mutex;

use super::*;
use crate::{
    data::SpawnFunctions, services::ManageWorkflows, utils::copy_bytes_from_wasm, ChannelId,
};
use tardigrade_shared::{abi::TryFromWasm, interface::ChannelKind, SpawnError, WorkflowId};

#[derive(Debug)]
struct NewWorkflowCall {
    args: Vec<u8>,
    commands_channel_id: ChannelId,
    traces_channel_id: ChannelId,
}

#[derive(Debug)]
struct MockWorkflowManager {
    interface: Interface<()>,
    calls: Mutex<Vec<NewWorkflowCall>>,
}

impl MockWorkflowManager {
    const INTERFACE_BYTES: &'static [u8] = br#"{"v":0,"in":{"commands":{}},"out":{"traces":{}}}"#;

    fn new() -> Self {
        Self {
            interface: Interface::from_bytes(Self::INTERFACE_BYTES),
            calls: Mutex::new(vec![]),
        }
    }
}

impl ManageWorkflows for MockWorkflowManager {
    fn interface(&self, id: &str) -> Option<&Interface<()>> {
        if id == "test:latest" {
            Some(&self.interface)
        } else {
            None
        }
    }

    fn create_workflow(
        &self,
        id: &str,
        args: Vec<u8>,
        handles: &ChannelHandles<'_>,
    ) -> Result<WorkflowId, SpawnError> {
        assert_eq!(id, "test:latest");
        assert_eq!(handles.inbound.len(), 1);
        assert_eq!(handles.outbound.len(), 1);

        if args == b"err_input" {
            Err(SpawnError::new("invalid input!"))
        } else {
            let mut calls = self.calls.lock().unwrap();
            calls.push(NewWorkflowCall {
                args,
                commands_channel_id: handles.inbound["commands"],
                traces_channel_id: handles.outbound["traces"],
            });
            Ok(calls.len() as WorkflowId)
        }
    }
}

fn create_workflow_with_manager(manager: Arc<MockWorkflowManager>) -> Receipt<Workflow<()>> {
    let engine = WorkflowEngine::default();
    let mut spawner = WorkflowModule::new(&engine, ExportsMock::MOCK_MODULE_BYTES)
        .unwrap()
        .for_untyped_workflow("TestWorkflow")
        .unwrap()
        .with_clock(MockScheduler::default());
    spawner.services.workflows = manager;

    spawner
        .spawn(b"test_input".to_vec())
        .unwrap()
        .init()
        .unwrap()
}

fn spawn_child_workflow(mut ctx: StoreContextMut<'_, WorkflowData>) -> Result<Poll<()>, Trap> {
    // Emulate getting interface.
    let (id_ptr, id_len) = WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"test:latest")?;
    let interface_res = SpawnFunctions::workflow_interface(ctx.as_context_mut(), id_ptr, id_len)?;
    {
        let (ptr, len) = decode_string(interface_res);
        let memory = ctx.data().exports().memory;
        let interface = copy_bytes_from_wasm(&ctx, &memory, ptr, len)?;
        let interface = Interface::from_bytes(&interface);
        assert_eq!(interface.inbound_channels().len(), 1);
        assert!(interface.inbound_channel("commands").is_some());
        assert_eq!(interface.outbound_channels().len(), 1);
        assert!(interface.outbound_channel("traces").is_some());
    }

    // Emulate creating a spawner.
    let (args_ptr, args_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"child_input")?;
    let spawner =
        SpawnFunctions::create_spawner(ctx.as_context_mut(), id_ptr, id_len, args_ptr, args_len)?;
    assert!(spawner.is_some());

    let workflow = SpawnFunctions::spawn(ctx.as_context_mut(), spawner, ERROR_PTR)?;
    assert!(workflow.is_some());

    // Emulate getting an inbound channel for the workflow.
    let (name_ptr, name_len) = WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"traces")?;
    let traces = WorkflowFunctions::get_receiver(
        ctx.as_context_mut(),
        workflow,
        name_ptr,
        name_len,
        ERROR_PTR,
    )
    .unwrap();
    assert!(traces.is_some());

    // ...then polling this channel
    let poll_res =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), traces, POLL_CX).unwrap();
    assert_eq!(poll_res, -1); // Poll::Pending

    Ok(Poll::Pending)
}

#[test]
fn spawning_child_workflow() {
    let poll_fns = Answers::from_value(spawn_child_workflow as MockPollFn);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = Arc::new(MockWorkflowManager::new());
    let workflow = create_workflow_with_manager(Arc::clone(&manager)).into_inner();

    let mut children: Vec<_> = workflow.child_workflows().collect();
    assert_eq!(children.len(), 1);
    let (_, child) = children.pop().unwrap();
    let commands = child.outbound_channel("commands").unwrap();
    let traces = child.inbound_channel("traces").unwrap();

    let calls = manager.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].args, b"child_input");
    assert_eq!(calls[0].commands_channel_id, commands.id());
    assert_eq!(calls[0].traces_channel_id, traces.id());
}

fn spawn_child_workflow_errors(
    mut ctx: StoreContextMut<'_, WorkflowData>,
) -> Result<Poll<()>, Trap> {
    let (bogus_id_ptr, bogus_id_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"bogus:latest")?;
    let interface_res =
        SpawnFunctions::workflow_interface(ctx.as_context_mut(), bogus_id_ptr, bogus_id_len)?;
    assert_eq!(interface_res, -1); // `None`

    let (args_ptr, args_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"err_input")?;
    let err = SpawnFunctions::create_spawner(
        ctx.as_context_mut(),
        bogus_id_ptr,
        bogus_id_len,
        args_ptr,
        args_len,
    )
    .unwrap_err()
    .to_string();

    assert!(err.contains("workflow with ID `bogus:latest`"), "{}", err);

    let (id_ptr, id_len) = WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"test:latest")?;
    let spawner =
        SpawnFunctions::create_spawner(ctx.as_context_mut(), id_ptr, id_len, args_ptr, args_len)?;
    assert!(spawner.is_some());

    let err = SpawnFunctions::set_spawner_handle(
        ctx.as_context_mut(),
        spawner.clone(),
        ChannelKind::Inbound.into_abi_in_wasm(),
        bogus_id_ptr,
        bogus_id_len,
    )
    .unwrap_err()
    .to_string();

    assert!(
        err.contains("inbound channel `bogus:latest` not found"),
        "{}",
        err
    );

    let workflow = SpawnFunctions::spawn(ctx.as_context_mut(), spawner, ERROR_PTR)?;
    assert!(workflow.is_none());
    {
        let mut err_bytes = [0_u8; 8];
        let memory = ctx.data().exports().memory;
        memory
            .read(ctx.as_context_mut(), ERROR_PTR as usize, &mut err_bytes)
            .unwrap();
        let (err_ptr, err_len) = decode_string(i64::from_le_bytes(err_bytes));
        let err = copy_string_from_wasm(ctx.as_context_mut(), &memory, err_ptr, err_len)?;
        assert!(err.contains("invalid input"), "{}", err);
    }

    Ok(Poll::Pending)
}

#[test]
fn spawning_child_workflow_errors() {
    let poll_fns = Answers::from_value(spawn_child_workflow_errors as MockPollFn);
    let _guard = ExportsMock::prepare(poll_fns);
    let manager = Arc::new(MockWorkflowManager::new());
    let workflow = create_workflow_with_manager(manager).into_inner();

    assert!(workflow.child_workflows().next().is_none());
}

fn consume_message_from_child(
    mut ctx: StoreContextMut<'_, WorkflowData>,
) -> Result<Poll<()>, Trap> {
    let traces = Some(WorkflowData::inbound_channel_ref(Some(1), "traces"));
    let commands = Some(WorkflowData::outbound_channel_ref(Some(1), "commands"));

    let poll_res =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), traces, POLL_CX)?;

    let (msg_ptr, msg_len) = decode_string(poll_res);
    let memory = ctx.data().exports().memory;
    let message = copy_string_from_wasm(&ctx, &memory, msg_ptr, msg_len)?;
    assert_eq!(message, "trace #1");

    // Emit a command to the child workflow.
    let poll_res =
        WorkflowFunctions::poll_ready_for_sender(ctx.as_context_mut(), commands.clone(), POLL_CX)?;
    assert_eq!(poll_res, 0); // Poll::Ready

    let (command_ptr, command_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"command #1")?;
    WorkflowFunctions::start_send(
        ctx.as_context_mut(),
        commands.clone(),
        command_ptr,
        command_len,
    )?;

    let poll_res =
        WorkflowFunctions::poll_flush_for_sender(ctx.as_context_mut(), commands, POLL_CX)?;
    assert_eq!(poll_res, -1); // Poll::Pending

    Ok(Poll::Pending)
}

#[test]
fn consuming_message_from_child_workflow() {
    let poll_fns = Answers::from_values([spawn_child_workflow, consume_message_from_child]);
    let exports_guard = ExportsMock::prepare(poll_fns);
    let manager = Arc::new(MockWorkflowManager::new());
    let mut workflow = create_workflow_with_manager(manager).into_inner();

    workflow
        .push_inbound_message(Some(1), "traces", b"trace #1".to_vec())
        .unwrap();
    let receipt = workflow.tick().unwrap();

    let exports = exports_guard.into_inner();
    assert_eq!(exports.consumed_wakers.len(), 1);
    assert!(exports.consumed_wakers.contains(&0));

    assert_child_inbound_message_receipt(&receipt);

    let (start_idx, commands) = workflow.take_outbound_messages(Some(1), "commands");
    assert_eq!(start_idx, 0);
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0], b"command #1");
    let child = workflow.child_workflow(1).unwrap();
    assert_eq!(
        child.inbound_channel("traces").unwrap().received_messages(),
        1
    );
    assert_eq!(
        child
            .outbound_channel("commands")
            .unwrap()
            .flushed_messages(),
        1
    );
}

fn assert_child_inbound_message_receipt(receipt: &Receipt) {
    assert_matches!(
        &receipt.executions()[0],
        Execution {
            function: ExecutedFunction::Waker {
                waker_id: 0,
                wake_up_cause: WakeUpCause::InboundMessage {
                    workflow_id: Some(1),
                    channel_name,
                    message_index: 0,
                }
            },
            events,
        } if channel_name == "traces" && events.is_empty()
    );
    let task_execution = &receipt.executions()[1];
    assert_matches!(
        task_execution.function,
        ExecutedFunction::Task { task_id: 0, .. }
    );

    let events = task_execution.events.iter().map(|evt| match evt {
        Event::Channel(evt) => evt,
        _ => panic!("unexpected event"),
    });
    let events: Vec<_> = events.collect();
    assert_matches!(
        &events[0..4],
        [
            ChannelEvent {
                kind: ChannelEventKind::InboundChannelPolled { result: Poll::Ready(Some(_)) },
                channel_name: traces,
                workflow_id: Some(1),
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelReady {
                    result: Poll::Ready(Ok(())),
                },
                channel_name: commands,
                workflow_id: Some(1),
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundMessageSent { .. },
                channel_name: commands2,
                workflow_id: Some(1),
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelFlushed { result: Poll::Pending },
                channel_name: commands3,
                workflow_id: Some(1),
            },
        ] if traces == "traces" && commands == "commands" && commands2 == "commands"
            && commands3 == "commands"
    );
}