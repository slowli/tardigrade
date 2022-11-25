//! Workflow tests for spawning child workflows.

use wasmtime::ExternRef;

use std::borrow::Cow;

use super::*;
use crate::{data::SpawnFunctions, module::StashWorkflow, utils::copy_bytes_from_wasm};
use tardigrade::{
    abi::TryFromWasm,
    interface::{ChannelKind, Interface},
    spawn::{HostError, ManageInterfaces},
    ChannelId,
};

#[derive(Debug)]
struct NewWorkflowCall {
    stub_id: WorkflowId,
    args: Vec<u8>,
    channels: ChannelsConfig<ChannelId>,
}

#[derive(Debug)]
struct MockWorkflowManager {
    interface: Interface,
    calls: Vec<NewWorkflowCall>,
    next_workflow_id: WorkflowId,
    next_channel_id: ChannelId,
}

impl MockWorkflowManager {
    const INTERFACE_BYTES: &'static [u8] = br#"{"v":0,"in":{"commands":{}},"out":{"traces":{}}}"#;

    fn new() -> Self {
        Self {
            interface: Interface::from_bytes(Self::INTERFACE_BYTES),
            calls: vec![],
            next_channel_id: 1,
            next_workflow_id: 1,
        }
    }

    fn init_single_child<'s>(
        &'s mut self,
        mut parent: PersistedWorkflow,
        clock: &'s MockScheduler,
    ) -> Workflow<'s> {
        assert_eq!(self.calls.len(), 1);
        let call = self.calls.pop().unwrap();

        if call.args == b"err_input" {
            parent.notify_on_child_spawn_error(call.stub_id, HostError::new("invalid input!"));
        } else {
            let channel_ids = mock_channel_ids(&self.interface, &mut self.next_channel_id);
            let child_id = self.next_workflow_id;
            self.next_workflow_id += 1;
            parent.notify_on_child_init(call.stub_id, child_id, &call.channels, channel_ids);
        }

        restore_workflow(
            parent,
            Services {
                clock,
                workflows: Some(self),
                tracer: None,
            },
        )
    }
}

impl ManageInterfaces for MockWorkflowManager {
    fn interface(&self, id: &str) -> Option<Cow<'_, Interface>> {
        if id == "test:latest" {
            Some(Cow::Borrowed(&self.interface))
        } else {
            None
        }
    }
}

impl StashWorkflow for MockWorkflowManager {
    fn stash_workflow(
        &mut self,
        stub_id: WorkflowId,
        id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<ChannelId>,
    ) {
        assert_eq!(id, "test:latest");
        assert_eq!(channels.inbound.len(), 1);
        assert_eq!(channels.outbound.len(), 1);
        self.calls.push(NewWorkflowCall {
            stub_id,
            args,
            channels,
        });
    }
}

fn create_workflow_with_manager(services: Services<'_>) -> Workflow<'_> {
    let engine = WorkflowEngine::default();
    let spawner = WorkflowModule::new(&engine, ExportsMock::MOCK_MODULE_BYTES)
        .unwrap()
        .for_untyped_workflow("TestWorkflow")
        .unwrap();

    let channel_ids = mock_channel_ids(spawner.interface(), &mut 0);
    let mut workflow = spawner
        .spawn(b"test_input".to_vec(), channel_ids, services)
        .unwrap();
    workflow.initialize().unwrap();
    workflow
}

fn spawn_child_workflow(mut ctx: StoreContextMut<'_, WorkflowData>) -> anyhow::Result<Poll<()>> {
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
    let handles = SpawnFunctions::create_channel_handles();
    assert!(handles.is_some());
    configure_handles(ctx.as_context_mut(), handles.clone())?;
    let stub = SpawnFunctions::spawn(
        ctx.as_context_mut(),
        id_ptr,
        id_len,
        args_ptr,
        args_len,
        handles,
    )?;
    assert!(stub.is_some());

    let maybe_workflow =
        SpawnFunctions::poll_workflow_init(ctx.as_context_mut(), stub, POLL_CX, ERROR_PTR)?;
    assert!(maybe_workflow.is_none()); // Poll::Pending

    Ok(Poll::Pending)
}

fn get_child_workflow_channel(
    mut ctx: StoreContextMut<'_, WorkflowData>,
) -> anyhow::Result<Poll<()>> {
    let stub = Some(HostResource::WorkflowStub(0).into_ref());
    let workflow =
        SpawnFunctions::poll_workflow_init(ctx.as_context_mut(), stub, POLL_CX, ERROR_PTR)?;
    assert!(workflow.is_some());

    // Emulate getting an inbound channel for the workflow.
    let (name_ptr, name_len) = WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"traces")?;
    let traces = WorkflowFunctions::get_receiver(
        ctx.as_context_mut(),
        workflow,
        name_ptr,
        name_len,
        ERROR_PTR,
    )?;
    assert!(traces.is_some());

    // ...then polling this channel
    let poll_res =
        WorkflowFunctions::poll_next_for_receiver(ctx.as_context_mut(), traces, POLL_CX).unwrap();
    assert_eq!(poll_res, -1); // Poll::Pending

    Ok(Poll::Pending)
}

fn configure_handles(
    mut ctx: StoreContextMut<'_, WorkflowData>,
    handles: Option<ExternRef>,
) -> anyhow::Result<()> {
    let (name_ptr, name_len) =
        WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"commands")?;
    SpawnFunctions::set_channel_handle(
        ctx.as_context_mut(),
        handles.clone(),
        ChannelKind::Inbound.into_abi_in_wasm(),
        name_ptr,
        name_len,
        0,
    )?;
    let (name_ptr, name_len) = WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"traces")?;
    SpawnFunctions::set_channel_handle(
        ctx.as_context_mut(),
        handles,
        ChannelKind::Outbound.into_abi_in_wasm(),
        name_ptr,
        name_len,
        0,
    )?;
    Ok(())
}

#[test]
fn spawning_child_workflow() {
    let poll_fns = Answers::from_values([
        spawn_child_workflow as MockPollFn,
        get_child_workflow_channel,
    ]);
    let _guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let mut manager = MockWorkflowManager::new();
    let workflow = create_workflow_with_manager(Services {
        clock: &clock,
        workflows: Some(&mut manager),
        tracer: None,
    });

    let persisted = workflow.persist();
    let mut workflow = manager.init_single_child(persisted, &clock);
    workflow.tick().unwrap();

    let mut children: Vec<_> = workflow.data().persisted.child_workflows().collect();
    assert_eq!(children.len(), 1);
    let (_, child) = children.pop().unwrap();
    let commands_id = child.channels().sender_id("commands").unwrap();
    let traces_id = child.channels().receiver_id("traces").unwrap();
    assert_ne!(commands_id, traces_id);
}

#[test]
fn spawning_child_workflow_with_unknown_interface() {
    let spawn_with_unknown_interface: MockPollFn = |mut ctx| {
        let (bogus_id_ptr, bogus_id_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"bogus:latest")?;
        let interface_res =
            SpawnFunctions::workflow_interface(ctx.as_context_mut(), bogus_id_ptr, bogus_id_len)?;
        assert_eq!(interface_res, -1); // `None`

        let (args_ptr, args_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"err_input")?;
        let handles = SpawnFunctions::create_channel_handles();
        configure_handles(ctx.as_context_mut(), handles.clone())?;

        let err = SpawnFunctions::spawn(
            ctx.as_context_mut(),
            bogus_id_ptr,
            bogus_id_len,
            args_ptr,
            args_len,
            handles,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("workflow with ID `bogus:latest`"), "{err}");
        Ok(Poll::Pending)
    };

    let poll_fns = Answers::from_value(spawn_with_unknown_interface);
    let _guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let mut manager = MockWorkflowManager::new();
    let workflow = create_workflow_with_manager(Services {
        clock: &clock,
        workflows: Some(&mut manager),
        tracer: None,
    });

    assert!(workflow.data().persisted.child_workflows().next().is_none());
}

#[test]
fn spawning_child_workflow_with_extra_channel() {
    let spawn_with_extra_channel: MockPollFn = |mut ctx| {
        let (id_ptr, id_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"test:latest")?;
        let (args_ptr, args_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"child_input")?;
        let handles = SpawnFunctions::create_channel_handles();
        configure_handles(ctx.as_context_mut(), handles.clone())?;

        SpawnFunctions::set_channel_handle(
            ctx.as_context_mut(),
            handles.clone(),
            ChannelKind::Inbound.into_abi_in_wasm(),
            id_ptr,
            id_len,
            0,
        )?;

        let err = SpawnFunctions::spawn(
            ctx.as_context_mut(),
            id_ptr,
            id_len,
            args_ptr,
            args_len,
            handles,
        )
        .unwrap_err()
        .to_string();

        assert_eq!(err, "extra inbound handles: `test:latest`");
        Ok(Poll::Pending)
    };

    let poll_fns = Answers::from_value(spawn_with_extra_channel);
    let _guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let mut manager = MockWorkflowManager::new();
    let workflow = create_workflow_with_manager(Services {
        clock: &clock,
        workflows: Some(&mut manager),
        tracer: None,
    });

    assert!(workflow.data().persisted.child_workflows().next().is_none());
}

#[test]
fn spawning_child_workflow_with_host_error() {
    let spawn_with_host_error: MockPollFn = |mut ctx| {
        let (id_ptr, id_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"test:latest")?;
        let (args_ptr, args_len) =
            WasmAllocator::new(ctx.as_context_mut()).copy_to_wasm(b"err_input")?;
        let handles = SpawnFunctions::create_channel_handles();
        configure_handles(ctx.as_context_mut(), handles.clone())?;

        let stub = SpawnFunctions::spawn(
            ctx.as_context_mut(),
            id_ptr,
            id_len,
            args_ptr,
            args_len,
            handles,
        )?;
        assert!(stub.is_some());

        Ok(Poll::Pending)
    };
    let poll_child_stub: MockPollFn = |mut ctx| {
        let stub = Some(HostResource::WorkflowStub(0).into_ref());
        let workflow =
            SpawnFunctions::poll_workflow_init(ctx.as_context_mut(), stub, POLL_CX, ERROR_PTR)?;

        assert!(workflow.is_none());
        let mut err_bytes = [0_u8; 8];
        let memory = ctx.data().exports().memory;
        memory
            .read(ctx.as_context_mut(), ERROR_PTR as usize, &mut err_bytes)
            .unwrap();
        let (err_ptr, err_len) = decode_string(i64::from_le_bytes(err_bytes));
        let err = copy_string_from_wasm(ctx.as_context_mut(), &memory, err_ptr, err_len)?;
        assert!(err.contains("invalid input"), "{}", err);

        Ok(Poll::Pending)
    };

    let poll_fns = Answers::from_values([spawn_with_host_error, poll_child_stub]);
    let _guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let mut manager = MockWorkflowManager::new();
    let workflow = create_workflow_with_manager(Services {
        clock: &clock,
        workflows: Some(&mut manager),
        tracer: None,
    });

    let persisted = workflow.persist();
    let mut workflow = manager.init_single_child(persisted, &clock);
    workflow.tick().unwrap();
    assert!(workflow.data().persisted.child_workflows().next().is_none());
}

fn consume_message_from_child(
    mut ctx: StoreContextMut<'_, WorkflowData>,
) -> anyhow::Result<Poll<()>> {
    let traces = Some(ctx.data().inbound_channel_ref(Some(1), "traces"));
    let commands = Some(ctx.data_mut().outbound_channel_ref(Some(1), "commands"));

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
    let (poll_fns, mut poll_fn_sx) = Answers::channel();
    let exports_guard = ExportsMock::prepare(poll_fns);
    let clock = MockScheduler::default();
    let mut manager = MockWorkflowManager::new();
    let workflow = poll_fn_sx.send(spawn_child_workflow).scope(|| {
        create_workflow_with_manager(Services {
            clock: &clock,
            workflows: Some(&mut manager),
            tracer: None,
        })
    });
    let persisted = workflow.persist();
    let mut workflow = manager.init_single_child(persisted, &clock);
    poll_fn_sx.send(get_child_workflow_channel).scope(|| {
        workflow.tick().unwrap(); // makes the workflow listen to the `traces` child channel
    });

    let persisted = &workflow.data().persisted;
    let child = persisted.child_workflow(1).unwrap();
    let child_commands_id = child.channels().sender_id("commands").unwrap();
    let child_traces_id = child.channels().receiver_id("traces").unwrap();

    workflow
        .data_mut()
        .persisted
        .push_inbound_message(child_traces_id, b"trace #1".to_vec())
        .unwrap();
    let receipt = poll_fn_sx
        .send(consume_message_from_child)
        .scope(|| workflow.tick())
        .unwrap();

    let exports = exports_guard.into_inner();
    assert_eq!(exports.consumed_wakers.len(), 2); // child init + child `traces` handle
    assert_child_inbound_message_receipt(&workflow, &receipt);

    let messages = workflow.data_mut().drain_messages();
    assert_eq!(messages[&child_commands_id].len(), 1);
    assert_eq!(messages[&child_commands_id][0].as_ref(), b"command #1");

    let persisted = &workflow.data().persisted;
    let child_traces = &persisted.channels.inbound[&child_traces_id];
    assert_eq!(child_traces.received_messages, 1);
    let child_commands = &persisted.channels.outbound[&child_commands_id];
    assert_eq!(child_commands.flushed_messages, 1);
}

fn assert_child_inbound_message_receipt(workflow: &Workflow<'_>, receipt: &Receipt) {
    let mut children = workflow.data().persisted.child_workflows();
    let (_, child) = children.next().unwrap();
    let child_commands_id = child.channels().sender_id("commands").unwrap();
    let child_traces_id = child.channels().receiver_id("traces").unwrap();

    assert_matches!(
        &receipt.executions()[0],
        Execution {
            function: ExecutedFunction::Waker {
                wake_up_cause: WakeUpCause::InboundMessage {
                    channel_id: traces,
                    message_index: 0,
                },
                ..
            },
            events,
            ..
        } if *traces == child_traces_id && events.is_empty()
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
                channel_id: traces,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelReady {
                    result: Poll::Ready(Ok(())),
                },
                channel_id: commands,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundMessageSent { .. },
                channel_id: commands2,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundChannelFlushed { result: Poll::Pending },
                channel_id: commands3,
            },
        ] if *traces == child_traces_id && *commands == child_commands_id
            && *commands2 == child_commands_id && *commands3 == child_commands_id
    );
}
