//! Workflow tests for spawning child workflows.

use std::borrow::Cow;

use super::*;
use crate::{engine::DefineWorkflow, manager::StashStub, workflow::WorkflowAndChannelIds};
use tardigrade::{
    handle::{Handle, ReceiverAt, SenderAt},
    interface::Interface,
    spawn::{HostError, ManageInterfaces},
};

#[derive(Debug)]
struct NewWorkflowCall {
    stub_id: WorkflowId,
    args: Vec<u8>,
}

#[derive(Debug)]
struct MockWorkflowManager {
    interface: Interface,
    calls: Vec<NewWorkflowCall>,
}

impl MockWorkflowManager {
    const INTERFACE_BYTES: &'static [u8] = br#"{
        "v": 0,
        "handles": {
            "commands": { "receiver": {} },
            "traces": { "sender": {} }
        }
    }"#;

    fn new() -> Self {
        Self {
            interface: Interface::from_bytes(Self::INTERFACE_BYTES),
            calls: vec![],
        }
    }

    fn init_single_child(self, parent: &mut Workflow<MockInstance>) {
        let mut calls = self.calls;
        assert_eq!(calls.len(), 1);
        let call = calls.pop().unwrap();

        let result = if call.args == b"err_input" {
            Err(HostError::new("invalid input!"))
        } else {
            let channel_ids = mock_channel_ids(&self.interface, &mut 100);
            for id_handle in channel_ids.values() {
                let id = *id_handle.as_ref().factor();
                parent.notify_on_channel_init(id, id, &mut Receipt::default());
            }

            Ok(WorkflowAndChannelIds {
                workflow_id: 1,
                channel_ids,
            })
        };
        parent.notify_on_child_init(call.stub_id, result, &mut Receipt::default());
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

impl StashStub for MockWorkflowManager {
    fn stash_workflow(
        &mut self,
        stub_id: WorkflowId,
        id: &str,
        args: Vec<u8>,
        channels: ChannelIds,
    ) -> anyhow::Result<()> {
        assert_eq!(id, "test:latest");
        assert_eq!(channels.len(), 2);
        self.calls.push(NewWorkflowCall { stub_id, args });
        Ok(())
    }

    fn stash_channel(&mut self, _stub_id: ChannelId) {
        // Do nothing
    }
}

fn create_workflow_with_manager(poll_fns: MockAnswers) -> Workflow<MockInstance> {
    let definition = MockDefinition::new(poll_fns);
    let channel_ids = mock_channel_ids(definition.interface(), &mut 1);
    let services = Services {
        clock: Arc::new(MockScheduler::default()),
        stubs: Some(Box::new(MockWorkflowManager::new())),
        tracer: None,
    };
    let data = WorkflowData::new(definition.interface(), channel_ids, services);
    let args = vec![].into();
    let mut workflow = Workflow::new(&definition, data, Some(args)).unwrap();
    workflow.initialize().unwrap();
    workflow
}

fn spawn_child_workflow(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    // Emulate getting interface.
    let interface = ctx.data().workflow_interface("test:latest").unwrap();
    assert_eq!(interface.handles().len(), 2);
    assert!(interface.handle(ReceiverAt("commands")).is_ok());
    assert!(interface.handle(SenderAt("traces")).is_ok());

    // Emulate creating a workflow.
    ctx.data_mut().create_workflow_stub(
        0,
        "test:latest",
        b"child_input".to_vec(),
        configure_handles(),
    )?;
    Ok(Poll::Pending)
}

fn get_child_workflow_channel(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let child_id = 1;
    // Emulate getting a receiver for the workflow.
    let child = ctx.data().persisted.child_workflow(child_id).unwrap();
    let traces_id = child.channels().channel_id("traces").unwrap();
    // ...then polling this channel
    let mut traces = ctx.data_mut().receiver(traces_id);
    let poll_res = traces.poll_next().into_inner(ctx)?;
    assert!(poll_res.is_pending()); // Poll::Pending

    Ok(Poll::Pending)
}

// **NB.** This only works if called once per test.
fn configure_handles() -> ChannelIds {
    let mut config = ChannelIds::new();
    config.insert("commands".into(), Handle::Receiver(1));
    config.insert("traces".into(), Handle::Sender(2));
    config
}

fn init_child(workflow: &mut Workflow<MockInstance>) {
    let manager = workflow.take_services().stubs.unwrap();
    let manager = manager.downcast::<MockWorkflowManager>();
    manager.init_single_child(workflow);
}

#[test]
fn spawning_child_workflow() {
    let (poll_fns, mut poll_fns_sx) = MockAnswers::channel();
    let mut workflow = poll_fns_sx
        .send(spawn_child_workflow)
        .scope(|| create_workflow_with_manager(poll_fns));
    init_child(&mut workflow);
    poll_fns_sx
        .send(get_child_workflow_channel)
        .scope(|| workflow.tick().unwrap());

    let mut children: Vec<_> = workflow.data().persisted.child_workflows().collect();
    assert_eq!(children.len(), 1);
    let (_, child) = children.pop().unwrap();
    let commands_id = child.channels().channel_id("commands").unwrap();
    let traces_id = child.channels().channel_id("traces").unwrap();
    assert_ne!(commands_id, traces_id);
}

#[test]
fn spawning_child_workflow_with_unknown_interface() {
    let spawn_with_unknown_interface: MockPollFn = |ctx| {
        let bogus_id = "bogus:latest";
        let interface = ctx.data().workflow_interface(bogus_id);
        assert!(interface.is_none());

        let args = b"err_input".to_vec();
        let err = ctx
            .data_mut()
            .create_workflow_stub(0, bogus_id, args, configure_handles())
            .unwrap_err()
            .to_string();

        assert!(err.contains("workflow with ID `bogus:latest`"), "{err}");
        Ok(Poll::Pending)
    };

    let poll_fns = MockAnswers::from_value(spawn_with_unknown_interface);
    let workflow = create_workflow_with_manager(poll_fns);

    assert!(workflow.data().persisted.child_workflows().next().is_none());
}

#[test]
fn spawning_child_workflow_with_extra_channel() {
    let spawn_with_extra_channel: MockPollFn = |ctx| {
        let id = "test:latest";
        let args = b"child_input".to_vec();
        let mut handles = configure_handles();
        handles.insert(id.into(), Handle::Receiver(3));

        let err = ctx
            .data_mut()
            .create_workflow_stub(0, id, args, handles)
            .unwrap_err()
            .to_string();

        assert_eq!(err, "extra handles: `test:latest`");
        Ok(Poll::Pending)
    };
    let poll_fns = MockAnswers::from_value(spawn_with_extra_channel);
    let workflow = create_workflow_with_manager(poll_fns);

    assert!(workflow.data().persisted.child_workflows().next().is_none());
}

#[test]
fn spawning_child_workflow_with_host_error() {
    let spawn_with_host_error: MockPollFn = |ctx| {
        let id = "test:latest";
        let args = b"err_input".to_vec();
        ctx.data_mut()
            .create_workflow_stub(0, id, args, configure_handles())?;

        Ok(Poll::Pending)
    };

    let poll_fns = MockAnswers::from_values([spawn_with_host_error]);
    let mut workflow = create_workflow_with_manager(poll_fns);
    init_child(&mut workflow);
    assert!(workflow.data().persisted.child_workflows().next().is_none());
}

fn consume_message_from_child(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let child_channels = ctx.data().persisted.child_workflow(1).unwrap().channels();
    let traces_id = child_channels.channel_id("traces").unwrap();
    let commands_id = child_channels.channel_id("commands").unwrap();

    let mut traces = ctx.data_mut().receiver(traces_id);
    let poll_res = traces.poll_next().into_inner(ctx)?;
    assert_eq!(extract_message(poll_res), b"trace #1");

    // Emit a command to the child workflow.
    let mut commands = ctx.data_mut().sender(commands_id);
    let poll_res = commands.poll_ready().into_inner(ctx)?;
    assert_matches!(poll_res, Poll::Ready(Ok(_)));

    let mut commands = ctx.data_mut().sender(commands_id);
    commands.start_send(b"command #1".to_vec())?;
    let poll_res = commands.poll_flush().into_inner(ctx)?;
    assert!(poll_res.is_pending());

    Ok(Poll::Pending)
}

#[test]
fn consuming_message_from_child_workflow() {
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let mut workflow = poll_fn_sx
        .send(spawn_child_workflow)
        .scope(|| create_workflow_with_manager(poll_fns));

    init_child(&mut workflow);
    workflow.data_mut().current_wakeup_cause = Some(WakeUpCause::StubInitialized);
    workflow.data_mut().task(0).schedule_wakeup();
    workflow.data_mut().current_wakeup_cause = None;
    // ^ Emulates wakeup notification for workflow stub

    poll_fn_sx.send(get_child_workflow_channel).scope(|| {
        workflow.tick().unwrap(); // makes the workflow listen to the `traces` child channel
    });

    let persisted = &workflow.data().persisted;
    let child = persisted.child_workflow(1).unwrap();
    let child_commands_id = child.channels().channel_id("commands").unwrap();
    let child_traces_id = child.channels().channel_id("traces").unwrap();

    workflow
        .data_mut()
        .persisted
        .push_message_for_receiver(child_traces_id, b"trace #1".to_vec())
        .unwrap();
    let receipt = poll_fn_sx
        .send(consume_message_from_child)
        .scope(|| workflow.tick())
        .unwrap();
    assert_child_inbound_message_receipt(&workflow, &receipt);

    let messages = workflow.data_mut().drain_messages();
    assert_eq!(messages[&child_commands_id].len(), 1);
    assert_eq!(messages[&child_commands_id][0].as_ref(), b"command #1");

    let persisted = &workflow.data().persisted;
    let child_traces = &persisted.channels.receivers[&child_traces_id];
    assert_eq!(child_traces.received_messages, 1);
    let child_commands = &persisted.channels.senders[&child_commands_id];
    assert_eq!(child_commands.flushed_messages, 1);
}

fn assert_child_inbound_message_receipt(workflow: &Workflow<MockInstance>, receipt: &Receipt) {
    let mut children = workflow.data().persisted.child_workflows();
    let (_, child) = children.next().unwrap();
    let child_commands_id = child.channels().channel_id("commands").unwrap();
    let child_traces_id = child.channels().channel_id("traces").unwrap();

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
                kind: ChannelEventKind::ReceiverPolled { result: Poll::Ready(Some(_)) },
                channel_id: traces,
            },
            ChannelEvent {
                kind: ChannelEventKind::SenderReady {
                    result: Poll::Ready(Ok(())),
                },
                channel_id: commands,
            },
            ChannelEvent {
                kind: ChannelEventKind::OutboundMessageSent { .. },
                channel_id: commands2,
            },
            ChannelEvent {
                kind: ChannelEventKind::SenderFlushed { result: Poll::Pending },
                channel_id: commands3,
            },
        ] if *traces == child_traces_id && *commands == child_commands_id
            && *commands2 == child_commands_id && *commands3 == child_commands_id
    );
}
