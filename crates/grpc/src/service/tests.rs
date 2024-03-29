//! Tests for `RuntimeWrapper`.

use assert_matches::assert_matches;
use futures::TryStreamExt;
use tokio::{task, time};
use tonic::{Code, Request};

use std::{collections::HashMap, sync::Arc, task::Poll, time::Duration};

use crate::{
    proto::{
        self,
        channels_service_server::ChannelsService,
        create_workflow_request,
        persisted_workflow::channel,
        push_messages_request::{pushed, Pushed},
        runtime_service_server::RuntimeService,
        test_service_server::TestService,
        update_worker_request::UpdateType,
        workers_service_server::WorkersService,
    },
    RuntimeWrapper, StorageWrapper,
};
use tardigrade::{
    interface::{ArgsSpec, InterfaceBuilder, ReceiverSpec, SenderSpec},
    TimerDefinition, TimerId, WorkflowId,
};
use tardigrade_rt::{
    engine::AsWorkflowData,
    runtime::Runtime,
    storage::{LocalStorage, Streaming},
    test::engine::{MockAnswers, MockEngine, MockInstance},
    MockScheduler, Schedule, TokioScheduler,
};

type TestRuntime<C> = Runtime<MockEngine, C, Streaming<Arc<LocalStorage>>>;

fn create_storage() -> Streaming<Arc<LocalStorage>> {
    let storage = Arc::new(LocalStorage::default());
    let (storage, routing_task) = Streaming::new(storage);
    task::spawn(routing_task);
    storage
}

fn create_runtime<C: Schedule>(engine: MockEngine, clock: C) -> TestRuntime<C> {
    Runtime::builder(engine, create_storage())
        .with_clock(clock)
        .build()
}

fn mock_engine(poll_fns: MockAnswers) -> MockEngine {
    let interface = {
        let mut builder = InterfaceBuilder::new(ArgsSpec::default());
        builder.insert_receiver("orders", ReceiverSpec::default());
        builder.insert_sender("events", SenderSpec::default());
        builder.build()
    };
    MockEngine::new(poll_fns).with_module(b"test", "Workflow", interface)
}

fn init_workflow(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let channels = ctx.data().persisted().channels();
    let orders_id = channels.channel_id("orders").unwrap();
    assert_ne!(orders_id, 0);

    let mut orders = ctx.data_mut().receiver(orders_id);
    let poll_result = orders.poll_next().into_inner(ctx)?;
    assert!(poll_result.is_pending());
    Ok(Poll::Pending)
}

fn complete_workflow(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let channels = ctx.data().persisted().channels();
    let orders_id = channels.channel_id("orders").unwrap();
    let events_id = channels.channel_id("events").unwrap();

    let mut orders = ctx.data_mut().receiver(orders_id);
    let poll_result = orders.poll_next().into_inner(ctx)?;
    assert_matches!(
        poll_result,
        Poll::Ready(Some(payload)) if payload == b"order #0"
    );

    let mut events = ctx.data_mut().sender(events_id);
    let poll_result = events.poll_ready().into_inner(ctx)?;
    assert!(poll_result.is_ready());
    let mut events = ctx.data_mut().sender(events_id);
    events.start_send(b"event #0".to_vec())?;

    Ok(Poll::Ready(()))
}

fn assert_str_payload(message: &proto::Message, expected: &str) {
    match &message.payload {
        Some(proto::message::Payload::Str(s)) => assert_eq!(s, expected),
        other => panic!("unexpected payload format: {other:?}"),
    }
}

fn assert_raw_payload(message: &proto::Message, expected: &[u8]) {
    match &message.payload {
        Some(proto::message::Payload::Raw(bytes)) => assert_eq!(bytes, expected),
        other => panic!("unexpected payload format: {other:?}"),
    }
}

#[tokio::test]
async fn channel_management() {
    let storage = create_storage();
    let service = StorageWrapper::new(storage);

    let request = proto::CreateChannelRequest {};
    let channel = service.create_channel(Request::new(request)).await;
    let channel = channel.unwrap().into_inner();

    assert_ne!(channel.id, 0);
    assert_eq!(channel.receiver_workflow_id, None);
    assert!(channel.sender_workflow_ids.is_empty());
    assert!(channel.has_external_sender);

    let request = proto::PushMessagesRequest {
        channel_id: channel.id,
        messages: vec![
            Pushed {
                payload: Some(pushed::Payload::Str("test".to_owned())),
            },
            Pushed {
                payload: Some(pushed::Payload::Str("other".to_owned())),
            },
        ],
    };
    service.push_messages(Request::new(request)).await.unwrap();

    let request = proto::GetMessageRequest {
        r#ref: Some(proto::MessageRef {
            channel_id: channel.id,
            index: 0,
        }),
        codec: proto::MessageCodec::Json.into(),
    };
    let message = service.get_message(Request::new(request)).await;
    let message = message.unwrap().into_inner();

    assert_str_payload(&message, "test");

    let request = proto::CloseChannelRequest {
        id: channel.id,
        half: proto::HandleType::Receiver as i32,
    };
    let channel = service.close_channel(Request::new(request)).await;
    let channel = channel.unwrap().into_inner();

    assert!(channel.is_closed);

    let request = proto::StreamMessagesRequest {
        channel_id: channel.id,
        start_index: 0,
        codec: proto::MessageCodec::Json.into(),
        follow: true,
    };
    let messages = service.stream_messages(Request::new(request)).await;
    let messages = messages.unwrap().into_inner();
    let messages: Vec<_> = messages.try_collect().await.unwrap();

    assert_eq!(messages.len(), 2);
    assert_str_payload(&messages[0], "test");
    assert_str_payload(&messages[1], "other");
}

#[tokio::test]
async fn server_basics() {
    let (poll_fns, mut poll_fns_sx) = MockAnswers::channel();
    let runtime = create_runtime(mock_engine(poll_fns), TokioScheduler);
    let runtime = RuntimeWrapper::new(runtime);
    let channels = runtime.storage_wrapper();

    test_module_deployment(&runtime).await;
    test_workflow_creation_errors(&runtime).await;

    let workflow = poll_fns_sx
        .send(init_workflow)
        .async_scope(test_workflow_creation(&runtime, true))
        .await;
    poll_fns_sx
        .send(complete_workflow)
        .async_scope(test_workflow_completion(&runtime, &channels, &workflow))
        .await;
}

async fn test_module_deployment(runtime: &impl RuntimeService) {
    let request = proto::DeployModuleRequest {
        id: "test".to_owned(),
        bytes: b"test".to_vec(),
        dry_run: false,
    };
    let module = runtime.deploy_module(Request::new(request)).await;
    let module = module.unwrap().into_inner();
    assert_eq!(module.id, "test");

    assert_eq!(module.definitions.len(), 1);
    let interface = module.definitions["Workflow"].interface.as_ref().unwrap();
    assert_eq!(
        interface.handles["orders"].r#type,
        proto::HandleType::Receiver as i32
    );
    assert_eq!(
        interface.handles["events"].r#type,
        proto::HandleType::Sender as i32
    );

    let modules = runtime.list_modules(Request::new(())).await;
    let modules: Vec<_> = modules.unwrap().into_inner().try_collect().await.unwrap();
    assert_eq!(modules.len(), 1);
    assert_eq!(modules[0].id, "test");
}

async fn test_workflow_creation_errors(runtime: &impl RuntimeService) {
    let request = proto::CreateWorkflowRequest {
        module_id: "bogus".to_owned(), // <<< invalid: module with this ID is not deployed
        name_in_module: "Workflow".to_owned(),
        args: Some(create_workflow_request::Args::RawArgs(vec![])),
        channels: HashMap::new(),
    };
    let err = runtime
        .create_workflow(Request::new(request))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    assert!(
        err.message().contains("definition `bogus::Workflow`"),
        "{}",
        err.message()
    );

    let request = proto::CreateWorkflowRequest {
        module_id: "test".to_owned(),
        name_in_module: "Bogus".to_owned(), // <<< invalid: no workflow with this name in module
        args: Some(create_workflow_request::Args::RawArgs(vec![])),
        channels: HashMap::new(),
    };
    let err = runtime
        .create_workflow(Request::new(request))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    assert!(
        err.message().contains("definition `test::Bogus`"),
        "{}",
        err.message()
    );

    let request = proto::CreateWorkflowRequest {
        module_id: "test".to_owned(),
        name_in_module: "Workflow".to_owned(),
        args: Some(create_workflow_request::Args::RawArgs(vec![])),
        channels: HashMap::new(), // <<< invalid: handles are missing
    };
    let err = runtime
        .create_workflow(Request::new(request))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("invalid shape"), "{}", err.message());
}

async fn test_workflow_creation(
    runtime: &impl RuntimeService,
    has_driver: bool,
) -> proto::Workflow {
    let orders_config = proto::ChannelConfig {
        r#type: proto::HandleType::Receiver as _,
        reference: Some(proto::channel_config::Reference::New(())),
    };
    let events_config = proto::ChannelConfig {
        r#type: proto::HandleType::Sender as _,
        reference: Some(proto::channel_config::Reference::New(())),
    };

    let request = proto::CreateWorkflowRequest {
        module_id: "test".to_owned(),
        name_in_module: "Workflow".to_owned(),
        args: Some(create_workflow_request::Args::StrArgs(String::new())),
        channels: HashMap::from_iter([
            ("orders".to_owned(), orders_config),
            ("events".to_owned(), events_config),
        ]),
    };
    let workflow = runtime.create_workflow(Request::new(request)).await;
    let workflow = workflow.unwrap().into_inner();

    assert_eq!(workflow.module_id, "test");
    assert_eq!(workflow.name_in_module, "Workflow");
    assert_eq!(workflow.parent_id, None);

    if has_driver {
        // Wait until the workflow is initialized.
        time::sleep(Duration::from_millis(100)).await;
    }

    let request = proto::GetWorkflowRequest { id: workflow.id };
    let workflow = runtime.get_workflow(Request::new(request)).await;
    workflow.unwrap().into_inner()
}

async fn test_workflow_completion(
    runtime: &impl RuntimeService,
    channels: &impl ChannelsService,
    workflow: &proto::Workflow,
) {
    const TIMEOUT: Duration = Duration::from_millis(20);

    assert!(workflow.execution_count > 0);
    let persisted = match workflow.state.as_ref().unwrap() {
        proto::workflow::State::Active(state) => state.persisted.as_ref().unwrap(),
        other => panic!("unexpected workflow state: {other:?}"),
    };
    assert_eq!(persisted.tasks.len(), 1);
    assert!(!persisted.tasks[&0].is_completed);

    let orders = &persisted.channels["orders"];
    let orders_state = orders.state.as_ref().unwrap();
    assert_matches!(orders_state, channel::State::Receiver(_));
    let orders_id = orders.id;
    let events_id = persisted.channels["events"].id;

    let request = proto::GetChannelRequest { id: events_id };
    let events_info = channels.get_channel(Request::new(request)).await;
    let events_info = events_info.unwrap().into_inner();
    assert!(events_info.receiver_workflow_id.is_none());
    assert_eq!(events_info.sender_workflow_ids, vec![workflow.id]);
    assert_eq!(events_info.received_messages, 0);

    let request = proto::StreamMessagesRequest {
        channel_id: events_id,
        start_index: 0,
        codec: proto::MessageCodec::Unspecified.into(),
        follow: true,
    };
    let events_rx = channels.stream_messages(Request::new(request)).await;
    let events_rx = events_rx.unwrap().into_inner();
    futures::pin_mut!(events_rx);

    assert!(time::timeout(TIMEOUT, events_rx.try_next()).await.is_err());

    let request = proto::PushMessagesRequest {
        channel_id: orders_id,
        messages: vec![Pushed {
            payload: Some(pushed::Payload::Raw(b"order #0".to_vec())),
        }],
    };
    channels.push_messages(Request::new(request)).await.unwrap();

    let event = events_rx.try_next().await.unwrap().unwrap();
    assert_eq!(
        event.reference,
        Some(proto::MessageRef {
            channel_id: events_id,
            index: 0,
        })
    );
    assert_raw_payload(&event, b"event #0");

    let request = proto::GetWorkflowRequest { id: workflow.id };
    let workflow = runtime.get_workflow(Request::new(request)).await;
    let workflow = workflow.unwrap().into_inner();
    assert_matches!(workflow.state, Some(proto::workflow::State::Completed(_)));
}

fn init_workflow_with_timer(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    let now = ctx.data().current_timestamp();
    let timer_id = ctx.data_mut().create_timer(TimerDefinition {
        expires_at: now + chrono::Duration::milliseconds(50),
    });
    assert_eq!(timer_id, 0);

    let poll_result = ctx.data_mut().timer(timer_id).poll().into_inner(ctx)?;
    assert!(poll_result.is_pending());
    Ok(Poll::Pending)
}

fn resolve_timer(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
    const TIMER_ID: TimerId = 0;

    let poll_result = ctx.data_mut().timer(TIMER_ID).poll().into_inner(ctx)?;
    assert!(poll_result.is_ready());
    Ok(Poll::Ready(()))
}

async fn test_workflow_with_mock_scheduler(has_driver: bool) {
    let (poll_fns, mut poll_fns_sx) = MockAnswers::channel();
    let runtime = create_runtime(mock_engine(poll_fns), MockScheduler::default());
    let service = if has_driver {
        RuntimeWrapper::new(runtime)
    } else {
        RuntimeWrapper::from(runtime)
    };

    test_module_deployment(&service).await;

    let workflow_id = poll_fns_sx
        .send(init_workflow_with_timer)
        .async_scope(async {
            let workflow_id = test_workflow_creation(&service, has_driver).await.id;
            if !has_driver {
                tick_workflow_and_expect_success(&service, workflow_id).await;
            }
            workflow_id
        })
        .await;

    let request = proto::TickWorkflowRequest {
        workflow_id: Some(workflow_id + 1), // <<< non-existing ID
    };
    let result = service.tick_workflow(Request::new(request)).await;
    let err = result.unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    let request = proto::TickWorkflowRequest {
        workflow_id: Some(workflow_id),
    };
    let result = service.tick_workflow(Request::new(request)).await;
    let result = result.unwrap().into_inner();
    assert_eq!(result.workflow_id, Some(workflow_id));
    let outcome = result.outcome.as_ref().unwrap();
    let next_timer = match outcome {
        proto::tick_result::Outcome::WouldBlock(would_block) => {
            would_block.nearest_timer.clone().unwrap()
        }
        other => panic!("unexpected outcome: {other:?}"),
    };

    let workflow = poll_fns_sx
        .send(resolve_timer)
        .async_scope(async {
            service.set_time(Request::new(next_timer)).await.unwrap();
            if has_driver {
                // Wait until the workflow is updated
                time::sleep(Duration::from_millis(100)).await;
            } else {
                tick_workflow_and_expect_success(&service, workflow_id).await;
            }

            let request = proto::GetWorkflowRequest { id: workflow_id };
            service.get_workflow(Request::new(request)).await
        })
        .await;
    let workflow = workflow.unwrap().into_inner();
    assert_matches!(workflow.state, Some(proto::workflow::State::Completed(_)));

    let req = proto::TickWorkflowRequest {
        workflow_id: Some(workflow_id),
    };
    let result = service.tick_workflow(Request::new(req)).await;
    let err = result.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("cannot tick workflow"),
        "{}",
        err.message()
    );

    let req = proto::TickWorkflowRequest { workflow_id: None };
    let result = service.tick_workflow(Request::new(req)).await;
    let result = result.unwrap().into_inner();

    match result.outcome {
        Some(proto::tick_result::Outcome::WouldBlock(would_block)) => {
            assert_eq!(would_block.nearest_timer, None);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

async fn tick_workflow_and_expect_success<S>(service: &S, workflow_id: WorkflowId)
where
    S: RuntimeService,
{
    let req = proto::TickWorkflowRequest {
        workflow_id: Some(workflow_id),
    };
    let result = service.tick_workflow(Request::new(req)).await;
    let result = result.unwrap().into_inner();

    assert_eq!(result.workflow_id, Some(workflow_id));
    assert_matches!(result.outcome, Some(proto::tick_result::Outcome::Ok(())));
}

#[tokio::test]
async fn workflow_with_mock_scheduler() {
    test_workflow_with_mock_scheduler(false).await;
}

#[tokio::test]
async fn workflow_with_mock_scheduler_and_driver() {
    test_workflow_with_mock_scheduler(true).await;
}

#[tokio::test]
async fn workers_basics() {
    let storage = create_storage();
    let storage = StorageWrapper::new(storage);

    let mut req = proto::GetOrCreateWorkerRequest {
        name: "tardigrade.v0.Timers".to_owned(),
        create_if_missing: false,
    };
    let worker = storage
        .get_or_create_worker(Request::new(req.clone()))
        .await;
    let err = worker.unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    req.create_if_missing = true;
    let worker = storage.get_or_create_worker(Request::new(req)).await;
    let worker = worker.unwrap().into_inner();
    assert_eq!(worker.name, "tardigrade.v0.Timers");
    assert_eq!(worker.cursor, 0);

    let inbound_channel_id = worker.inbound_channel_id;
    let req = proto::GetChannelRequest {
        id: inbound_channel_id,
    };
    let channel = storage.get_channel(Request::new(req)).await;
    let channel = channel.unwrap().into_inner();
    assert!(!channel.is_closed);

    let req = proto::UpdateWorkerRequest {
        worker_id: worker.id,
        update_type: Some(UpdateType::Cursor(2)),
    };
    storage.update_worker(Request::new(req)).await.unwrap();

    let req = proto::GetOrCreateWorkerRequest {
        name: "tardigrade.v0.Timers".to_owned(),
        create_if_missing: false,
    };
    let worker = storage.get_or_create_worker(Request::new(req)).await;
    let worker = worker.unwrap().into_inner();
    assert_eq!(worker.cursor, 2);
}
