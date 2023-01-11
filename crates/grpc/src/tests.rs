//! Tests for `ManagerWrapper`.

use assert_matches::assert_matches;
use futures::TryStreamExt;
use tokio::{task, time};
use tonic::{Code, Request};

use std::{collections::HashMap, sync::Arc, task::Poll, time::Duration};

use crate::{
    proto::{
        self, persisted_workflow::channel, tardigrade_channels_server::TardigradeChannels,
        tardigrade_server::Tardigrade,
    },
    ManagerWrapper,
};
use tardigrade::interface::{ArgsSpec, InterfaceBuilder, ReceiverSpec, SenderSpec};
use tardigrade_rt::{
    engine::AsWorkflowData,
    manager::WorkflowManager,
    storage::{LocalStorage, Streaming},
    test::engine::{MockAnswers, MockEngine, MockInstance},
    TokioScheduler,
};

async fn create_service(engine: MockEngine) -> impl Tardigrade + TardigradeChannels {
    let storage = Arc::new(LocalStorage::default());
    let (storage, routing_task) = Streaming::new(storage);
    task::spawn(routing_task);

    let manager = WorkflowManager::builder(engine, storage)
        .with_clock(TokioScheduler)
        .build();

    ManagerWrapper::new(manager)
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

#[tokio::test]
async fn channel_management() {
    let (poll_fns, _) = MockAnswers::channel();
    let service = create_service(mock_engine(poll_fns)).await;

    let request = proto::CreateChannelRequest {};
    let channel = service.create_channel(Request::new(request)).await;
    let channel = channel.unwrap().into_inner();

    assert_ne!(channel.id, 0);
    assert_eq!(channel.receiver_workflow_id, None);
    assert!(channel.sender_workflow_ids.is_empty());
    assert!(channel.has_external_sender);

    let request = proto::PushMessagesRequest {
        channel_id: channel.id,
        payloads: vec![b"test".to_vec(), b"other".to_vec()],
    };
    service.push_messages(Request::new(request)).await.unwrap();

    let request = proto::MessageRef {
        channel_id: channel.id,
        index: 0,
    };
    let message = service.get_message(Request::new(request)).await;
    let message = message.unwrap().into_inner();

    assert_eq!(message.payload, b"test");

    let request = proto::CloseChannelRequest {
        id: channel.id,
        half: proto::HandleType::Receiver as i32,
    };
    let channel = service.close_channel(Request::new(request)).await;
    let channel = channel.unwrap().into_inner();

    assert!(channel.is_closed);

    let request = proto::StreamMessagesRequest {
        id: channel.id,
        start_index: 0,
    };
    let messages = service.stream_messages(Request::new(request)).await;
    let messages = messages.unwrap().into_inner();
    let messages: Vec<_> = messages.try_collect().await.unwrap();

    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].payload, b"test");
    assert_eq!(messages[1].payload, b"other");
}

#[tokio::test]
async fn server_basics() {
    let (poll_fns, mut poll_fns_sx) = MockAnswers::channel();
    let service = create_service(mock_engine(poll_fns)).await;

    test_module_deployment(&service).await;
    test_workflow_creation_errors(&service).await;

    let workflow = poll_fns_sx
        .send(init_workflow)
        .async_scope(test_workflow_creation(&service))
        .await;
    poll_fns_sx
        .send(complete_workflow)
        .async_scope(test_workflow_completion(&service, &workflow))
        .await;
}

async fn test_module_deployment<S: Tardigrade + TardigradeChannels>(service: &S) {
    let request = proto::DeployModuleRequest {
        id: "test".to_owned(),
        bytes: b"test".to_vec(),
        dry_run: false,
    };
    let module = service.deploy_module(Request::new(request)).await;
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

    let modules = service.list_modules(Request::new(())).await;
    let modules: Vec<_> = modules.unwrap().into_inner().try_collect().await.unwrap();
    assert_eq!(modules.len(), 1);
    assert_eq!(modules[0].id, "test");
}

async fn test_workflow_creation_errors<S: Tardigrade + TardigradeChannels>(service: &S) {
    let request = proto::CreateWorkflowRequest {
        module_id: "bogus".to_owned(), // <<< invalid: module with this ID is not deployed
        name_in_module: "Workflow".to_owned(),
        args: vec![],
        channels: HashMap::new(),
    };
    let err = service
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
        args: vec![],
        channels: HashMap::new(),
    };
    let err = service
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
        args: vec![],
        channels: HashMap::new(), // <<< invalid: handles are missing
    };
    let err = service
        .create_workflow(Request::new(request))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("invalid shape"), "{}", err.message());
}

async fn test_workflow_creation<S: Tardigrade + TardigradeChannels>(
    service: &S,
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
        args: vec![],
        channels: HashMap::from_iter([
            ("orders".to_owned(), orders_config),
            ("events".to_owned(), events_config),
        ]),
    };
    let workflow = service.create_workflow(Request::new(request)).await;
    let workflow = workflow.unwrap().into_inner();

    assert_eq!(workflow.module_id, "test");
    assert_eq!(workflow.name_in_module, "Workflow");
    assert_eq!(workflow.parent_id, None);

    // Wait until the workflow is initialized.
    time::sleep(Duration::from_millis(100)).await;

    let request = proto::GetWorkflowRequest { id: workflow.id };
    let workflow = service.get_workflow(Request::new(request)).await;
    workflow.unwrap().into_inner()
}

async fn test_workflow_completion<S: Tardigrade + TardigradeChannels>(
    service: &S,
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
    let events_info = service.get_channel(Request::new(request)).await;
    let events_info = events_info.unwrap().into_inner();
    assert!(events_info.receiver_workflow_id.is_none());
    assert_eq!(events_info.sender_workflow_ids, vec![workflow.id]);
    assert_eq!(events_info.received_messages, 0);

    let request = proto::StreamMessagesRequest {
        id: events_id,
        start_index: 0,
    };
    let events_rx = service.stream_messages(Request::new(request)).await;
    let events_rx = events_rx.unwrap().into_inner();
    futures::pin_mut!(events_rx);

    assert!(time::timeout(TIMEOUT, events_rx.try_next()).await.is_err());

    let request = proto::PushMessagesRequest {
        channel_id: orders_id,
        payloads: vec![b"order #0".to_vec()],
    };
    service.push_messages(Request::new(request)).await.unwrap();

    let event = events_rx.try_next().await.unwrap().unwrap();
    assert_eq!(
        event.reference,
        Some(proto::MessageRef {
            channel_id: events_id,
            index: 0,
        })
    );
    assert_eq!(event.payload, b"event #0");

    let request = proto::GetWorkflowRequest { id: workflow.id };
    let workflow = service.get_workflow(Request::new(request)).await;
    let workflow = workflow.unwrap().into_inner();
    assert_matches!(workflow.state, Some(proto::workflow::State::Completed(_)));
}
