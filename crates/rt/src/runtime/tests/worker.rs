//! Worker-related tests.

use async_std::task;
use async_trait::async_trait;
use futures::StreamExt;
use mimicry::AnswersSender;

use std::time::Duration;

use super::*;
use crate::storage::InProcessConnection;
use tardigrade::{channel, interface::Interface, Codec, Json};
use tardigrade_worker::{
    HandleRequest, Request, Worker, WorkerInterface, WorkerStorageConnection, WorkerStoragePool,
};

type StreamingStorage = Streaming<Arc<LocalStorage>>;

async fn create_test_runtime_with_worker<C: Clock>(
    poll_fns: MockAnswers,
    clock: C,
) -> LocalRuntime<C, StreamingStorage> {
    const WORKER_INTERFACE: &[u8] = br#"{
        "v": 0,
        "handles": {
            "baking/requests": {
                "sender": { "worker": "tardigrade.test.v0.Baking" }
            },
            "baking/responses": { "receiver": {} }
        }
    }"#;

    let worker_interface = Interface::from_bytes(WORKER_INTERFACE);
    let engine = MockEngine::new(poll_fns)
        .with_module(b"test", "TestWorkflow", test_interface())
        .with_module(b"worker", "WithWorker", worker_interface);

    let module_record = ModuleRecord {
        id: "worker@latest".to_owned(),
        bytes: Arc::new(*b"worker"),
        definitions: HashMap::new(),
        tracing_metadata: PersistedMetadata::default(),
    };
    let worker_module = engine.restore_module(&module_record).await.unwrap();

    let storage = Arc::new(LocalStorage::default());
    let (storage, routing_task) = Streaming::new(storage);
    task::spawn(routing_task);
    let runtime = Runtime::builder(engine, storage).with_clock(clock).build();
    runtime.insert_module("worker@latest", worker_module).await;
    runtime
}

#[async_std::test]
async fn resolving_sender_for_non_existing_worker() {
    let (poll_fns, _) = MockAnswers::channel();
    let runtime = create_test_runtime_with_worker(poll_fns, ()).await;
    let storage = runtime.storage();
    let spawner = runtime.spawner();

    let mut handles = HandleMap::new();
    handles.insert(
        "baking/requests".into(),
        Handle::Sender(storage.closed_sender()),
    );
    handles.insert(
        "baking/responses".into(),
        Handle::Receiver(storage.closed_receiver()),
    );

    let builder = spawner
        .new_workflow::<()>("worker@latest::WithWorker")
        .await
        .unwrap();
    let err = builder
        .build(b"test_input".to_vec(), handles)
        .await
        .unwrap_err();
    let err = format!("{err:#}");
    assert!(
        err.contains("worker `tardigrade.test.v0.Baking` not found"),
        "{err}"
    );
    assert!(err.contains("`baking/requests`"), "{err}");
    assert!(
        err.contains("definition `worker@latest::WithWorker`"),
        "{err}"
    );
}

#[async_std::test]
async fn resolving_sender_for_worker() {
    let (poll_fns, _) = MockAnswers::channel();
    let runtime = create_test_runtime_with_worker(poll_fns, ()).await;
    let storage = runtime.storage();
    let worker_channel_id = {
        let mut transaction = storage.as_ref().transaction().await;
        let worker = transaction
            .get_or_create_worker("tardigrade.test.v0.Baking")
            .await
            .unwrap();
        transaction.commit().await;
        worker.inbound_channel_id
    };

    let spawner = runtime.spawner();
    let mut handles = HandleMap::new();
    handles.insert(
        "baking/requests".into(),
        Handle::Sender(storage.closed_sender()),
    );
    handles.insert(
        "baking/responses".into(),
        Handle::Receiver(storage.closed_receiver()),
    );

    let builder = spawner
        .new_workflow::<()>("worker@latest::WithWorker")
        .await
        .unwrap();
    let workflow = builder
        .build(b"test_input".to_vec(), handles)
        .await
        .unwrap();
    let requests_id = channel_id(workflow.channel_ids(), "baking/requests");
    assert_eq!(requests_id, worker_channel_id);
}

async fn test_executing_worker(
    runtime: &LocalRuntime<(), StreamingStorage>,
    poll_fn_sx: &mut AnswersSender<MockPollFn>,
) {
    let spawner = runtime.spawner();
    let builder = spawner
        .new_workflow::<()>("worker@latest::WithWorker")
        .await
        .unwrap();
    let (child_handles, self_handles) = builder.handles(|_| {}).await;
    let self_handles = self_handles.with_indexing();
    let requests_info = self_handles[SenderAt("baking/requests")].channel_info();
    assert!(requests_info.is_closed);
    let workflow = builder
        .build(b"child_input".to_vec(), child_handles)
        .await
        .unwrap();

    let send_request: MockPollFn = |ctx| {
        let channels = ctx.data().persisted().channels();
        let requests_id = channels.channel_id("baking/requests").unwrap();
        assert_ne!(requests_id, 0);
        let responses_id = channels.channel_id("baking/responses").unwrap();
        assert_ne!(responses_id, 0);
        assert_ne!(responses_id, requests_id);

        let request = channel::Request::New {
            response_channel_id: responses_id,
            id: 0,
            data: "test".to_owned(),
        };
        let request = Json::encode_value(request);
        ctx.data_mut().sender(requests_id).start_send(request)?;
        let poll_result = ctx
            .data_mut()
            .sender(requests_id)
            .poll_flush()
            .into_inner(ctx)?;
        assert!(poll_result.is_pending());

        Ok(Poll::Pending)
    };

    let tick_result = poll_fn_sx
        .send(send_request)
        .async_scope(runtime.tick_workflow(workflow.id()))
        .await
        .unwrap();
    tick_result.into_inner().unwrap();

    let responses_id = self_handles[ReceiverAt("baking/responses")].channel_id();
    let responses = runtime.storage().receiver(responses_id).await.unwrap();
    let mut responses = responses.stream_messages(0..);
    let response = responses.next().await.unwrap();
    let response: channel::Response<String> = Json::decode_bytes(response.into_bytes());
    assert_eq!(response.id, 0);
    assert_eq!(response.data, "test");
}

#[derive(Debug)]
struct Handler;

impl WorkerInterface for Handler {
    type Request = String;
    type Response = String;
    type Codec = Json;
}

#[async_trait]
impl<P: WorkerStoragePool + 'static> HandleRequest<P> for Handler {
    async fn handle_request(&mut self, request: Request<Self>, connection: &mut P::Connection<'_>) {
        let (request, response_sx) = request.into_parts();
        response_sx.send(request, connection).await.ok();
    }
}

#[async_std::test]
async fn executing_worker() {
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let runtime = create_test_runtime_with_worker(poll_fns, ()).await;
    let storage = runtime.storage().as_ref().clone();
    let worker = Worker::new(Handler, InProcessConnection(storage));
    task::spawn(worker.listen("tardigrade.test.v0.Baking"));
    task::sleep(Duration::from_millis(50)).await; // wait for the handler to be registered

    test_executing_worker(&runtime, &mut poll_fn_sx).await;
}

#[async_std::test]
async fn restarting_worker() {
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let runtime = create_test_runtime_with_worker(poll_fns, ()).await;
    let storage = runtime.storage();

    for _ in 0..2 {
        let storage = storage.as_ref().clone();
        let worker = Worker::new(Handler, InProcessConnection(storage));
        let worker_handle = task::spawn(worker.listen("tardigrade.test.v0.Baking"));
        task::sleep(Duration::from_millis(50)).await;

        test_executing_worker(&runtime, &mut poll_fn_sx).await;
        worker_handle.cancel().await;
    }
}
