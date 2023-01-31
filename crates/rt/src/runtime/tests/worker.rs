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

fn send_request(ctx: &mut MockInstance) -> anyhow::Result<Poll<()>> {
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
    let mut requests = ctx.data_mut().sender(requests_id);
    let poll_result = requests.poll_flush().into_inner(ctx)?;
    assert!(poll_result.is_pending());

    let mut responses = ctx.data_mut().receiver(responses_id);
    let poll_result = responses.poll_next().into_inner(ctx)?;
    assert!(poll_result.is_pending());

    Ok(Poll::Pending)
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
impl<P: WorkerStoragePool> HandleRequest<P> for Handler {
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
    task::sleep(Duration::from_millis(20)).await; // wait for the handler to be registered

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
        task::sleep(Duration::from_millis(20)).await;

        test_executing_worker(&runtime, &mut poll_fn_sx).await;
        worker_handle.cancel().await;
    }
}

#[derive(Debug)]
struct ConcurrentHandler<P> {
    connection_pool: Option<P>,
}

impl<P: WorkerStoragePool + Clone> ConcurrentHandler<P> {
    fn new() -> Self {
        Self {
            connection_pool: None,
        }
    }
}

impl<P: WorkerStoragePool + Clone> WorkerInterface for ConcurrentHandler<P> {
    type Request = String;
    type Response = String;
    type Codec = Json;
}

#[async_trait]
impl<P: WorkerStoragePool + Clone> HandleRequest<P> for ConcurrentHandler<P> {
    async fn initialize(&mut self, connection_pool: &P) {
        self.connection_pool = Some(connection_pool.clone());
    }

    async fn handle_request(&mut self, request: Request<Self>, _: &mut P::Connection<'_>) {
        let (request, response_sx) = request.into_parts();
        let connection_pool = self.connection_pool.clone().unwrap();
        task::spawn(async move {
            task::sleep(Duration::from_millis(10)).await;
            let mut connection = connection_pool.connect().await;
            response_sx.send(request, &mut connection).await.ok();
            connection.release().await;
        });
    }
}

#[async_std::test]
async fn executing_concurrent_worker() {
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let runtime = create_test_runtime_with_worker(poll_fns, ()).await;
    let storage = runtime.storage().as_ref().clone();
    let handler = ConcurrentHandler::new();
    let worker = Worker::new(handler, InProcessConnection(storage));
    task::spawn(worker.listen("tardigrade.test.v0.Baking"));
    task::sleep(Duration::from_millis(20)).await; // wait for the handler to be registered

    for _ in 0..3 {
        test_executing_worker(&runtime, &mut poll_fn_sx).await;
    }
}

#[derive(Debug)]
struct ClosingHandler {
    counter: usize,
    request_limit: usize,
}

impl ClosingHandler {
    fn new(request_limit: usize) -> Self {
        Self {
            counter: 0,
            request_limit,
        }
    }
}

impl WorkerInterface for ClosingHandler {
    type Request = String;
    type Response = String;
    type Codec = Json;
}

#[async_trait]
impl<P: WorkerStoragePool> HandleRequest<P> for ClosingHandler {
    async fn handle_request(&mut self, request: Request<Self>, connection: &mut P::Connection<'_>) {
        self.counter += 1;
        let (request, response_sx) = request.into_parts();
        if self.counter >= self.request_limit {
            response_sx.send_and_close(request, connection).await.ok();
        } else {
            response_sx.send(request, connection).await.ok();
        }
    }
}

#[async_std::test]
async fn worker_with_response_channel_closure() {
    let (poll_fns, mut poll_fn_sx) = MockAnswers::channel();
    let runtime = create_test_runtime_with_worker(poll_fns, ()).await;
    let storage = runtime.storage().as_ref().clone();
    let handler = ClosingHandler::new(1);
    let worker = Worker::new(handler, InProcessConnection(storage));
    task::spawn(worker.listen("tardigrade.test.v0.Baking"));
    task::sleep(Duration::from_millis(20)).await; // wait for the handler to be registered

    test_closing_worker(&runtime, &mut poll_fn_sx).await;
}

async fn test_closing_worker(
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
    let workflow = builder
        .build(b"child_input".to_vec(), child_handles)
        .await
        .unwrap();

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
    assert!(responses.next().await.is_none());
    assert!(responses.is_closed());

    // Sending new requests is possible; they won't be answered though.
    let send_second_request: MockPollFn = |ctx| {
        let channels = ctx.data().persisted().channels();
        let responses_id = channels.channel_id("baking/responses").unwrap();
        let mut responses = ctx.data_mut().receiver(responses_id);
        let poll_result = responses.poll_next().into_inner(ctx)?;
        assert_matches!(poll_result, Poll::Ready(Some(_)));

        send_request(ctx)
    };
    let tick_result = poll_fn_sx
        .send(send_second_request)
        .async_scope(runtime.tick_workflow(workflow.id()))
        .await
        .unwrap();
    tick_result.into_inner().unwrap();
}
