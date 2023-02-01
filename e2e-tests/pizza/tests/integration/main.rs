//! E2E tests for sample workflows.

use async_std::task;
use once_cell::sync::Lazy;
use tracing::{subscriber::DefaultGuard, Level, Subscriber};
use tracing_capture::{CaptureLayer, SharedStorage};
use tracing_subscriber::{
    filter::Targets, layer::SubscriberExt, registry::LookupSpan, FmtSubscriber,
};

use std::{collections::HashMap, error, sync::Arc};

use tardigrade::{
    handle::ReceiverAt,
    spawn::CreateWorkflow,
    workflow::{GetInterface, WorkflowFn},
};
use tardigrade_rt::{
    engine::{Wasmtime, WasmtimeModule, WorkflowEngine},
    handle::{HostHandles, WorkflowHandle},
    runtime::Runtime,
    storage::{CommitStream, LocalStorage, Storage, Streaming},
    test::{ModuleCompiler, WasmOpt},
    Clock,
};

mod driver;
mod requests;
mod spawn;
mod sync_env;
mod tasks;

type TestResult<T = ()> = Result<T, Box<dyn error::Error>>;
type LocalManager<C, S = LocalStorage> = Runtime<Wasmtime, C, S>;
type StreamingStorage = Streaming<Arc<LocalStorage>>;
type StreamingManager<C> = LocalManager<C, StreamingStorage>;

static MODULE: Lazy<WasmtimeModule> = Lazy::new(|| {
    // Since this closure is called once, it is a good place to do other initialization
    tracing::subscriber::set_global_default(create_fmt_subscriber()).ok();

    let module_bytes = ModuleCompiler::new(env!("CARGO_PKG_NAME"))
        .set_current_dir(env!("CARGO_MANIFEST_DIR"))
        .set_profile("wasm")
        .set_wasm_opt(WasmOpt::default())
        .compile();
    let engine = Wasmtime::default();
    task::block_on(engine.create_module(module_bytes.into())).unwrap()
});

async fn create_module() -> WasmtimeModule {
    task::spawn_blocking(|| &*MODULE).await.clone()
}

async fn do_create_manager<C: Clock, S: Storage>(
    clock: C,
    storage: S,
) -> TestResult<LocalManager<C, S>> {
    let module = create_module().await;
    let manager = Runtime::builder(Wasmtime::default(), storage)
        .with_clock(clock)
        .build();
    manager.insert_module("test", module).await;
    Ok(manager)
}

async fn create_manager<C: Clock>(clock: C) -> TestResult<LocalManager<C>> {
    let mut storage = LocalStorage::default();
    storage.truncate_workflow_messages();
    do_create_manager(clock, storage).await
}

async fn create_streaming_manager<C: Clock>(
    clock: C,
) -> TestResult<(StreamingManager<C>, CommitStream)> {
    let mut storage = LocalStorage::default();
    storage.truncate_workflow_messages();
    let (mut storage, router_task) = Streaming::new(Arc::new(storage));
    task::spawn(router_task);
    let commits_rx = storage.stream_commits();

    let manager = do_create_manager(clock, storage).await?;
    Ok((manager, commits_rx))
}

type WorkflowAndHandles<'m, W, S> = (WorkflowHandle<W, &'m S>, HostHandles<'m, W, S>);

async fn spawn_workflow<'m, S, W>(
    manager: &'m LocalManager<impl Clock, S>,
    definition_id: &str,
    args: W::Args,
) -> TestResult<WorkflowAndHandles<'m, W, S>>
where
    S: Storage,
    W: WorkflowFn + GetInterface,
{
    let spawner = manager.spawner().close_senders();
    let builder = spawner.new_workflow(definition_id).await?;
    let (child_handles, self_handles) = builder.handles(|_| { /* use default config */ }).await;
    let workflow = builder.build(args, child_handles).await?;
    Ok((workflow, self_handles))
}

fn create_fmt_subscriber() -> impl Subscriber + for<'a> LookupSpan<'a> {
    const FILTER: &str = "tardigrade_pizza=debug,\
        tardigrade=debug,\
        tardigrade_rt=debug,\
        externref=debug";

    FmtSubscriber::builder()
        .pretty()
        .with_test_writer()
        .with_env_filter(FILTER)
        .finish()
}

fn enable_tracing_assertions() -> (DefaultGuard, SharedStorage) {
    let storage = SharedStorage::default();
    let filter = Targets::new().with_target("tardigrade_pizza", Level::INFO);
    let layer = CaptureLayer::new(&storage).with_filter(filter);
    let subscriber = create_fmt_subscriber().with(layer);
    let guard = tracing::subscriber::set_default(subscriber);
    (guard, storage)
}

#[test]
fn module_information_is_correct() -> TestResult {
    let interfaces: HashMap<_, _> = MODULE.interfaces().collect();
    interfaces["PizzaDelivery"].handle(ReceiverAt("orders"))?;
    assert!(interfaces["PizzaDelivery"]
        .handle(ReceiverAt("baking/responses"))
        .is_err());
    interfaces["PizzaDeliveryWithRequests"].handle(ReceiverAt("baking/responses"))?;
    Ok(())
}
