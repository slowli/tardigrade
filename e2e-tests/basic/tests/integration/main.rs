//! E2E tests for sample workflows.

use async_std::task;
use once_cell::sync::Lazy;
use tracing::{subscriber::DefaultGuard, Level, Subscriber};
use tracing_capture::{CaptureLayer, SharedStorage};
use tracing_subscriber::{
    filter::Targets, layer::SubscriberExt, registry::LookupSpan, FmtSubscriber,
};

use std::{collections::HashMap, error};

use tardigrade::{
    handle::ReceiverAt,
    spawn::ManageWorkflows,
    workflow::{GetInterface, WorkflowFn},
};
use tardigrade_rt::{
    engine::{Wasmtime, WasmtimeModule},
    manager::{ManagerHandles, WorkflowHandle, WorkflowManager},
    storage::LocalStorage,
    test::{ModuleCompiler, WasmOpt},
    Clock,
};

mod driver;
mod requests;
mod spawn;
mod sync_env;
mod tasks;

type TestResult<T = ()> = Result<T, Box<dyn error::Error>>;
type LocalManager<C> = WorkflowManager<Wasmtime, C, LocalStorage>;

static MODULE: Lazy<WasmtimeModule> = Lazy::new(|| {
    // Since this closure is called once, it is a good place to do other initialization
    tracing::subscriber::set_global_default(create_fmt_subscriber()).ok();

    let module_bytes = ModuleCompiler::new(env!("CARGO_PKG_NAME"))
        .set_current_dir(env!("CARGO_MANIFEST_DIR"))
        .set_profile("wasm")
        .set_wasm_opt(WasmOpt::default())
        .compile();
    let engine = Wasmtime::default();
    engine.create_module(module_bytes).unwrap()
});

async fn create_module() -> WasmtimeModule {
    task::spawn_blocking(|| &*MODULE).await.clone()
}

async fn create_manager<C: Clock>(clock: C) -> TestResult<LocalManager<C>> {
    let module = create_module().await;
    let mut storage = LocalStorage::default();
    storage.truncate_workflow_messages();
    let mut manager = WorkflowManager::builder(Wasmtime::default(), storage)
        .with_clock(clock)
        .build()
        .await?;
    manager.insert_module("test", module).await;
    Ok(manager)
}

type WorkflowAndHandles<'m, C, W> = (
    WorkflowHandle<'m, W, LocalManager<C>>,
    ManagerHandles<'m, W, LocalManager<C>>,
);

async fn spawn_workflow<'m, C, W>(
    manager: &'m LocalManager<C>,
    definition_id: &str,
    args: W::Args,
) -> TestResult<WorkflowAndHandles<'m, C, W>>
where
    C: Clock,
    W: WorkflowFn + GetInterface,
{
    let manager = manager.as_ref();
    let builder = manager.new_workflow(definition_id)?;
    let (child_handles, self_handles) = builder.handles(|_| { /* use default config */ }).await;
    let workflow = builder.build(args, child_handles).await?;
    Ok((workflow, self_handles))
}

fn create_fmt_subscriber() -> impl Subscriber + for<'a> LookupSpan<'a> {
    const FILTER: &str = "tardigrade_test_basic=debug,\
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
    let filter = Targets::new().with_target("tardigrade_test_basic", Level::INFO);
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
        .handle(ReceiverAt("baking::responses"))
        .is_err());
    interfaces["PizzaDeliveryWithRequests"].handle(ReceiverAt("baking::responses"))?;
    Ok(())
}
