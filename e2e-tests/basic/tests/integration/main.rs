//! E2E tests for sample workflows.

use async_std::task;
use once_cell::sync::Lazy;
use tracing::{subscriber::DefaultGuard, Level, Subscriber};
use tracing_capture::{CaptureLayer, SharedStorage};
use tracing_subscriber::{
    filter::Targets, layer::SubscriberExt, registry::LookupSpan, FmtSubscriber,
};

use std::{collections::HashMap, error};

use tardigrade_rt::{
    test::{ModuleCompiler, WasmOpt},
    WorkflowEngine, WorkflowModule,
};

mod async_env;
mod requests;
mod spawn;
mod sync_env;
mod tasks;

static MODULE_BYTES: Lazy<Vec<u8>> = Lazy::new(|| {
    // Since this closure is called once, it is a good place to do other initialization
    tracing::subscriber::set_global_default(create_fmt_subscriber()).ok();

    ModuleCompiler::new(env!("CARGO_PKG_NAME"))
        .set_current_dir(env!("CARGO_MANIFEST_DIR"))
        .set_profile("wasm")
        .set_wasm_opt(WasmOpt::default())
        .compile()
});
static MODULE: Lazy<WorkflowModule<'static>> = Lazy::new(|| {
    let engine = WorkflowEngine::default();
    WorkflowModule::new(&engine, &MODULE_BYTES).unwrap()
});

async fn create_module() -> WorkflowModule<'static> {
    task::spawn_blocking(|| &*MODULE).await.clone()
}

type TestResult<T = ()> = Result<T, Box<dyn error::Error>>;

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
    assert!(interfaces["PizzaDelivery"]
        .inbound_channel("orders")
        .is_some());
    assert!(interfaces["PizzaDelivery"]
        .inbound_channel("baking_responses")
        .is_none());
    assert!(interfaces["PizzaDeliveryWithRequests"]
        .inbound_channel("baking_responses")
        .is_some());
    Ok(())
}
