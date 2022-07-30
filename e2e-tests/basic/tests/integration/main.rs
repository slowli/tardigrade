//! E2E tests for sample workflows.

use externref::processor::Processor;
use once_cell::sync::Lazy;

use std::{collections::HashMap, error};

use tardigrade_rt::{
    test::{ModuleCompiler, WasmOpt},
    WorkflowEngine, WorkflowModule,
};

mod async_env;
mod sync_env;
mod tasks;

static MODULE: Lazy<WorkflowModule> = Lazy::new(|| {
    // Since this closure is called once, it is a good place to do other initialization
    enable_logs();

    let module_bytes = ModuleCompiler::new(env!("CARGO_PKG_NAME"))
        .set_current_dir(env!("CARGO_MANIFEST_DIR"))
        .set_profile("wasm")
        .set_wasm_opt(WasmOpt::default())
        .compile();
    let module_bytes = Processor::default()
        .set_drop_fn("tardigrade_rt", "drop_ref")
        .process_bytes(&module_bytes)
        .unwrap();
    let engine = WorkflowEngine::default();
    WorkflowModule::new(&engine, &module_bytes).unwrap()
});

type TestResult<T = ()> = Result<T, Box<dyn error::Error>>;

fn enable_logs() {
    env_logger::builder()
        .parse_filters("tardigrade_rt=trace,externref=debug")
        .is_test(true)
        .init();
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
    assert!(interfaces["PizzaDeliveryWithTasks"]
        .inbound_channel("baking_responses")
        .is_some());
    Ok(())
}
