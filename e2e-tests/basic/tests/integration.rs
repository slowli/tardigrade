use chrono::Duration;
use tardigrade_rt::{Workflow, WorkflowHandle, WorkflowModule};
use wasmtime::Engine;

use tardigrade_test::{DomainEvent, Inputs, PizzaDelivery, PizzaKind, PizzaOrder};

fn compile_module(_optimize: bool) -> Vec<u8> {
    use std::{
        env, fs,
        path::PathBuf,
        process::{Command, Stdio},
    };

    const TARGET: &str = "wasm32-unknown-unknown";

    fn target_dir() -> PathBuf {
        let mut path = env::current_exe().expect("Cannot get path to executing test");
        path.pop();
        if path.ends_with("deps") {
            path.pop();
        }
        path
    }

    fn wasm_target_dir(target_dir: PathBuf) -> PathBuf {
        let mut root_dir = target_dir;
        while !root_dir.join(TARGET).is_dir() {
            if !root_dir.pop() {
                panic!("Cannot find dir for the `{}` target", TARGET);
            }
        }
        root_dir.join(TARGET).join("wasm")
    }

    let mut command = Command::new("cargo")
        .args(["build", "--lib", "--target", TARGET, "--profile=wasm"])
        .env("RUSTFLAGS", "-C link-arg=-zstack-size=32768")
        .stdin(Stdio::null())
        .spawn()
        .expect("cannot run cargo");
    let exit_status = command.wait().expect("failed waiting for cargo");
    if !exit_status.success() {
        panic!("Compiling WASM module finished abnormally: {}", exit_status);
    }

    let wasm_dir = wasm_target_dir(target_dir());
    let mut wasm_file = env!("CARGO_PKG_NAME").replace('-', "_");
    wasm_file.push_str(".wasm");
    let wasm_file = wasm_dir.join(wasm_file);

    /*if optimize {
        let command = Command::new("wasm-opt")
            .args(["-Os", "--enable-mutable-globals", "--strip-debug", "-o"])
            .args([&wasm_file, &wasm_file])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("cannot run wasm-opt");

        let output = command.wait_with_output().expect("failed waiting for wasm-opt");
        if !output.status.success() {
            panic!(
                "Optimizing WASM module finished abnormally: {}\nStderr: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }*/

    fs::read(wasm_dir.join(&wasm_file)).unwrap_or_else(|err| panic!("Cannot read WASM: {}", err))
}

#[test]
fn basic_workflow() {
    let module_bytes = compile_module(false);
    let engine = Engine::default();
    let module = WorkflowModule::<PizzaDelivery>::new(&engine, &module_bytes).unwrap();

    let inputs = Inputs {
        oven_count: 1,
        deliverer_count: 1,
    };
    let (mut workflow, receipt) = Workflow::new(&module, inputs.into()).unwrap();
    dbg!(&receipt);
    // FIXME: assert on receipt / workflow

    let WorkflowHandle {
        mut interface,
        mut timers,
        ..
    } = workflow.handle();

    let order = PizzaOrder {
        kind: PizzaKind::Pepperoni,
        delivery_distance: 10,
    };
    let receipt = interface.orders.send(order).unwrap();
    dbg!(&receipt);

    let (events, receipt) = interface.events.flush_messages();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], DomainEvent::OrderTaken { index: 1, order });
    dbg!(&receipt);

    dbg!("!!!");
    let new_time = timers.current_time() + Duration::milliseconds(100);
    let receipt = timers.set_current_time(new_time);
    dbg!(&receipt);

    dbg!("???");
    let (events, receipt) = interface.events.flush_messages();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], DomainEvent::Baked { index: 1, order });
    dbg!(&receipt);

    /*
    let mut handle = workflow.handle();
    let timers = handle.timers.timers();
    dbg!(&timers);
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[&0].name(), "baking");

    let timer_expiration = timers[&0].definition().expires_at;
    assert!(timer_expiration > handle.timers.current_time());
    handle.timers.set_current_time(timer_expiration);
    let receipt = workflow.tick();
    dbg!(&receipt);
     */
}
