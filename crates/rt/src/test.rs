//! Helpers for workflow integration testing.
//!
//! # Examples
//!
//! Typically, it is useful to cache a [`WorkflowModule`](crate::WorkflowModule)
//! among multiple tests. This can be performed as follows:
//!
//! ```no_run
//! use once_cell::sync::Lazy;
//! use tardigrade_rt::{test::*, WorkflowEngine, WorkflowModule};
//!
//! static MODULE: Lazy<WorkflowModule> = Lazy::new(|| {
//!     let module_bytes = ModuleCompiler::new(env!("CARGO_PKG_NAME"))
//!         .set_current_dir(env!("CARGO_MANIFEST_DIR"))
//!         .set_profile("wasm")
//!         .set_wasm_opt(WasmOpt::default())
//!         .compile();
//!     let engine = WorkflowEngine::default();
//!     WorkflowModule::new(&engine, &module_bytes).unwrap()
//! });
//!
//! // The module can then be used in tests:
//! let spawner = MODULE.for_untyped_workflow("TestWorkflow").unwrap();
//! // Use `spawner` to spawn workflows...
//! ```

use chrono::{DateTime, Utc};
#[cfg(feature = "async")]
use futures::{channel::mpsc, Stream};

use std::{
    env, fs, ops,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Mutex,
};

#[cfg(feature = "async")]
use crate::manager::future::{Schedule, TimerFuture};
use crate::module::Clock;
use tardigrade::test::MockScheduler as SchedulerBase;

/// Options for the `wasm-opt` optimizer.
#[derive(Debug)]
pub struct WasmOpt {
    wasm_opt_command: String,
    args: Vec<String>,
}

/// Provides reasonable defaults: `-Os` optimization level, `--enable-mutable-globals`
/// (mutable globals are used by Rust, e.g. for the shadow stack pointer), and `--strip-debug`.
impl Default for WasmOpt {
    fn default() -> Self {
        Self {
            wasm_opt_command: "wasm-opt".to_string(),
            args: vec![
                "-Os".to_string(),
                "--enable-mutable-globals".to_string(),
                "--strip-debug".to_string(),
            ],
        }
    }
}

impl WasmOpt {
    /// Returns `PathBuf` to the optimized WASM module.
    fn optimize_wasm(&self, wasm_file: &Path) -> PathBuf {
        let mut opt_wasm_file = PathBuf::from(wasm_file);
        opt_wasm_file.set_extension("opt.wasm");

        let mut command = Command::new(&self.wasm_opt_command)
            .args(&self.args)
            .arg("-o")
            .args([opt_wasm_file.as_ref(), wasm_file])
            .stdin(Stdio::null())
            .spawn()
            .expect("cannot run wasm-opt");

        let exit_status = command.wait().expect("failed waiting for wasm-opt");
        assert!(
            exit_status.success(),
            "Optimizing WASM module finished abnormally: {}",
            exit_status
        );
        opt_wasm_file
    }
}

/// Compiler for WASM modules.
#[derive(Debug)]
pub struct ModuleCompiler {
    package_name: &'static str,
    profile: &'static str,
    current_dir: Option<PathBuf>,
    wasm_opt: Option<WasmOpt>,
}

impl ModuleCompiler {
    const TARGET: &'static str = "wasm32-unknown-unknown";

    /// Creates a compiler for the specified package. Usually, this should be
    /// `env!("CARGO_PKG_NAME")`.
    pub fn new(package_name: &'static str) -> Self {
        Self {
            package_name,
            profile: "release",
            current_dir: None,
            wasm_opt: None,
        }
    }

    /// Sets the current directory for executing commands. This may be important for `cargo`
    /// if some configuration options (e.g., linker options such as the stack size)
    /// are configured via the `.cargo` dir in the package dir.
    pub fn set_current_dir(&mut self, dir: impl AsRef<Path>) -> &mut Self {
        self.current_dir = Some(dir.as_ref().to_owned());
        self
    }

    /// Sets the build profile. By default, profile is set to `release`, but it may make sense
    /// to create a separate profile for WASM compilation, e.g., to optimize for size and
    /// to enable link-time optimization.
    pub fn set_profile(&mut self, profile: &'static str) -> &mut Self {
        self.profile = profile;
        self
    }

    /// Sets WASM optimization options.
    pub fn set_wasm_opt(&mut self, options: WasmOpt) -> &mut Self {
        self.wasm_opt = Some(options);
        self
    }

    fn target_dir() -> PathBuf {
        let mut path = env::current_exe().expect("Cannot get path to executing test");
        path.pop();
        if path.ends_with("deps") {
            path.pop();
        }
        path
    }

    fn wasm_target_dir(&self, target_dir: PathBuf) -> PathBuf {
        let mut root_dir = target_dir;
        while !root_dir.join(Self::TARGET).is_dir() {
            assert!(
                root_dir.pop(),
                "Cannot find dir for the `{}` target",
                Self::TARGET
            );
        }
        root_dir.join(Self::TARGET).join(self.profile)
    }

    fn wasm_file(&self) -> PathBuf {
        let wasm_dir = self.wasm_target_dir(Self::target_dir());
        let mut wasm_file = self.package_name.replace('-', "_");
        wasm_file.push_str(".wasm");
        wasm_dir.join(wasm_file)
    }

    fn compile_wasm(&self) -> PathBuf {
        let profile = format!("--profile={}", self.profile);
        let mut command = Command::new("cargo");
        if let Some(dir) = &self.current_dir {
            command.current_dir(dir);
        }
        command.args(["build", "--lib", "--target", Self::TARGET, &profile]);

        let mut command = command
            .stdin(Stdio::null())
            .spawn()
            .expect("cannot run cargo");
        let exit_status = command.wait().expect("failed waiting for cargo");
        assert!(
            exit_status.success(),
            "Compiling WASM module finished abnormally: {}",
            exit_status
        );
        self.wasm_file()
    }

    fn do_compile(&self) -> PathBuf {
        let wasm_file = self.compile_wasm();
        if let Some(wasm_opt) = &self.wasm_opt {
            wasm_opt.optimize_wasm(&wasm_file)
        } else {
            wasm_file
        }
    }

    /// Compiles the WASM module and returns its bytes.
    ///
    /// # Panics
    ///
    /// Panics if any error occurs during compilation or optimization. In this case, the output
    /// of `cargo build` / `wasm-opt` will be available to determine the error cause.
    pub fn compile(&self) -> Vec<u8> {
        let wasm_file = self.do_compile();
        fs::read(&wasm_file).unwrap_or_else(|err| {
            panic!(
                "Error reading file `{}`: {}",
                wasm_file.to_string_lossy(),
                err
            )
        })
    }
}

/// Mock [wall clock](Clock) and [scheduler](Schedule).
///
/// # Examples
///
/// A primary use case is to use the scheduler with [`AsyncEnv`] for integration testing:
///
/// [`AsyncEnv`]: crate::manager::future::AsyncEnv
///
/// ```
/// # use async_std::task;
/// # use futures::TryStreamExt;
/// # use std::sync::Arc;
/// use tardigrade::{interface::OutboundChannel, spawn::ManageWorkflowsExt};
/// use tardigrade_rt::{test::MockScheduler, WorkflowModule};
/// use tardigrade_rt::manager::{future::AsyncEnv, WorkflowHandle, WorkflowManager};
///
/// # async fn test_wrapper(module: WorkflowModule) -> anyhow::Result<()> {
/// let scheduler = Arc::new(MockScheduler::default());
/// // Set the mocked wall clock for the workflow manager.
/// let mut manager = WorkflowManager::builder()
///     .with_clock(Arc::clone(&scheduler))
///     // set other options (e.g., spawners)
///     .build();
/// let inputs: Vec<u8> = // ...
/// #   vec![];
/// let mut workflow =
///     manager.new_workflow::<()>("test", inputs)?.build()?;
///
/// // Spin up the environment to execute the `workflow`.
/// let mut env = AsyncEnv::new(scheduler.clone());
/// let mut handle = workflow.handle();
/// let mut events_rx = handle.remove(OutboundChannel("events"))
///     .unwrap()
///     .into_async(&mut env);
/// task::spawn(async move { env.run(&mut manager).await });
///
/// // Advance mocked wall clock.
/// let now = scheduler.now();
/// scheduler.set_now(now + chrono::Duration::seconds(1));
/// // This can lead to the workflow progressing, e.g., by emitting messages
/// let message: Option<Vec<u8>> = events_rx.try_next().await?;
/// // Assert on `message`...
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct MockScheduler {
    inner: Mutex<SchedulerBase>,
    #[cfg(feature = "async")]
    new_expirations_sx: mpsc::UnboundedSender<DateTime<Utc>>,
}

#[allow(clippy::derivable_impls)] // triggered if building without `async` feature on
impl Default for MockScheduler {
    fn default() -> Self {
        Self {
            inner: Mutex::default(),
            #[cfg(feature = "async")]
            new_expirations_sx: mpsc::unbounded().0,
        }
    }
}

impl MockScheduler {
    /// Creates a mock scheduler together with a stream that notifies the consumer
    /// about new timer expirations.
    #[cfg_attr(docsrs, doc(cfg(feature = "async")))]
    #[cfg(feature = "async")]
    pub fn with_expirations() -> (Self, impl Stream<Item = DateTime<Utc>> + Unpin) {
        let (new_expirations_sx, rx) = mpsc::unbounded();
        let this = Self {
            inner: Mutex::default(),
            new_expirations_sx,
        };
        (this, rx)
    }

    fn inner(&self) -> impl ops::DerefMut<Target = SchedulerBase> + '_ {
        self.inner.lock().unwrap()
    }

    /// Returns the expiration for the nearest timer, or `None` if there are no active timers.
    pub fn next_timer_expiration(&self) -> Option<DateTime<Utc>> {
        self.inner().next_timer_expiration()
    }

    /// Returns the current timestamp.
    pub fn now(&self) -> DateTime<Utc> {
        self.inner().now()
    }

    /// Sets the current timestamp for the scheduler.
    pub fn set_now(&self, now: DateTime<Utc>) {
        self.inner().set_now(now);
    }
}

impl Clock for MockScheduler {
    fn now(&self) -> DateTime<Utc> {
        self.now()
    }
}

#[cfg(feature = "async")]
impl Schedule for MockScheduler {
    fn create_timer(&self, expires_at: DateTime<Utc>) -> TimerFuture {
        use futures::{future, FutureExt};

        let mut guard = self.inner();
        let now = guard.now();
        if now >= expires_at {
            Box::pin(future::ready(now))
        } else {
            self.new_expirations_sx.unbounded_send(expires_at).ok();
            Box::pin(guard.insert_timer(expires_at).then(|res| match res {
                Ok(timestamp) => future::ready(timestamp).left_future(),
                Err(_) => future::pending().right_future(),
                // ^ An error can occur when the mock scheduler is dropped, usually at the end
                // of a test. In this case the timer never expires.
            }))
        }
    }
}
