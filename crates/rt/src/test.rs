//! Helpers for workflow integration testing.
//!
//! # Examples
//!
//! Typically, it is useful to cache a [`WorkflowModule`](crate::WorkflowModule)
//! among multiple tests. This can be performed as follows:
//!
//! ```no_run
//! use once_cell::sync::Lazy;
//! use tardigrade_rt::{test::*, WasmtimeEngine, WorkflowModule};
//!
//! static MODULE: Lazy<WorkflowModule> = Lazy::new(|| {
//!     let module_bytes = ModuleCompiler::new(env!("CARGO_PKG_NAME"))
//!         .set_current_dir(env!("CARGO_MANIFEST_DIR"))
//!         .set_profile("wasm")
//!         .set_wasm_opt(WasmOpt::default())
//!         .compile();
//!     let engine = WasmtimeEngine::default();
//!     WorkflowModule::new(&engine, module_bytes).unwrap()
//! });
//! // The module can then be used in tests
//! ```

use externref::processor::Processor;

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

pub use crate::backends::MockScheduler;

/// Options for the `wasm-opt` optimizer.
///
/// Note that `wasm-opt` before and including version 109 [mangles table exports][export-bug],
/// which most probably will result in the module validation error ("`externrefs` export is not
/// a table of `externref`s"). The first release of `wasm-opt` to include the fix is [version 110].
///
/// [export-bug]: https://github.com/WebAssembly/binaryen/pull/4736
/// [version 110]: https://github.com/WebAssembly/binaryen/releases/tag/version_110
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
                "--enable-reference-types".to_string(),
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

    fn process_refs(wasm_file: &Path) -> PathBuf {
        let module_bytes = fs::read(wasm_file).unwrap_or_else(|err| {
            panic!(
                "Error reading file `{}`: {err}",
                wasm_file.to_string_lossy()
            )
        });
        let processed_bytes = Processor::default()
            .set_drop_fn("tardigrade_rt", "drop_ref")
            .process_bytes(&module_bytes)
            .unwrap();

        let mut ref_wasm_file = PathBuf::from(wasm_file);
        ref_wasm_file.set_extension("ref.wasm");
        fs::write(&ref_wasm_file, &processed_bytes).unwrap_or_else(|err| {
            panic!(
                "Error writing externref-processed module to file `{}`: {err}",
                wasm_file.to_string_lossy()
            )
        });
        ref_wasm_file
    }

    fn do_compile(&self) -> PathBuf {
        let wasm_file = self.compile_wasm();
        let wasm_file = Self::process_refs(&wasm_file);
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
                "Error reading file `{}`: {err}",
                wasm_file.to_string_lossy()
            )
        })
    }
}
