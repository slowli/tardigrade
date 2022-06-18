//! Helpers for workflow integration testing.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Mutex, PoisonError},
};

#[derive(Debug)]
pub struct WasmOpt {
    wasm_opt_command: String,
    args: Vec<String>,
}

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
        if !exit_status.success() {
            panic!(
                "Optimizing WASM module finished abnormally: {}",
                exit_status
            );
        }
        opt_wasm_file
    }
}

#[derive(Debug)]
pub struct ModuleCompiler {
    package_name: &'static str,
    profile: &'static str,
    stack_size: Option<usize>,
    wasm_opt: Option<WasmOpt>,
    cache: Mutex<Option<PathBuf>>,
}

impl ModuleCompiler {
    const TARGET: &'static str = "wasm32-unknown-unknown";

    pub fn new(package_name: &'static str) -> Self {
        Self {
            package_name,
            profile: "release",
            stack_size: None,
            wasm_opt: None,
            cache: Mutex::new(None),
        }
    }

    fn drop_cache(&mut self) {
        *self.cache.get_mut().unwrap_or_else(PoisonError::into_inner) = None;
    }

    pub fn set_profile(&mut self, profile: &'static str) -> &mut Self {
        self.drop_cache();
        self.profile = profile;
        self
    }

    pub fn set_stack_size(&mut self, stack_size: usize) -> &mut Self {
        self.drop_cache();
        self.stack_size = Some(stack_size);
        self
    }

    pub fn set_wasm_opt(&mut self, options: WasmOpt) -> &mut Self {
        self.drop_cache();
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
            if !root_dir.pop() {
                panic!("Cannot find dir for the `{}` target", Self::TARGET);
            }
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
        command.args(["build", "--lib", "--target", Self::TARGET, &profile]);
        if let Some(stack_size) = self.stack_size {
            // TODO: combine with existing flags?
            let rust_flags = format!("-C link-arg=-zstack-size={}", stack_size);
            command.env("RUSTFLAGS", &rust_flags);
        }

        let mut command = command
            .stdin(Stdio::null())
            .spawn()
            .expect("cannot run cargo");
        let exit_status = command.wait().expect("failed waiting for cargo");
        if !exit_status.success() {
            panic!("Compiling WASM module finished abnormally: {}", exit_status);
        }
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

    pub fn compile(&self) -> Vec<u8> {
        let wasm_file = {
            let mut guard = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            guard.get_or_insert_with(|| self.do_compile()).clone()
        };
        fs::read(&wasm_file).unwrap_or_else(|err| {
            panic!(
                "Error reading file `{}`: {}",
                wasm_file.to_string_lossy(),
                err
            )
        })
    }
}
