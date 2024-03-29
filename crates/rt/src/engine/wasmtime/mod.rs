//! Workflow execution engine based on `wasmtime`.

use anyhow::Context;
use async_trait::async_trait;
use wasmtime::{AsContext, AsContextMut, Engine, Memory, StoreContextMut};

use std::{fmt, sync::Arc};

mod api;
mod functions;
mod instance;
mod module;

pub use self::{
    instance::WasmtimeInstance,
    module::{WasmtimeDefinition, WasmtimeModule},
};

use self::instance::InstanceData;
use super::WorkflowEngine;
use tardigrade::abi::AllocateBytes;

/// [`WorkflowEngine`] based on the `wasmtime` crate.
#[derive(Default)]
pub struct Wasmtime(Engine);

impl fmt::Debug for Wasmtime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("Wasmtime").finish()
    }
}

#[async_trait]
impl WorkflowEngine for Wasmtime {
    type Instance = WasmtimeInstance;
    type Definition = WasmtimeDefinition;
    type Module = WasmtimeModule;

    /// Creates a module from the specified WASM module bytes.
    ///
    /// # Errors
    ///
    /// Returns an error in any of the following cases:
    ///
    /// - `bytes` is not a valid WASM module.
    /// - The module has bogus imports from the `tardigrade_rt` module, such as an unknown function
    ///   or a known function with an incorrect signature.
    /// - The module does not have necessary exports.
    /// - The module does not have a custom section with the workflow interface definition(s).
    #[tracing::instrument(skip_all, err, fields(bytes.len = bytes.len()))]
    async fn create_module(&self, bytes: Arc<[u8]>) -> anyhow::Result<Self::Module> {
        WasmtimeModule::new(&self.0, bytes)
    }
}

struct WasmAllocator<'ctx>(StoreContextMut<'ctx, InstanceData>);

impl fmt::Debug for WasmAllocator<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("WasmAllocator").field(&"_").finish()
    }
}

impl<'ctx> WasmAllocator<'ctx> {
    fn new(ctx: StoreContextMut<'ctx, InstanceData>) -> Self {
        Self(ctx)
    }
}

impl AllocateBytes for WasmAllocator<'_> {
    type Error = anyhow::Error;

    #[tracing::instrument(level = "trace", skip_all, ret, err, fields(bytes.len = bytes.len()))]
    fn copy_to_wasm(&mut self, bytes: &[u8]) -> anyhow::Result<(u32, u32)> {
        let bytes_len =
            u32::try_from(bytes.len()).context("integer overflow for message length")?;
        let exports = self.0.data().exports();
        let ptr = exports.alloc_bytes(self.0.as_context_mut(), bytes_len)?;

        let host_ptr = usize::try_from(ptr).unwrap();
        let memory = self.0.data_mut().exports().memory;
        memory
            .write(&mut self.0, host_ptr, bytes)
            .context("cannot write to WASM memory")?;
        Ok((ptr, bytes_len))
    }
}

pub(crate) fn copy_bytes_from_wasm(
    ctx: impl AsContext,
    memory: &Memory,
    ptr: u32,
    len: u32,
) -> anyhow::Result<Vec<u8>> {
    let ptr = usize::try_from(ptr).unwrap();
    let len = usize::try_from(len).unwrap();
    let mut buffer = vec![0_u8; len];
    memory
        .read(ctx, ptr, &mut buffer)
        .context("error copying memory from WASM")?;
    Ok(buffer)
}

pub(crate) fn copy_string_from_wasm(
    ctx: impl AsContext,
    memory: &Memory,
    ptr: u32,
    len: u32,
) -> anyhow::Result<String> {
    let buffer = copy_bytes_from_wasm(ctx, memory, ptr, len)?;
    String::from_utf8(buffer).map_err(From::from)
}
