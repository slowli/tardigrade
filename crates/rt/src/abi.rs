//! Host-side WASM ABI helpers.

use wasmtime::{AsContext, Caller, Memory, Trap};

use std::fmt;

use crate::state::State;
use tardigrade_shared::AllocateBytes;

pub(crate) struct WasmAllocator<'a>(Caller<'a, State>);

impl fmt::Debug for WasmAllocator<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("WasmAllocator").field(&"_").finish()
    }
}

impl<'a> WasmAllocator<'a> {
    pub fn new(caller: Caller<'a, State>) -> Self {
        Self(caller)
    }
}

impl AllocateBytes for WasmAllocator<'_> {
    type Error = Trap;

    fn copy_to_wasm(&mut self, bytes: &[u8]) -> Result<(u32, u32), Trap> {
        let bytes_len = u32::try_from(bytes.len())
            .map_err(|_| Trap::new("integer overflow for message length"))?;
        let alloc = self.0.data().exports().alloc_bytes;
        let ptr = alloc.call(&mut self.0, bytes_len)?;

        let host_ptr = usize::try_from(ptr).unwrap();
        let memory = self.0.data_mut().exports().memory;
        memory.write(&mut self.0, host_ptr, bytes).map_err(|err| {
            let message = format!("cannot write to WASM memory: {}", err);
            Trap::new(message)
        })?;
        Ok((ptr, bytes_len))
    }
}

pub(crate) fn copy_bytes_from_wasm(
    ctx: impl AsContext,
    memory: &Memory,
    ptr: u32,
    len: u32,
) -> Result<Vec<u8>, Trap> {
    let ptr = usize::try_from(ptr).unwrap();
    let len = usize::try_from(len).unwrap();
    let mut buffer = vec![0_u8; len];
    memory.read(ctx, ptr, &mut buffer).map_err(|err| {
        let message = format!("error copying memory from WASM: {}", err);
        Trap::new(message)
    })?;
    Ok(buffer)
}

pub(crate) fn copy_string_from_wasm(
    ctx: impl AsContext,
    memory: &Memory,
    ptr: u32,
    len: u32,
) -> Result<String, Trap> {
    let buffer = copy_bytes_from_wasm(ctx, memory, ptr, len)?;
    String::from_utf8(buffer).map_err(|err| Trap::new(format!("invalid UTF-8 string: {}", err)))
}
