//! Misc utils.

use wasmtime::{AsContext, AsContextMut, Memory, StoreContextMut, Trap};

use std::{fmt, task::Poll};

use crate::data::WorkflowData;
use tardigrade_shared::abi::AllocateBytes;

#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => {
        log::trace!(target: "tardigrade_rt", $($arg)*);
    };
}

#[macro_export]
macro_rules! log_result {
    ($result:tt, $($arg:tt)*) => {{
        match &$result {
            Ok(val) => {
                log::trace!(target: "tardigrade_rt", "{}: {:?}", format_args!($($arg)*), val);
            }
            Err(err) => {
                log::warn!(target: "tardigrade_rt", "{}: {}", format_args!($($arg)*), err);
            }
        }
        $result
    }};
}

pub(crate) struct WasmAllocator<'a>(StoreContextMut<'a, WorkflowData>);

impl fmt::Debug for WasmAllocator<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("WasmAllocator").field(&"_").finish()
    }
}

impl<'a> WasmAllocator<'a> {
    pub fn new(ctx: StoreContextMut<'a, WorkflowData>) -> Self {
        Self(ctx)
    }
}

impl AllocateBytes for WasmAllocator<'_> {
    type Error = Trap;

    fn copy_to_wasm(&mut self, bytes: &[u8]) -> Result<(u32, u32), Trap> {
        let bytes_len = u32::try_from(bytes.len())
            .map_err(|_| Trap::new("integer overflow for message length"))?;
        let exports = self.0.data().exports();
        let ptr = exports.alloc_bytes(self.0.as_context_mut(), bytes_len)?;

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

pub(crate) fn drop_value<T>(poll_result: &Poll<T>) -> Poll<()> {
    match poll_result {
        Poll::Pending => Poll::Pending,
        Poll::Ready(_) => Poll::Ready(()),
    }
}
