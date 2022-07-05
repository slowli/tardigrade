//! ABI.

use std::{error, fmt, task::Poll};

use crate::{
    types::{JoinError, PollMessage, PollTask},
    workflow::HandleErrorKind,
};

/// Value directly representable in WASM ABI, e.g., `i64`.
pub trait WasmValue {}

impl WasmValue for i32 {}
impl WasmValue for i64 {}

/// Simplest allocator interface.
pub trait AllocateBytes {
    type Error: error::Error + 'static;

    /// Copies `bytes` to WASM and returns a (pointer, length) tuple for the copied slice.
    fn copy_to_wasm(&mut self, bytes: &[u8]) -> Result<(u32, u32), Self::Error>;
}

/// Value that can be transferred from host to WASM.
pub trait IntoWasm: Sized {
    /// Output of the transformation.
    type Abi: WasmValue;
    /// Converts a host value into WASM presentation.
    fn into_wasm<A: AllocateBytes>(self, alloc: &mut A) -> Result<Self::Abi, A::Error>;
    /// Performs the conversion.
    ///
    /// # Safety
    ///
    /// This function is marked as unsafe because transformation from `abi` may involve
    /// transmutations, pointer casts etc. The caller is responsible that the function
    /// is only called on the output of [`IntoAbi::into_abi()`].
    unsafe fn from_abi_in_wasm(abi: Self::Abi) -> Self;
}

/// Value that can be transferred from WASM to host.
pub trait TryFromWasm: Sized {
    /// Output of the transformation.
    type Abi: WasmValue;
    /// Converts WASM value into transferable presentation.
    fn into_abi_in_wasm(self) -> Self::Abi;
    /// Tries to read the value on host; fails if the value is invalid.
    fn try_from_wasm(abi: Self::Abi) -> Result<Self, FromWasmError>;
}

#[derive(Debug)]
pub struct FromWasmError {
    message: String,
}

impl FromWasmError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for FromWasmError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "failed ABI conversion: {}", self.message)
    }
}

impl From<FromWasmError> for String {
    fn from(err: FromWasmError) -> Self {
        err.message
    }
}

impl error::Error for FromWasmError {}

impl IntoWasm for Poll<()> {
    type Abi = i32;

    fn into_wasm<A: AllocateBytes>(self, _: &mut A) -> Result<Self::Abi, A::Error> {
        Ok(TryFromWasm::into_abi_in_wasm(self))
    }

    unsafe fn from_abi_in_wasm(abi: i32) -> Self {
        match abi {
            -1 => Self::Pending,
            0 => Self::Ready(()),
            _ => unreachable!("Unexpected ABI value"),
        }
    }
}

impl TryFromWasm for Poll<()> {
    type Abi = i32;

    fn into_abi_in_wasm(self) -> Self::Abi {
        match self {
            Self::Pending => -1,
            Self::Ready(()) => 0,
        }
    }

    fn try_from_wasm(abi: Self::Abi) -> Result<Self, FromWasmError> {
        Ok(match abi {
            -1 => Self::Pending,
            0 => Self::Ready(()),
            _ => return Err(FromWasmError::new("unexpected `Poll<()>` value")),
        })
    }
}

impl IntoWasm for PollTask {
    type Abi = i64;

    fn into_wasm<A: AllocateBytes>(self, _: &mut A) -> Result<Self::Abi, A::Error> {
        Ok(match self {
            Self::Pending => -1,
            Self::Ready(Ok(())) => -2,
            Self::Ready(Err(JoinError::Aborted)) => -3,
            Self::Ready(Err(JoinError::Trapped)) => 0,
        })
    }

    unsafe fn from_abi_in_wasm(abi: i64) -> Self {
        match abi {
            -1 => Self::Pending,
            -2 => Self::Ready(Ok(())),
            -3 => Self::Ready(Err(JoinError::Aborted)),
            0 => Self::Ready(Err(JoinError::Trapped)),
            _ => panic!("Unexpected ABI value"),
        }
    }
}

impl IntoWasm for Option<Vec<u8>> {
    type Abi = i64;

    fn into_wasm<A: AllocateBytes>(self, alloc: &mut A) -> Result<i64, A::Error> {
        Ok(if let Some(bytes) = self {
            let (ptr, len) = alloc.copy_to_wasm(&bytes)?;
            assert!(ptr < u32::MAX, "Pointer is too large");
            (i64::from(ptr) << 32) + i64::from(len)
        } else {
            -1
        })
    }

    unsafe fn from_abi_in_wasm(abi: i64) -> Self {
        match abi {
            -1 => None,
            _ => Some({
                let ptr = (abi >> 32) as *mut u8;
                let len = (abi & 0xffff_ffff) as usize;
                Vec::from_raw_parts(ptr, len, len)
            }),
        }
    }
}

impl IntoWasm for PollMessage {
    type Abi = i64;

    fn into_wasm<A: AllocateBytes>(self, alloc: &mut A) -> Result<Self::Abi, A::Error> {
        Ok(match self {
            Self::Pending => -1,
            Self::Ready(None) => -2,
            Self::Ready(Some(bytes)) => {
                let (ptr, len) = alloc.copy_to_wasm(&bytes)?;
                assert!(ptr < u32::MAX, "Pointer is too large");
                (i64::from(ptr) << 32) + i64::from(len)
            }
        })
    }

    unsafe fn from_abi_in_wasm(abi: i64) -> Self {
        match abi {
            -1 => Self::Pending,
            -2 => Self::Ready(None),
            _ => {
                let ptr = (abi >> 32) as *mut u8;
                let len = (abi & 0xffff_ffff) as usize;
                let bytes = Vec::from_raw_parts(ptr, len, len);
                Self::Ready(Some(bytes))
            }
        }
    }
}

impl IntoWasm for Result<(), HandleErrorKind> {
    type Abi = i64;

    fn into_wasm<A: AllocateBytes>(self, alloc: &mut A) -> Result<Self::Abi, A::Error> {
        Ok(match self {
            Ok(()) => 0,
            Err(HandleErrorKind::Unknown) => -1,
            Err(HandleErrorKind::AlreadyAcquired) => -2,
            Err(HandleErrorKind::Custom(err)) => {
                let message = err.to_string();
                let (ptr, len) = alloc.copy_to_wasm(message.as_bytes())?;
                (i64::from(ptr) << 32) + i64::from(len)
            }
        })
    }

    unsafe fn from_abi_in_wasm(abi: i64) -> Self {
        Err(match abi {
            0 => return Ok(()),
            -1 => HandleErrorKind::Unknown,
            -2 => HandleErrorKind::AlreadyAcquired,
            _ => {
                let ptr = (abi >> 32) as *mut u8;
                let len = (abi & 0xffff_ffff) as usize;
                let message = String::from_raw_parts(ptr, len, len);
                HandleErrorKind::Custom(message.into())
            }
        })
    }
}
