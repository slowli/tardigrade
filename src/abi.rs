//! Simple WASM ABI for interaction between the host and the workflow client.
//!
//! In the future, this ABI could be replaced with [Canonical ABI] generated with [`wit-bindgen`].
//! For now, a hand-written conversions between host and WASM environments are used instead.
//!
//! [Canonical ABI]: https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md
//! [`wit-bindgen`]: https://github.com/bytecodealliance/wit-bindgen

use chrono::{DateTime, TimeZone, Utc};
use futures::future::Aborted;

use std::{error, fmt, task::Poll};

use crate::{
    error::{HostError, SendError, TaskError},
    interface::{AccessErrorKind, ChannelKind},
};

/// Result of polling a receiver end of a channel.
pub type PollMessage = Poll<Option<Vec<u8>>>;
/// Result of polling a workflow task.
pub type PollTask = Poll<Result<(), Aborted>>;

/// Value directly representable in WASM ABI, e.g., `i64`.
pub trait WasmValue {}

impl WasmValue for i32 {}
impl WasmValue for i64 {}

/// Simple WASM allocator interface.
pub trait AllocateBytes {
    /// Allocation error.
    type Error: error::Error + 'static;

    /// Copies `bytes` to WASM and returns a (pointer, length) tuple for the copied slice.
    ///
    /// # Errors
    ///
    /// Returns an error on allocation failure (e.g., if the allocator runs out of available
    /// memory).
    fn copy_to_wasm(&mut self, bytes: &[u8]) -> Result<(u32, u32), Self::Error>;
}

/// Value that can be transferred from host to WASM.
pub trait IntoWasm: Sized {
    /// Output of the transformation.
    type Abi: WasmValue;

    /// Converts a host value into WASM presentation. The conversion logic can use the provided
    /// `alloc`ator.
    ///
    /// # Errors
    ///
    /// Returns an allocation error, if any occurs during the conversion.
    fn into_wasm<A: AllocateBytes>(self, alloc: &mut A) -> Result<Self::Abi, A::Error>;

    /// Performs the conversion.
    ///
    /// # Safety
    ///
    /// This function is marked as unsafe because transformation from `abi` may involve
    /// transmutations, pointer casts etc. The caller is responsible that the function
    /// is only called on the output of [`Self::into_wasm()`].
    unsafe fn from_abi_in_wasm(abi: Self::Abi) -> Self;
}

/// Value that can be transferred from WASM to host.
pub trait TryFromWasm: Sized {
    /// Output of the transformation.
    type Abi: WasmValue;

    /// Converts WASM value into transferable presentation.
    fn into_abi_in_wasm(self) -> Self::Abi;

    /// Tries to read the value on host.
    ///
    /// # Errors
    ///
    /// Returns an error if the WASM value is invalid.
    fn try_from_wasm(abi: Self::Abi) -> Result<Self, FromWasmError>;
}

/// Errors that can occur when converting a WASM value on host using [`TryFromWasm`] trait.
#[derive(Debug)]
pub struct FromWasmError {
    message: String,
}

impl FromWasmError {
    /// Creates an error with the specified `message`.
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

    #[allow(clippy::cast_sign_loss)] // safe by design
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

impl IntoWasm for TaskError {
    type Abi = i64;

    fn into_wasm<A: AllocateBytes>(self, alloc: &mut A) -> Result<Self::Abi, A::Error> {
        let serialized = serde_json::to_vec(&self).expect("failed serializing `TaskError`");
        Some(serialized).into_wasm(alloc)
    }

    unsafe fn from_abi_in_wasm(abi: Self::Abi) -> Self {
        let serialized = Option::<Vec<u8>>::from_abi_in_wasm(abi).unwrap();
        serde_json::from_slice(&serialized).expect("failed deserializing `TaskError`")
    }
}

impl IntoWasm for PollTask {
    type Abi = i64;

    fn into_wasm<A: AllocateBytes>(self, _: &mut A) -> Result<Self::Abi, A::Error> {
        Ok(match self {
            Self::Pending => -1,
            Self::Ready(Ok(())) => -2,
            Self::Ready(Err(Aborted)) => -3,
        })
    }

    unsafe fn from_abi_in_wasm(abi: i64) -> Self {
        match abi {
            -1 => Self::Pending,
            -2 => Self::Ready(Ok(())),
            -3 => Self::Ready(Err(Aborted)),
            _ => panic!("Unexpected ABI value"),
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

    #[allow(clippy::cast_sign_loss)] // safe by design
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

impl IntoWasm for Poll<DateTime<Utc>> {
    type Abi = i64;

    fn into_wasm<A: AllocateBytes>(self, _alloc: &mut A) -> Result<Self::Abi, A::Error> {
        Ok(match self {
            Poll::Pending => -1,
            Poll::Ready(timestamp) => timestamp.timestamp_millis(),
        })
    }

    unsafe fn from_abi_in_wasm(abi: i64) -> Self {
        match abi {
            -1 => Poll::Pending,
            _ => Poll::Ready(Utc.timestamp_millis(abi)),
        }
    }
}

impl IntoWasm for Result<(), AccessErrorKind> {
    type Abi = i64;

    fn into_wasm<A: AllocateBytes>(self, alloc: &mut A) -> Result<Self::Abi, A::Error> {
        Ok(match self {
            Ok(()) => 0,
            Err(AccessErrorKind::Unknown) => -1,
            Err(AccessErrorKind::Custom(err)) => {
                let message = err.to_string();
                let (ptr, len) = alloc.copy_to_wasm(message.as_bytes())?;
                (i64::from(ptr) << 32) + i64::from(len)
            }
            Err(_) => unreachable!(), // no other `AccessErrorKind` variants
        })
    }

    #[allow(clippy::cast_sign_loss)] // safe by design
    unsafe fn from_abi_in_wasm(abi: i64) -> Self {
        Err(match abi {
            0 => return Ok(()),
            -1 => AccessErrorKind::Unknown,
            _ => {
                let ptr = (abi >> 32) as *mut u8;
                let len = (abi & 0xffff_ffff) as usize;
                let message = String::from_raw_parts(ptr, len, len);
                AccessErrorKind::Custom(message.into())
            }
        })
    }
}

impl IntoWasm for Result<(), SendError> {
    type Abi = i32;

    fn into_wasm<A: AllocateBytes>(self, _: &mut A) -> Result<Self::Abi, A::Error> {
        Ok(match self {
            Ok(()) => 0,
            Err(SendError::Full) => 1,
            Err(SendError::Closed) => 2,
        })
    }

    unsafe fn from_abi_in_wasm(abi: i32) -> Self {
        Err(match abi {
            0 => return Ok(()),
            1 => SendError::Full,
            2 => SendError::Closed,
            _ => panic!("Unexpected ABI value"),
        })
    }
}

impl IntoWasm for Poll<Result<(), SendError>> {
    type Abi = i32;

    fn into_wasm<A: AllocateBytes>(self, alloc: &mut A) -> Result<Self::Abi, A::Error> {
        match self {
            Poll::Ready(value) => value.into_wasm(alloc),
            Poll::Pending => Ok(-1),
        }
    }

    unsafe fn from_abi_in_wasm(abi: i32) -> Self {
        match abi {
            -1 => Poll::Pending,
            _ => Poll::Ready(Result::from_abi_in_wasm(abi)),
        }
    }
}

impl IntoWasm for Result<(), HostError> {
    type Abi = i64;

    fn into_wasm<A: AllocateBytes>(self, alloc: &mut A) -> Result<Self::Abi, A::Error> {
        Ok(match self {
            Ok(()) => 0,
            Err(err) => {
                let message = err.to_string();
                let (ptr, len) = alloc.copy_to_wasm(message.as_bytes())?;
                (i64::from(ptr) << 32) + i64::from(len)
            }
        })
    }

    #[allow(clippy::cast_sign_loss)] // safe by design
    unsafe fn from_abi_in_wasm(abi: i64) -> Self {
        if abi == 0 {
            Ok(())
        } else {
            let ptr = (abi >> 32) as *mut u8;
            let len = (abi & 0xffff_ffff) as usize;
            let message = String::from_raw_parts(ptr, len, len);
            Err(HostError::new(message))
        }
    }
}

impl TryFromWasm for ChannelKind {
    type Abi = i32;

    fn into_abi_in_wasm(self) -> Self::Abi {
        match self {
            Self::Inbound => 0,
            Self::Outbound => 1,
        }
    }

    fn try_from_wasm(abi: Self::Abi) -> Result<Self, FromWasmError> {
        match abi {
            0 => Ok(Self::Inbound),
            1 => Ok(Self::Outbound),
            _ => Err(FromWasmError::new("unexpected `ChannelKind` value")),
        }
    }
}
