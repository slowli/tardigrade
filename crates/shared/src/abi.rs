//! ABI.

use std::{error, task::Poll};

use crate::types::{JoinError, PollMessage, PollTask};

/// Value directly representable in WASM ABI, e.g., `i64`.
pub trait AbiValue {}

impl AbiValue for i32 {}
impl AbiValue for i64 {}

/// Simplest allocator interface.
pub trait AllocateBytes {
    type Error: error::Error + 'static;

    /// Copies `bytes` to WASM and returns a (pointer, length) tuple for the copied slice.
    fn copy_to_wasm(&mut self, bytes: &[u8]) -> Result<(u32, u32), Self::Error>;
}

/// Value convertible into WASM-compatible presentation.
pub trait IntoAbi {
    /// Output of the transformation.
    type Output: AbiValue;
    /// Performs the conversion.
    fn into_abi<A: AllocateBytes>(self, alloc: &mut A) -> Result<Self::Output, A::Error>;
}

pub trait IntoAbiOnStack {
    type Output: AbiValue;

    fn into_abi(self) -> Self::Output;
}

impl<T: IntoAbiOnStack> IntoAbi for T {
    type Output = <Self as IntoAbiOnStack>::Output;

    fn into_abi<A: AllocateBytes>(self, _: &mut A) -> Result<Self::Output, A::Error> {
        Ok(<Self as IntoAbiOnStack>::into_abi(self))
    }
}

/// Value convertible from WASM-compatible presentation.
pub trait FromAbi: IntoAbi {
    /// Performs the conversion.
    ///
    /// # Safety
    ///
    /// This function is marked as unsafe because transformation from `abi` may involve
    /// transmutations, pointer casts etc. The caller is responsible that the function
    /// is only called on the output of [`IntoAbi::into_abi()`].
    unsafe fn from_abi(abi: <Self as IntoAbi>::Output) -> Self;
}

impl IntoAbiOnStack for Poll<()> {
    type Output = i32;

    fn into_abi(self) -> Self::Output {
        match self {
            Self::Pending => -1,
            Self::Ready(()) => 0,
        }
    }
}

impl FromAbi for Poll<()> {
    unsafe fn from_abi(abi: i32) -> Self {
        match abi {
            -1 => Self::Pending,
            0 => Self::Ready(()),
            _ => unreachable!("Unexpected ABI value"),
        }
    }
}

impl IntoAbiOnStack for PollTask {
    type Output = i64;

    fn into_abi(self) -> Self::Output {
        match self {
            Self::Pending => -1,
            Self::Ready(Ok(())) => -2,
            Self::Ready(Err(JoinError::Aborted)) => -3,
            Self::Ready(Err(JoinError::Trapped)) => 0,
        }
    }
}

impl FromAbi for PollTask {
    unsafe fn from_abi(abi: i64) -> Self {
        match abi {
            -1 => Self::Pending,
            -2 => Self::Ready(Ok(())),
            -3 => Self::Ready(Err(JoinError::Aborted)),
            0 => Self::Ready(Err(JoinError::Trapped)),
            _ => panic!("Unexpected ABI value"),
        }
    }
}

impl IntoAbi for Option<Vec<u8>> {
    type Output = i64;

    fn into_abi<A: AllocateBytes>(self, alloc: &mut A) -> Result<i64, A::Error> {
        Ok(if let Some(bytes) = self {
            let (ptr, len) = alloc.copy_to_wasm(&bytes)?;
            assert!(ptr < u32::MAX, "Pointer is too large");
            (i64::from(ptr) << 32) + i64::from(len)
        } else {
            -1
        })
    }
}

impl FromAbi for Option<Vec<u8>> {
    unsafe fn from_abi(abi: i64) -> Self {
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

impl IntoAbi for PollMessage {
    type Output = i64;

    fn into_abi<A: AllocateBytes>(self, alloc: &mut A) -> Result<Self::Output, A::Error> {
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
}

impl FromAbi for PollMessage {
    unsafe fn from_abi(abi: i64) -> Self {
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
