//! Misc utils.

use wasmtime::{AsContext, AsContextMut, Memory, StoreContextMut, Trap};

use std::{fmt, task::Poll};

use crate::data::WorkflowData;
use tardigrade_shared::abi::AllocateBytes;

#[macro_export]
#[doc(hidden)] // not public
macro_rules! trace {
    ($($arg:tt)*) => {
        log::trace!(target: "tardigrade_rt", $($arg)*);
    };
}

#[macro_export]
#[doc(hidden)] // not public
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

pub(crate) mod serde_b64 {
    use serde::{
        de::{Error as DeError, Unexpected, Visitor},
        Deserializer, Serializer,
    };

    use std::fmt;

    pub fn serialize<T: AsRef<[u8]>, S: Serializer>(
        value: &T,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&base64::encode_config(value, base64::URL_SAFE_NO_PAD))
        } else {
            serializer.serialize_bytes(value.as_ref())
        }
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: From<Vec<u8>>,
    {
        struct HexVisitor;

        impl<'de> Visitor<'de> for HexVisitor {
            type Value = Vec<u8>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("base64-encoded byte array")
            }

            fn visit_str<E: DeError>(self, value: &str) -> Result<Self::Value, E> {
                base64::decode_config(value, base64::URL_SAFE_NO_PAD)
                    .map_err(|_| E::invalid_type(Unexpected::Str(value), &self))
            }

            fn visit_bytes<E: DeError>(self, value: &[u8]) -> Result<Self::Value, E> {
                Ok(value.to_vec())
            }
        }

        struct BytesVisitor;

        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = Vec<u8>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("byte array")
            }

            fn visit_bytes<E: DeError>(self, value: &[u8]) -> Result<Self::Value, E> {
                Ok(value.to_vec())
            }

            fn visit_byte_buf<E: DeError>(self, value: Vec<u8>) -> Result<Self::Value, E> {
                Ok(value)
            }
        }

        let maybe_bytes = if deserializer.is_human_readable() {
            deserializer.deserialize_str(HexVisitor)
        } else {
            deserializer.deserialize_byte_buf(BytesVisitor)
        };
        maybe_bytes.map(From::from)
    }
}

pub(crate) mod serde_poll {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use std::task::Poll;

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum PollResult<E> {
        Pending,
        Ok,
        Error(E),
    }

    impl<'a, E> PollResult<&'a E> {
        fn from_ref(poll: &'a Poll<Result<(), E>>) -> Self {
            match poll {
                Poll::Pending => Self::Pending,
                Poll::Ready(Ok(())) => Self::Ok,
                Poll::Ready(Err(err)) => Self::Error(err),
            }
        }
    }

    impl<E> From<PollResult<E>> for Poll<Result<(), E>> {
        fn from(result: PollResult<E>) -> Self {
            match result {
                PollResult::Pending => Poll::Pending,
                PollResult::Ok => Poll::Ready(Ok(())),
                PollResult::Error(err) => Poll::Ready(Err(err)),
            }
        }
    }

    pub fn serialize<E: Serialize, S: Serializer>(
        value: &Poll<Result<(), E>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        PollResult::from_ref(value).serialize(serializer)
    }

    pub fn deserialize<'de, E, D>(deserializer: D) -> Result<Poll<Result<(), E>>, D::Error>
    where
        E: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        PollResult::<E>::deserialize(deserializer).map(From::from)
    }
}
