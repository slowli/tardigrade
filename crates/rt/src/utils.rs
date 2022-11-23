//! Misc utils.

use anyhow::Context;
use futures::future::Aborted;
use serde::{Deserialize, Serialize};
use wasmtime::{AsContext, AsContextMut, Memory, StoreContextMut};

use std::{fmt, task::Poll};

use crate::data::WorkflowData;
use tardigrade::{abi::AllocateBytes, task::JoinError};

pub(crate) fn debug_result<T, E>(result: &Result<T, E>)
where
    T: fmt::Debug,
    E: fmt::Debug + fmt::Display,
{
    match result {
        Ok(_) => tracing::debug!(result = ?result),
        Err(err) => tracing::warn!(result.err = %err, "erroneous result"),
    }
}

/// Thin wrapper around `Vec<u8>`.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct Message(#[serde(with = "serde_b64")] Vec<u8>);

impl fmt::Debug for Message {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Message")
            .field("len", &self.0.len())
            .finish()
    }
}

impl AsRef<[u8]> for Message {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Message {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<Message> for Vec<u8> {
    fn from(message: Message) -> Self {
        message.0
    }
}

pub(crate) struct WasmAllocator<'ctx, 'a>(StoreContextMut<'ctx, WorkflowData<'a>>);

impl fmt::Debug for WasmAllocator<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("WasmAllocator").field(&"_").finish()
    }
}

impl<'ctx, 'a> WasmAllocator<'ctx, 'a> {
    pub fn new(ctx: StoreContextMut<'ctx, WorkflowData<'a>>) -> Self {
        Self(ctx)
    }
}

impl AllocateBytes for WasmAllocator<'_, '_> {
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

#[cfg(test)]
#[allow(clippy::cast_sign_loss)]
pub(crate) fn decode_string(poll_res: i64) -> (u32, u32) {
    let ptr = ((poll_res as u64) >> 32) as u32;
    let len = (poll_res & 0x_ffff_ffff) as u32;
    (ptr, len)
}

pub(crate) fn drop_value<T>(poll_result: &Poll<T>) -> Poll<()> {
    match poll_result {
        Poll::Pending => Poll::Pending,
        Poll::Ready(_) => Poll::Ready(()),
    }
}

pub(crate) fn clone_join_error(err: &JoinError) -> JoinError {
    match err {
        JoinError::Aborted => JoinError::Aborted,
        JoinError::Err(err) => JoinError::Err(err.clone_boxed()),
    }
}

pub(crate) fn clone_completion_result(
    result: &Poll<Result<(), JoinError>>,
) -> Poll<Result<(), JoinError>> {
    match result {
        Poll::Pending => Poll::Pending,
        Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        Poll::Ready(Err(err)) => Poll::Ready(Err(clone_join_error(err))),
    }
}

pub(crate) fn extract_task_poll_result(result: Result<(), &JoinError>) -> Result<(), Aborted> {
    result.or_else(|err| match err {
        JoinError::Err(_) => Ok(()),
        JoinError::Aborted => Err(Aborted),
    })
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
        struct Base64Visitor;

        impl<'de> Visitor<'de> for Base64Visitor {
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
            deserializer.deserialize_str(Base64Visitor)
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
    enum SerializedPoll<T> {
        Pending,
        Ready(T),
    }

    impl<'a, T> SerializedPoll<&'a T> {
        fn from_ref(poll: &'a Poll<T>) -> Self {
            match poll {
                Poll::Pending => Self::Pending,
                Poll::Ready(value) => Self::Ready(value),
            }
        }
    }

    impl<T> From<SerializedPoll<T>> for Poll<T> {
        fn from(poll: SerializedPoll<T>) -> Self {
            match poll {
                SerializedPoll::Pending => Poll::Pending,
                SerializedPoll::Ready(value) => Poll::Ready(value),
            }
        }
    }

    pub fn serialize<T: Serialize, S: Serializer>(
        value: &Poll<T>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        SerializedPoll::from_ref(value).serialize(serializer)
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<Poll<T>, D::Error>
    where
        T: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        SerializedPoll::<T>::deserialize(deserializer).map(From::from)
    }
}

pub(crate) mod serde_poll_res {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use std::task::Poll;

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum PollResult<T, E> {
        Pending,
        Ok(T),
        Error(E),
    }

    impl<'a, T, E> PollResult<&'a T, &'a E> {
        fn from_ref(poll: &'a Poll<Result<T, E>>) -> Self {
            match poll {
                Poll::Pending => Self::Pending,
                Poll::Ready(Ok(value)) => Self::Ok(value),
                Poll::Ready(Err(err)) => Self::Error(err),
            }
        }
    }

    impl<T, E> From<PollResult<T, E>> for Poll<Result<T, E>> {
        fn from(result: PollResult<T, E>) -> Self {
            match result {
                PollResult::Pending => Poll::Pending,
                PollResult::Ok(value) => Poll::Ready(Ok(value)),
                PollResult::Error(err) => Poll::Ready(Err(err)),
            }
        }
    }

    pub fn serialize<T: Serialize, E: Serialize, S: Serializer>(
        value: &Poll<Result<T, E>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        PollResult::from_ref(value).serialize(serializer)
    }

    pub fn deserialize<'de, T, E, D>(deserializer: D) -> Result<Poll<Result<T, E>>, D::Error>
    where
        T: Deserialize<'de>,
        E: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        PollResult::<T, E>::deserialize(deserializer).map(From::from)
    }
}

pub(crate) mod serde_trap {
    use serde::{Deserialize, Deserializer, Serializer};

    use std::sync::Arc;

    pub fn serialize<S: Serializer>(
        trap: &anyhow::Error,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&trap.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<anyhow::Error>, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(|message| Arc::new(anyhow::Error::msg(message)))
    }
}
