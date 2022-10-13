//! Misc utils.

use serde::{Deserialize, Serialize};
use wasmtime::{AsContext, AsContextMut, Memory, StoreContextMut, Trap};

use std::{cmp::Ordering, fmt, mem, task::Poll};

use crate::data::WorkflowData;
use tardigrade_shared::{abi::AllocateBytes, JoinError};

#[cfg(feature = "log")]
macro_rules! trace {
    ($($arg:tt)*) => {
        log::trace!(target: "tardigrade_rt", $($arg)*);
    };
}

#[cfg(not(feature = "log"))]
macro_rules! trace {
    ($($arg:tt)*) => {{
        let _ = format_args!($($arg)*);
    }}
}

#[cfg(feature = "log")]
macro_rules! warn {
    ($($arg:tt)*) => {
        log::warn!(target: "tardigrade_rt", $($arg)*);
    };
}

#[cfg(not(feature = "log"))]
macro_rules! warn {
    ($($arg:tt)*) => {{
        let _ = format_args!($($arg)*);
    }}
}

#[cfg(feature = "log")]
macro_rules! log_result {
    ($result:ident, $($arg:tt)*) => {{
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

#[cfg(not(feature = "log"))]
macro_rules! log_result {
    ($result:tt, $($arg:tt)*) => {
        ($result, format_args!($($arg)*)).0
    };
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

/// Merges two ordered `Vec`s into a single ordered `Vec`.
pub(crate) fn merge_vec<T: Ord>(target: &mut Vec<T>, source: Vec<T>) {
    debug_assert!(target.windows(2).all(|window| match window {
        [prev, next] => prev <= next,
        _ => unreachable!(),
    }));
    debug_assert!(source.windows(2).all(|window| match window {
        [prev, next] => prev <= next,
        _ => unreachable!(),
    }));

    if target.is_empty() {
        *target = source;
        return;
    }

    let mut xs = mem::replace(target, Vec::with_capacity(target.len() + source.len()))
        .into_iter()
        .peekable();
    let mut ys = source.into_iter().peekable();
    loop {
        match (xs.peek(), ys.peek()) {
            (Some(x), Some(y)) => match x.cmp(y) {
                Ordering::Less => target.push(xs.next().unwrap()),
                Ordering::Greater => target.push(ys.next().unwrap()),
                Ordering::Equal => {
                    target.push(xs.next().unwrap());
                    target.push(ys.next().unwrap());
                }
            },
            (Some(_), None) => {
                target.extend(xs);
                break;
            }
            (None, Some(_)) => {
                target.extend(ys);
                break;
            }
            (None, None) => break,
        }
    }
}

#[test]
fn merging_vectors() {
    let mut target = vec![];
    merge_vec(&mut target, vec![1, 3, 4, 7, 9]);
    assert_eq!(target, [1, 3, 4, 7, 9]);

    merge_vec(&mut target, vec![2, 3, 3, 8, 13, 20]);
    assert_eq!(target, [1, 2, 3, 3, 3, 4, 7, 8, 9, 13, 20]);

    merge_vec(&mut target, vec![5]);
    assert_eq!(target, [1, 2, 3, 3, 3, 4, 5, 7, 8, 9, 13, 20]);
}

pub(crate) fn clone_completion_result(
    result: &Poll<Result<(), JoinError>>,
) -> Poll<Result<(), JoinError>> {
    match result {
        Poll::Pending => Poll::Pending,
        Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        Poll::Ready(Err(JoinError::Aborted)) => Poll::Ready(Err(JoinError::Aborted)),
        Poll::Ready(Err(JoinError::Err(err))) => {
            Poll::Ready(Err(JoinError::Err(err.clone_boxed())))
        }
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
