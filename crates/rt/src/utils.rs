//! Misc utils.

use futures::{future::Aborted, stream::BoxStream, Stream, StreamExt};
use ouroboros::self_referencing;
use serde::{Deserialize, Serialize};

use std::{
    borrow::Borrow,
    fmt,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use tardigrade::task::JoinError;

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

pub(crate) fn extract_task_poll_result<E>(result: Result<(), E>) -> Result<(), Aborted>
where
    E: Borrow<JoinError>,
{
    result.or_else(|err| match err.borrow() {
        JoinError::Err(_) => Ok(()),
        JoinError::Aborted => Err(Aborted),
    })
}

/// Self-referential stream that borrows from a source. Used with readonly transactions.
#[self_referencing]
pub(crate) struct RefStream<'a, S: 'a, T> {
    // Fake lifetime to make ouroboros-generated code happy (otherwise, `S` is required
    // to have `'static` lifetime).
    lifetime: PhantomData<&'a ()>,
    source: S,
    #[borrows(source)]
    #[covariant]
    stream: BoxStream<'this, T>,
}

impl<'a, S: 'a, T> RefStream<'a, S, T> {
    pub fn from_source(source: S, stream_builder: impl FnOnce(&S) -> BoxStream<'_, T>) -> Self {
        RefStreamBuilder {
            lifetime: PhantomData,
            source,
            stream_builder,
        }
        .build()
    }
}

impl<'a, S: 'a, T> Stream for RefStream<'a, S, T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.with_stream_mut(|stream| stream.poll_next_unpin(cx))
    }

    #[allow(clippy::redundant_closure_for_method_calls)] // false positive
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.with_stream(|stream| stream.size_hint())
    }
}

pub(crate) mod serde_b64 {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
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
            serializer.serialize_str(&URL_SAFE_NO_PAD.encode(value))
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
                URL_SAFE_NO_PAD
                    .decode(value)
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
