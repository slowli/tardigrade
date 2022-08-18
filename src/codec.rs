//! Codecs for converting values from / to bytes.

use std::{convert::Infallible, error};

/// Decoder for a particular type.
///
/// When used in workflows (e.g., in [`Receiver`]), [`Self::decode_bytes()`] method
/// is used. That is, decoding operations panic if an error occurs during decoding. This is usually
/// the intended behavior, since the primary reason of such a panic would be an incorrect input
/// supplied via arguments or an inbound channel. The workflow runtime is expected to roll back
/// the workflow state in this case.
///
/// [`Receiver`]: crate::channel::Receiver
pub trait Decode<T> {
    /// Decoding error.
    type Error: error::Error + Send + Sync + 'static;

    /// Tries to decode `bytes`.
    ///
    /// # Errors
    ///
    /// Returns an error if `bytes` do not represent valid data.
    fn try_decode_bytes(&mut self, bytes: Vec<u8>) -> Result<T, Self::Error>;

    /// Decodes `bytes`. This is a convenience method that `unwrap()`s the result
    /// of [`Self::try_decode_bytes()`].
    ///
    /// # Panics
    ///
    /// Panics if `bytes` do not represent valid data.
    fn decode_bytes(&mut self, bytes: Vec<u8>) -> T {
        self.try_decode_bytes(bytes).expect("Cannot decode bytes")
    }
}

/// Encoder of a particular type.
///
/// Unlike [`Decode`]rs, `Encode`rs are assumed to be infallible.
pub trait Encode<T> {
    /// Encodes `value`.
    fn encode_value(&mut self, value: T) -> Vec<u8>;
}

/// Raw / identity codec that passes through byte [`Vec`]s without changes.
#[derive(Debug, Clone, Copy, Default)]
pub struct Raw;

impl Encode<Vec<u8>> for Raw {
    fn encode_value(&mut self, value: Vec<u8>) -> Vec<u8> {
        value
    }
}

impl Decode<Vec<u8>> for Raw {
    type Error = Infallible;

    fn try_decode_bytes(&mut self, bytes: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        Ok(bytes)
    }
}

#[cfg(feature = "serde_json")]
mod json {
    use serde::{de::DeserializeOwned, Serialize};

    use super::{Decode, Encode};

    /// JSON codec.
    #[derive(Debug, Clone, Copy, Default)]
    #[cfg_attr(docsrs, doc(cfg(feature = "serde_json")))]
    pub struct Json;

    /// Panics if the value cannot be serialized. Serialization errors are usually confined
    /// to edge cases (e.g., very deeply nested / recursive objects).
    impl<T: Serialize> Encode<T> for Json {
        fn encode_value(&mut self, value: T) -> Vec<u8> {
            serde_json::to_vec(&value).expect("cannot serialize value")
        }
    }

    impl<T: DeserializeOwned> Decode<T> for Json {
        type Error = serde_json::Error;

        fn try_decode_bytes(&mut self, bytes: Vec<u8>) -> Result<T, Self::Error> {
            serde_json::from_slice(&bytes)
        }
    }
}

#[cfg(feature = "serde_json")]
pub use self::json::Json;
