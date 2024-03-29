//! Codecs for converting values from / to bytes.

use serde::{de::DeserializeOwned, Serialize};

use std::{convert::Infallible, error};

/// Codec for a particular data type.
///
/// When used for decoding in workflows (e.g., in [`Receiver`]), [`Self::decode_bytes()`] method
/// is used. That is, decoding operations panic if an error occurs during decoding. This is usually
/// the intended behavior, since the primary reason of such a panic would be an incorrect input
/// supplied via arguments or a channel receiver. The workflow runtime is expected to roll back
/// the workflow state in this case.
///
/// [`Receiver`]: crate::channel::Receiver
pub trait Codec<T>: 'static + Send + Sync {
    /// Decoding error.
    type Error: error::Error + Send + Sync + 'static;

    /// Encodes `value`.
    fn encode_value(value: T) -> Vec<u8>;

    /// Tries to decode `bytes`.
    ///
    /// # Errors
    ///
    /// Returns an error if `bytes` do not represent valid data.
    fn try_decode_bytes(bytes: Vec<u8>) -> Result<T, Self::Error>;

    /// Decodes `bytes`. This is a convenience method that `unwrap()`s the result
    /// of [`Self::try_decode_bytes()`].
    ///
    /// # Panics
    ///
    /// Panics if `bytes` do not represent valid data.
    fn decode_bytes(bytes: Vec<u8>) -> T {
        Self::try_decode_bytes(bytes).expect("Cannot decode bytes")
    }
}

/// Raw / identity codec that passes through byte [`Vec`]s without changes.
#[derive(Debug, Clone, Copy, Default)]
pub struct Raw;

impl Codec<Vec<u8>> for Raw {
    type Error = Infallible;

    fn encode_value(value: Vec<u8>) -> Vec<u8> {
        value
    }

    fn try_decode_bytes(bytes: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        Ok(bytes)
    }
}

/// JSON codec.
#[derive(Debug, Clone, Copy, Default)]
pub struct Json;

/// Panics if the value cannot be serialized. Serialization errors are usually confined
/// to edge cases (e.g., very deeply nested / recursive objects).
impl<T: Serialize + DeserializeOwned> Codec<T> for Json {
    type Error = serde_json::Error;

    fn encode_value(value: T) -> Vec<u8> {
        serde_json::to_vec(&value).expect("cannot serialize value")
    }

    fn try_decode_bytes(bytes: Vec<u8>) -> Result<T, Self::Error> {
        serde_json::from_slice(&bytes)
    }
}
