//! Codecs for converting values from / to bytes.

use std::{convert::Infallible, error};

/// Decoder for a particular type.
pub trait Decoder<T> {
    /// Decoding error.
    type Error: error::Error + Send + Sync + 'static;

    /// Tries to decode `bytes`.
    ///
    /// # Errors
    ///
    /// - Returns an error if `bytes` do not represent valid data.
    fn try_decode_bytes(&mut self, bytes: Vec<u8>) -> Result<T, Self::Error>;

    /// Decodes `bytes`. This is a convenience method that `unwrap()`s the result
    /// of [`Self::try_decode_bytes()`].
    ///
    /// # Panics
    ///
    /// - Panics if `bytes` do not represent valid data.
    fn decode_bytes(&mut self, bytes: Vec<u8>) -> T {
        self.try_decode_bytes(bytes).expect("Cannot decode bytes")
    }
}

/// Encoder of a particular type.
pub trait Encoder<T> {
    /// Encoding error.
    type Error: error::Error + Send + Sync + 'static;

    /// Tries to encode `value`.
    ///
    /// # Errors
    ///
    /// - Returns an error if the value cannot be encoded.
    fn try_encode_value(&mut self, value: T) -> Result<Vec<u8>, Self::Error>;

    /// Encodes `value`. This is a convenience method that `unwrap()`s the result
    /// of [`Self::try_encode_value()`].
    fn encode_value(&mut self, value: T) -> Vec<u8> {
        self.try_encode_value(value).expect("Cannot encode value")
    }
}

/// Raw / identity codec that passes through byte [`Vec`]s without changes.
#[derive(Debug, Clone, Copy, Default)]
pub struct Raw;

impl Encoder<Vec<u8>> for Raw {
    type Error = Infallible;

    fn try_encode_value(&mut self, value: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        Ok(value)
    }
}

impl Decoder<Vec<u8>> for Raw {
    type Error = Infallible;

    fn try_decode_bytes(&mut self, bytes: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        Ok(bytes)
    }
}

#[cfg(feature = "serde_json")]
mod json {
    use serde::{de::DeserializeOwned, Serialize};

    use super::{Decoder, Encoder};

    /// JSON codec.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct Json;

    impl<T: Serialize> Encoder<T> for Json {
        type Error = serde_json::Error;

        fn try_encode_value(&mut self, value: T) -> Result<Vec<u8>, Self::Error> {
            serde_json::to_vec(&value)
        }
    }

    impl<T: DeserializeOwned> Decoder<T> for Json {
        type Error = serde_json::Error;

        fn try_decode_bytes(&mut self, bytes: Vec<u8>) -> Result<T, Self::Error> {
            serde_json::from_slice(&bytes)
        }
    }
}

#[cfg(feature = "serde_json")]
pub use self::json::Json;
