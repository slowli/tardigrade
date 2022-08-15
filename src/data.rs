//! Data inputs.

use std::marker::PhantomData;

use crate::{
    codec::{Decode, Raw},
    interface::{AccessError, AccessErrorKind, InterfaceLocation},
    Encode,
};

/// Data input for a workflow.
///
/// A data input is characterized by data type that it holds, and a codec used to [decode](Decode)
/// this data type from raw bytes (in case of the WASM environment), or [encode](Encode) it
/// to bytes (in case of usage on host).
#[derive(Debug, Clone)]
pub struct Data<T, C> {
    inner: T,
    codec: PhantomData<fn() -> C>,
}

impl<T, C> Data<T, C> {
    /// Returns the enclosed data.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T, C: Encode<T> + Default> Data<T, C> {
    /// Creates a new data instance wrapping the provided data.
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            codec: PhantomData,
        }
    }

    /// Converts this data into raw binary presentation.
    pub fn into_raw(self) -> Vec<u8> {
        C::default().encode_value(self.inner)
    }
}

impl<T, C> AsRef<T> for Data<T, C> {
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<T, C> AsMut<T> for Data<T, C> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T, C: Decode<T> + Default> Data<T, C> {
    pub(crate) fn from_raw(raw: Vec<u8>) -> Result<Self, AccessError> {
        C::default()
            .try_decode_bytes(raw)
            .map(Self::from)
            .map_err(|err| {
                AccessErrorKind::Custom(Box::new(err)).with_location(InterfaceLocation::DataInput)
            })
    }
}

impl<T, C: Decode<T> + Default> From<T> for Data<T, C> {
    fn from(inner: T) -> Self {
        Self {
            inner,
            codec: PhantomData,
        }
    }
}

/// [`Data`] input of raw bytes.
pub type RawData = Data<Vec<u8>, Raw>;
