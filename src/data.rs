//! Data inputs.

use std::marker::PhantomData;

use crate::{
    codec::{Decoder, Encoder, Raw},
    interface::{AccessError, AccessErrorKind, DataInput, Interface, ValidateInterface},
    workflow::{Initialize, InputsBuilder, TakeHandle, Wasm},
};

#[cfg(target_arch = "wasm32")]
mod imp {
    use tardigrade_shared::abi::IntoWasm;

    #[link(wasm_import_module = "tardigrade_rt")]
    extern "C" {
        #[link_name = "data_input::get"]
        fn data_input_get(input_name_ptr: *const u8, input_name_len: usize) -> i64;
    }

    pub fn try_get_raw_data(id: &str) -> Option<Vec<u8>> {
        unsafe {
            let result = data_input_get(id.as_ptr(), id.len());
            IntoWasm::from_abi_in_wasm(result)
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use crate::test::Runtime;

    pub fn try_get_raw_data(id: &str) -> Option<Vec<u8>> {
        Runtime::with_mut(|rt| rt.data_input(id))
    }
}

/// Data input for a workflow.
///
/// A data input is characterized by data type that it holds, and a codec used to [decode](Decoder)
/// this data type from raw bytes (in case of the WASM environment), or [encode](Encoder) it
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

impl<T, C: Decoder<T> + Default> Data<T, C> {
    pub(crate) fn from_env(id: &str) -> Result<Self, AccessError> {
        let raw = imp::try_get_raw_data(id)
            .ok_or_else(|| AccessErrorKind::Unknown.for_handle(DataInput(id)))?;
        C::default()
            .try_decode_bytes(raw)
            .map(Self::from)
            .map_err(|err| AccessErrorKind::Custom(Box::new(err)).for_handle(DataInput(id)))
    }
}

impl<T, C: Decoder<T> + Default> From<T> for Data<T, C> {
    fn from(inner: T) -> Self {
        Self {
            inner,
            codec: PhantomData,
        }
    }
}

impl<T, C> TakeHandle<Wasm> for Data<T, C>
where
    C: Decoder<T> + Default,
{
    type Id = str;
    type Handle = Self;

    fn take_handle(_env: &mut Wasm, id: &str) -> Result<Self, AccessError> {
        Self::from_env(id)
    }
}

impl<T, C: Encoder<T> + Default> Initialize for Data<T, C> {
    type Init = T;
    type Id = str;

    fn initialize(builder: &mut InputsBuilder, init: T, id: &str) {
        let raw_data = C::default().encode_value(init);
        builder.insert(id, raw_data);
    }
}

/// [`Data`] input of raw bytes.
pub type RawData = Data<Vec<u8>, Raw>;

impl<T, C> ValidateInterface for Data<T, C>
where
    C: Encoder<T> + Decoder<T>,
{
    type Id = str;

    fn validate_interface(interface: &Interface<()>, id: &str) -> Result<(), AccessError> {
        if interface.data_input(id).is_none() {
            Err(AccessErrorKind::Unknown.for_handle(DataInput(id)))
        } else {
            Ok(())
        }
    }
}
