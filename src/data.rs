//! Data inputs.

use std::{error, fmt, marker::PhantomData};

use crate::{
    codec::{Decoder, Encoder},
    context::Wasm,
};
use tardigrade_shared::workflow::{Initialize, InputsBuilder, InterfaceValidation, TakeHandle};

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

#[derive(Debug, Clone)]
pub struct Data<T, C> {
    inner: T,
    codec: PhantomData<fn() -> C>,
}

impl<T, C> Data<T, C> {
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T, C: Decoder<T> + Default> Data<T, C> {
    pub(crate) fn from_env(id: &str) -> Self {
        let raw = imp::try_get_raw_data(id)
            .unwrap_or_else(|| panic!("data input `{}` not defined in workflow interface", id));
        C::default()
            .try_decode_bytes(raw)
            .map(Self::from)
            .unwrap_or_else(|err| panic!("data input `{}` cannot be decoded: {}", id, err))
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

    fn take_handle(_env: &mut Wasm, id: &str) -> Self {
        Self::from_env(id)
    }
}

impl<T, C: Encoder<T> + Default> Initialize for Data<T, C> {
    type Init = T;
    type Id = str;

    fn initialize(builder: &mut InputsBuilder, init: T, id: &str) {
        let raw_data = C::default().encode_value(init);
        builder.set_raw_input(id, raw_data);
    }
}

#[derive(Debug)]
pub struct DataValidationError {
    name: String,
}

impl fmt::Display for DataValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.name.is_empty() {
            formatter.write_str("missing default data input")
        } else {
            write!(formatter, "missing data input `{}`", self.name)
        }
    }
}

impl error::Error for DataValidationError {}

impl<T, C> TakeHandle<InterfaceValidation<'_>> for Data<T, C>
where
    C: Encoder<T> + Decoder<T>,
{
    type Id = str;
    type Handle = ();

    fn take_handle(env: &mut InterfaceValidation<'_>, id: &str) {
        if env.interface().data_input(id).is_none() {
            env.insert_error(DataValidationError {
                name: id.to_owned(),
            });
        }
    }
}
