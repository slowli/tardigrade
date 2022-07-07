//! Workflow initialization.

use std::collections::HashMap;

use tardigrade_shared::interface::Interface;

/// Type that can contribute to initializing a workflow.
///
/// This trait is sort of dual to [`TakeHandle`]. It is implemented for data inputs, and is derived
/// for workflow types.
///
/// [`TakeHandle`]: crate::workflow::TakeHandle
pub trait Initialize {
    /// Type of the initializing value. For example, this is actual data for data inputs.
    type Init;
    /// ID determining where to put the initializer, usually a `str` (for named values)
    /// or `()` (for singleton values).
    type Id: ?Sized;

    /// Inserts `init`ialization data into `builder` using its "location" `id`.
    fn initialize(builder: &mut InputsBuilder, init: Self::Init, id: &Self::Id);
}

/// Initializing value for a particular type.
pub type Init<T> = <T as Initialize>::Init;

impl Initialize for () {
    type Init = Inputs;
    type Id = ();

    fn initialize(builder: &mut InputsBuilder, init: Self::Init, _id: &Self::Id) {
        builder.inputs = init
            .inner
            .into_iter()
            .map(|(name, bytes)| (name, Some(bytes)))
            .collect();
    }
}

/// Builder for workflow [`Inputs`].
#[derive(Debug, Clone)]
pub struct InputsBuilder {
    inputs: HashMap<String, Option<Vec<u8>>>,
}

impl InputsBuilder {
    /// Creates a builder that can be used to supply [`Inputs`] to create workflow
    /// instances for the provided interface.
    pub fn new<W>(interface: &Interface<W>) -> InputsBuilder {
        Self {
            inputs: interface
                .data_inputs()
                .map(|(name, _)| (name.to_owned(), None))
                .collect(),
        }
    }

    /// Inserts `raw_data` for an input with the specified `name`. It is caller's responsibility
    /// to ensure that raw data has an appropriate format.
    ///
    /// # Panics
    ///
    /// Panics if `name` does not correspond to a data input in the workflow interface.
    /// This can be checked beforehand using [`Self::requires_input()`].
    pub fn insert(&mut self, name: &str, raw_data: Vec<u8>) {
        let data_entry = self
            .inputs
            .get_mut(name)
            .unwrap_or_else(|| panic!("Workflow does not have data input `{}`", name));
        *data_entry = Some(raw_data);
    }

    /// Checks whether the builder requires an input with the specified `name`.
    pub fn requires_input(&self, name: &str) -> bool {
        self.inputs.get(name).map_or(false, Option::is_none)
    }

    /// Returns names of missing inputs.
    pub fn missing_input_names(&self) -> impl Iterator<Item = &str> + '_ {
        self.inputs.iter().filter_map(|(name, maybe_data)| {
            if maybe_data.is_none() {
                Some(name.as_str())
            } else {
                None
            }
        })
    }

    /// Create [`Inputs`] from this builder.
    ///
    /// # Panics
    ///
    /// Panics if any inputs are not supplied.
    pub fn build(self) -> Inputs {
        let inputs = self.inputs.into_iter().map(|(name, maybe_data)| {
            let data =
                maybe_data.unwrap_or_else(|| panic!("Workflow input `{}` is not supplied", name));
            (name, data)
        });
        Inputs {
            inner: inputs.collect(),
        }
    }
}

/// Container for workflow inputs.
#[derive(Debug, Clone)]
pub struct Inputs {
    inner: HashMap<String, Vec<u8>>,
}

impl Inputs {
    #[doc(hidden)]
    pub fn for_interface<W>(interface: &Interface<W>, inputs: W::Init) -> Self
    where
        W: Initialize<Id = ()>,
    {
        let mut builder = InputsBuilder::new(interface);
        W::initialize(&mut builder, inputs, &());
        builder.build()
    }

    #[doc(hidden)]
    pub fn into_inner(self) -> HashMap<String, Vec<u8>> {
        self.inner
    }
}
