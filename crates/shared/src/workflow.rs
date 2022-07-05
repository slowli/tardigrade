//! Workflow-related types.

// TODO: use a newtype instead of `()` for untyped workflows?
// TODO: implement `TakeHandle` for `Interface<()>`

use serde::{Deserialize, Serialize};

use std::{collections::HashMap, error, fmt, marker::PhantomData, ops};

pub trait TakeHandle<Env> {
    type Id: ?Sized;
    type Handle;

    // FIXME: make fallible?
    fn take_handle(env: &mut Env, id: &Self::Id) -> Self::Handle;
}

pub type Handle<T, Env> = <T as TakeHandle<Env>>::Handle;

pub trait Initialize {
    type Init;
    type Id: ?Sized;

    fn initialize(builder: &mut InputsBuilder, init: Self::Init, id: &Self::Id);
}

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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InboundChannelSpec {
    // TODO: options?
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OutboundChannelSpec {
    /// Channel capacity, i.e., number of elements that can be buffered locally before
    /// the channel needs to be flushed. `None` means unbounded capacity.
    #[serde(default = "OutboundChannelSpec::default_capacity")]
    pub capacity: Option<usize>,
}

impl OutboundChannelSpec {
    const fn default_capacity() -> Option<usize> {
        Some(1)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataInputSpec {
    // TODO: options?
}

/// Newtype for indexing data inputs, e.g., in an [`Interface`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataInput<'a>(pub &'a str);

impl fmt::Display for DataInput<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "data input `{}`", self.0)
    }
}

/// Newtype for indexing inbound channels, e.g., in an [`Interface`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Inbound<'a>(pub &'a str);

impl fmt::Display for Inbound<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "inbound channel `{}`", self.0)
    }
}

/// Newtype for indexing outbound channels, e.g., in an [`Interface`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Outbound<'a>(pub &'a str);

impl fmt::Display for Outbound<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "outbound channel `{}`", self.0)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Interface<W: ?Sized> {
    #[serde(rename = "v")]
    version: u32,
    #[serde(rename = "in", default, skip_serializing_if = "HashMap::is_empty")]
    inbound_channels: HashMap<String, InboundChannelSpec>,
    #[serde(rename = "out", default, skip_serializing_if = "HashMap::is_empty")]
    outbound_channels: HashMap<String, OutboundChannelSpec>,
    #[serde(rename = "data", default, skip_serializing_if = "HashMap::is_empty")]
    data_inputs: HashMap<String, DataInputSpec>,
    #[serde(skip, default)]
    _workflow: PhantomData<*const W>,
}

impl<W: ?Sized> fmt::Debug for Interface<W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Interface")
            .field("version", &self.version)
            .field("inbound_channels", &self.inbound_channels)
            .field("outbound_channels", &self.outbound_channels)
            .field("data_inputs", &self.data_inputs)
            .finish()
    }
}

impl<W: ?Sized> Clone for Interface<W> {
    fn clone(&self) -> Self {
        Self {
            version: self.version,
            inbound_channels: self.inbound_channels.clone(),
            outbound_channels: self.outbound_channels.clone(),
            data_inputs: self.data_inputs.clone(),
            _workflow: PhantomData,
        }
    }
}

impl Default for Interface<()> {
    fn default() -> Self {
        Self {
            version: 0,
            inbound_channels: HashMap::new(),
            outbound_channels: HashMap::new(),
            data_inputs: HashMap::new(),
            _workflow: PhantomData,
        }
    }
}

impl<W> Interface<W> {
    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn inbound_channel(&self, name: &str) -> Option<&InboundChannelSpec> {
        self.inbound_channels.get(name)
    }

    pub fn inbound_channels(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, &InboundChannelSpec)> + '_ {
        self.inbound_channels
            .iter()
            .map(|(name, spec)| (name.as_str(), spec))
    }

    pub fn outbound_channel(&self, name: &str) -> Option<&OutboundChannelSpec> {
        self.outbound_channels.get(name)
    }

    pub fn outbound_channels(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, &OutboundChannelSpec)> + '_ {
        self.outbound_channels
            .iter()
            .map(|(name, spec)| (name.as_str(), spec))
    }

    pub fn data_input(&self, name: &str) -> Option<&DataInputSpec> {
        self.data_inputs.get(name)
    }

    pub fn data_inputs(&self) -> impl ExactSizeIterator<Item = (&str, &DataInputSpec)> + '_ {
        self.data_inputs
            .iter()
            .map(|(name, spec)| (name.as_str(), spec))
    }

    pub fn inputs_builder(&self) -> InputsBuilder {
        InputsBuilder {
            inputs: self
                .data_inputs
                .iter()
                .map(|(name, _)| (name.clone(), None))
                .collect(),
        }
    }
}

impl<W: Initialize<Id = ()>> Interface<W> {
    pub fn create_inputs(&self, inputs: W::Init) -> Inputs {
        let mut builder = self.inputs_builder();
        W::initialize(&mut builder, inputs, &());
        builder.build()
    }
}

impl<W> ops::Index<DataInput<'_>> for Interface<W> {
    type Output = DataInputSpec;

    fn index(&self, index: DataInput<'_>) -> &Self::Output {
        self.data_input(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<W> ops::Index<Inbound<'_>> for Interface<W> {
    type Output = InboundChannelSpec;

    fn index(&self, index: Inbound<'_>) -> &Self::Output {
        self.inbound_channel(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<W> ops::Index<Outbound<'_>> for Interface<W> {
    type Output = OutboundChannelSpec;

    fn index(&self, index: Outbound<'_>) -> &Self::Output {
        self.outbound_channel(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl Interface<()> {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        serde_json::from_slice(bytes)
            .unwrap_or_else(|err| panic!("Cannot deserialize spec: {}", err))
    }

    pub fn downcast<W>(self) -> Result<Interface<W>, InterfaceErrors>
    where
        W: for<'a> TakeHandle<InterfaceValidation<'a>, Id = ()>,
    {
        let mut validation = InterfaceValidation::new(&self);
        W::take_handle(&mut validation, &());
        validation.errors.into_result().map(|()| Interface {
            version: self.version,
            inbound_channels: self.inbound_channels,
            outbound_channels: self.outbound_channels,
            data_inputs: self.data_inputs,
            _workflow: PhantomData,
        })
    }
}

impl TakeHandle<InterfaceValidation<'_>> for () {
    type Id = ();
    type Handle = ();

    fn take_handle(_env: &mut InterfaceValidation<'_>, _id: &Self::Id) {
        // validation always succeeds
    }
}

#[derive(Debug, Default)]
pub struct InterfaceErrors {
    errors: Vec<Box<dyn error::Error + Send + Sync>>,
}

impl InterfaceErrors {
    pub fn into_result(self) -> Result<(), Self> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self)
        }
    }
}

impl fmt::Display for InterfaceErrors {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.errors.is_empty() {
            formatter.write_str("(no errors)")
        } else {
            formatter.write_str("error validating workflow interface: [")?;
            for (i, error) in self.errors.iter().enumerate() {
                fmt::Display::fmt(error, formatter)?;
                if i + 1 < self.errors.len() {
                    formatter.write_str(", ")?;
                }
            }
            formatter.write_str("]")
        }
    }
}

impl error::Error for InterfaceErrors {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.errors
            .get(0)
            .map(|err| err.as_ref() as &(dyn error::Error + 'static))
    }
}

#[derive(Debug)]
pub struct InterfaceValidation<'a> {
    interface: &'a Interface<()>,
    errors: InterfaceErrors,
}

impl<'a> InterfaceValidation<'a> {
    fn new(interface: &'a Interface<()>) -> Self {
        Self {
            interface,
            errors: InterfaceErrors::default(),
        }
    }

    pub fn interface(&self) -> &'a Interface<()> {
        self.interface
    }

    pub fn insert_error<E>(&mut self, error: E)
    where
        E: error::Error + Send + Sync + 'static,
    {
        self.errors.errors.push(Box::new(error));
    }
}

/// Builder for workflow [`Inputs`]. Builders can be instantiated using
/// [`Interface::inputs_builder()`].
#[derive(Debug, Clone)]
pub struct InputsBuilder {
    inputs: HashMap<String, Option<Vec<u8>>>,
}

impl InputsBuilder {
    /// Inserts `raw_data` for an input with the specified `name`. It is caller's responsibility
    /// to ensure that raw data has an appropriate format.
    pub fn insert(&mut self, name: &str, raw_data: Vec<u8>) {
        let data_entry = self
            .inputs
            .get_mut(name)
            .unwrap_or_else(|| panic!("Workflow does not have data input `{}`", name));
        *data_entry = Some(raw_data);
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
    /// - Panics if any inputs are not supplied.
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
    pub fn into_inner(self) -> HashMap<String, Vec<u8>> {
        self.inner
    }
}

pub trait GetInterface {
    fn interface() -> Interface<Self>;
}
