//! Workflow-related types.

// TODO: use a newtype instead of `()` for untyped workflows?

use serde::{Deserialize, Serialize};

use std::{collections::HashMap, fmt, marker::PhantomData, ops};

mod handle;

pub use self::handle::{
    ChannelKind, Handle, HandleError, HandleErrorKind, HandleLocation, TakeHandle,
};

/// Signals that a type can participate in initializing a workflow.
///
/// This trait is sort of dual to [`TakeHandle`].
pub trait Initialize {
    /// Type of the initializer.
    type Init;
    /// ID determining where to put the initializer.
    type Id: ?Sized;

    /// Puts `init` into the `builder` using the provided "location" `id`.
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

/// Specification of an inbound channel in the workflow [`Interface`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InboundChannelSpec {
    /// Human-readable channel description.
    pub description: String,
}

/// Specification of an outbound workflow channel in the workflow [`Interface`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OutboundChannelSpec {
    /// Human-readable channel description.
    pub description: String,
    /// Channel capacity, i.e., number of elements that can be buffered locally before
    /// the channel needs to be flushed. `None` means unbounded capacity.
    #[serde(default = "OutboundChannelSpec::default_capacity")]
    pub capacity: Option<usize>,
}

impl OutboundChannelSpec {
    #[allow(clippy::unnecessary_wraps)] // required by `serde`
    const fn default_capacity() -> Option<usize> {
        Some(1)
    }
}

/// Specification of a data input in the workflow [`Interface`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataInputSpec {
    /// Human-readable data input description.
    pub description: String,
}

/// Newtype for indexing data inputs, e.g., in an [`Interface`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataInput<'a>(pub &'a str);

impl fmt::Display for DataInput<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "data input `{}`", self.0)
    }
}

impl From<DataInput<'_>> for HandleLocation {
    fn from(data_input: DataInput<'_>) -> Self {
        Self::DataInput(data_input.0.to_owned())
    }
}

/// Newtype for indexing inbound channels, e.g., in an [`Interface`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InboundChannel<'a>(pub &'a str);

impl fmt::Display for InboundChannel<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "inbound channel `{}`", self.0)
    }
}

impl From<InboundChannel<'_>> for HandleLocation {
    fn from(channel: InboundChannel<'_>) -> Self {
        Self::Channel {
            kind: ChannelKind::Inbound,
            name: channel.0.to_owned(),
        }
    }
}

/// Newtype for indexing outbound channels, e.g., in an [`Interface`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OutboundChannel<'a>(pub &'a str);

impl fmt::Display for OutboundChannel<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "outbound channel `{}`", self.0)
    }
}

impl From<OutboundChannel<'_>> for HandleLocation {
    fn from(channel: OutboundChannel<'_>) -> Self {
        Self::Channel {
            kind: ChannelKind::Outbound,
            name: channel.0.to_owned(),
        }
    }
}

/// Specification of a workflow interface. Contains info about inbound / outbound channels,
/// data inputs etc.
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
    /// Returns the version of this interface definition.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Returns spec for an inbound channel, or `None` if the channel with the specified `name`
    /// is not present in this interface.
    pub fn inbound_channel(&self, name: &str) -> Option<&InboundChannelSpec> {
        self.inbound_channels.get(name)
    }

    /// Lists all inbound channels in this interface.
    pub fn inbound_channels(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, &InboundChannelSpec)> + '_ {
        self.inbound_channels
            .iter()
            .map(|(name, spec)| (name.as_str(), spec))
    }

    /// Returns spec for an outbound channel, or `None` if the channel with the specified `name`
    /// is not present in this interface.
    pub fn outbound_channel(&self, name: &str) -> Option<&OutboundChannelSpec> {
        self.outbound_channels.get(name)
    }

    /// Lists all outbound channels in this interface.
    pub fn outbound_channels(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, &OutboundChannelSpec)> + '_ {
        self.outbound_channels
            .iter()
            .map(|(name, spec)| (name.as_str(), spec))
    }

    /// Returns spec for a data input, or `None` if the data input with the specified `name`
    /// is not present in this interface.
    pub fn data_input(&self, name: &str) -> Option<&DataInputSpec> {
        self.data_inputs.get(name)
    }

    /// Lists all data inputs in this interface.
    pub fn data_inputs(&self) -> impl ExactSizeIterator<Item = (&str, &DataInputSpec)> + '_ {
        self.data_inputs
            .iter()
            .map(|(name, spec)| (name.as_str(), spec))
    }

    /// Returns an [`InputsBuilder`] that can be used to supply [`Inputs`] to create workflow
    /// instances.
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
    #[doc(hidden)]
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

impl<W> ops::Index<InboundChannel<'_>> for Interface<W> {
    type Output = InboundChannelSpec;

    fn index(&self, index: InboundChannel<'_>) -> &Self::Output {
        self.inbound_channel(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<W> ops::Index<OutboundChannel<'_>> for Interface<W> {
    type Output = OutboundChannelSpec;

    fn index(&self, index: OutboundChannel<'_>) -> &Self::Output {
        self.outbound_channel(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl Interface<()> {
    /// Parses interface definition from `bytes`.
    ///
    /// Currently, this assumes that the definition is JSON-encoded, but this should be considered
    /// an implementation detail.
    ///
    /// # Panics
    ///
    /// - Panics if `bytes` do not represent a valid interface definition.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        serde_json::from_slice(bytes)
            .unwrap_or_else(|err| panic!("Cannot deserialize spec: {}", err))
    }

    /// Tries to downcast this interface to a particular workflow.
    ///
    /// # Errors
    ///
    /// - Returns an error if there is a mismatch between the interface of the workflow type
    ///   and this interface, e.g., if the workflow type relies on a channel / data input
    ///   not present in this interface.
    pub fn downcast<W>(self) -> Result<Interface<W>, HandleError>
    where
        W: for<'a> TakeHandle<&'a Interface<()>, Id = ()>,
    {
        W::take_handle(&mut &self, &())?;
        Ok(Interface {
            version: self.version,
            inbound_channels: self.inbound_channels,
            outbound_channels: self.outbound_channels,
            data_inputs: self.data_inputs,
            _workflow: PhantomData,
        })
    }
}

impl TakeHandle<&Interface<()>> for () {
    type Id = ();
    type Handle = ();

    fn take_handle(_env: &mut &Interface<()>, _id: &Self::Id) -> Result<(), HandleError> {
        Ok(()) // validation always succeeds
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
    ///
    /// # Panics
    ///
    /// - Panics if `name` does not correspond to a data input in the workflow interface.
    ///   This can be checked beforehand using [`Self::requires_input()`].
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

/// Allows obtaining an interface from the workflow.
pub trait GetInterface {
    /// Obtains the workflow interface.
    fn interface() -> Interface<Self>;
}
