//! Workflow-related types.

use serde::Deserialize;

use std::{collections::HashMap, error, fmt, marker::PhantomData};

pub trait WithHandle<Env> {
    type Handle;
}

pub trait TakeHandle<Env, Id>: WithHandle<Env> {
    fn take_handle(env: &mut Env, id: Id) -> Self::Handle;
}

pub trait PutHandle<Env, Id>: WithHandle<Env> {
    fn put_handle(env: &mut Env, id: Id, handle: Self::Handle);
}

#[derive(Debug, Default, Deserialize)]
pub struct InboundChannelSpec {
    // TODO: options?
}

#[derive(Debug, Default, Deserialize)]
pub struct OutboundChannelSpec {
    // TODO: options?
}

#[derive(Debug, Default, Deserialize)]
pub struct DataInputSpec {
    // TODO: options?
}

#[derive(Deserialize)]
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

    pub fn inbound_channels(&self) -> impl Iterator<Item = (&str, &InboundChannelSpec)> + '_ {
        self.inbound_channels
            .iter()
            .map(|(name, spec)| (name.as_str(), spec))
    }

    pub fn outbound_channel(&self, name: &str) -> Option<&OutboundChannelSpec> {
        self.outbound_channels.get(name)
    }

    pub fn outbound_channels(&self) -> impl Iterator<Item = (&str, &OutboundChannelSpec)> + '_ {
        self.outbound_channels
            .iter()
            .map(|(name, spec)| (name.as_str(), spec))
    }

    pub fn data_input(&self, name: &str) -> Option<&DataInputSpec> {
        self.data_inputs.get(name)
    }

    pub fn data_inputs(&self) -> impl Iterator<Item = (&str, &DataInputSpec)> + '_ {
        self.data_inputs
            .iter()
            .map(|(name, spec)| (name.as_str(), spec))
    }

    fn inputs_builder(&self) -> InputsBuilder {
        InputsBuilder {
            inputs: self
                .data_inputs
                .iter()
                .map(|(name, _)| (name.clone(), None))
                .collect(),
        }
    }
}

impl Interface<()> {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        serde_json::from_slice(bytes)
            .unwrap_or_else(|err| panic!("Cannot deserialize spec: {}", err))
    }

    pub fn downcast<W>(self) -> Result<Interface<W>, W::Error>
    where
        W: ValidateInterface<()>,
    {
        let result = W::validate_interface(&self, ());
        result.map(|()| Interface {
            version: self.version,
            inbound_channels: self.inbound_channels,
            outbound_channels: self.outbound_channels,
            data_inputs: self.data_inputs,
            _workflow: PhantomData,
        })
    }
}

impl<W: PutHandle<InputsBuilder, ()>> Interface<W> {
    pub fn create_inputs(&self, inputs: W::Handle) -> Inputs {
        let mut builder = self.inputs_builder();
        W::put_handle(&mut builder, (), inputs);
        builder.build()
    }
}

#[derive(Debug, Default)]
pub struct ValidationErrors {
    errors: HashMap<String, Box<dyn error::Error + Send + Sync>>,
}

impl ValidationErrors {
    pub fn insert_error<E>(&mut self, name: &str, error: E)
    where
        E: error::Error + Send + Sync + 'static,
    {
        self.errors.insert(name.to_owned(), Box::new(error));
    }

    pub fn into_result(self) -> Result<(), Self> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self)
        }
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.errors.is_empty() {
            formatter.write_str("(no errors)")
        } else {
            formatter.write_str("error validating workflow interface: [")?;
            for (field_name, error) in &self.errors {
                write!(formatter, "{}: {},", field_name, error)?;
            }
            formatter.write_str("]")
        }
    }
}

impl error::Error for ValidationErrors {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.errors
            .values()
            .next()
            .map(|err| err.as_ref() as &(dyn error::Error + 'static))
    }
}

pub trait ValidateInterface<Id> {
    type Error: error::Error + Send + Sync + 'static;

    fn validate_interface(interface: &Interface<()>, id: Id) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone)]
pub struct InputsBuilder {
    inputs: HashMap<String, Option<Vec<u8>>>,
}

impl InputsBuilder {
    #[doc(hidden)]
    pub fn set_raw_input(&mut self, name: &str, raw_data: Vec<u8>) {
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

pub trait ProvideInterface {
    fn interface() -> Interface<Self>;
}

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
