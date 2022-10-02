//! Types related to workflow interface definition.

use serde::{Deserialize, Serialize};

use std::{collections::HashMap, error, fmt, marker::PhantomData, ops};

use crate::abi::{FromWasmError, TryFromWasm};

/// Kind of a channel in a workflow interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ChannelKind {
    /// Inbound channel.
    Inbound = 0,
    /// Outbound channel.
    Outbound = 1,
}

impl fmt::Display for ChannelKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        })
    }
}

impl TryFromWasm for ChannelKind {
    type Abi = i32;

    fn into_abi_in_wasm(self) -> Self::Abi {
        self as i32
    }

    fn try_from_wasm(abi: Self::Abi) -> Result<Self, FromWasmError> {
        match abi {
            0 => Ok(Self::Inbound),
            1 => Ok(Self::Outbound),
            _ => Err(FromWasmError::new("unexpected `ChannelKind` value")),
        }
    }
}

/// Kind of an [`AccessError`].
#[derive(Debug)]
#[non_exhaustive]
pub enum AccessErrorKind {
    /// Channel was not registered in the workflow interface.
    Unknown,
    /// Custom error.
    Custom(Box<dyn error::Error + Send + Sync>),
}

impl fmt::Display for AccessErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => formatter.write_str("not registered in the workflow interface"),
            Self::Custom(err) => fmt::Display::fmt(err, formatter),
        }
    }
}

impl AccessErrorKind {
    /// Creates a custom error with the provided description.
    pub fn custom(err: impl Into<String>) -> Self {
        Self::Custom(err.into().into())
    }

    /// Adds a location to this error kind, converting it to an [`AccessError`].
    pub fn with_location(self, location: impl Into<InterfaceLocation>) -> AccessError {
        AccessError {
            kind: self,
            location: Some(location.into()),
        }
    }
}

/// Location in a workflow [`Interface`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum InterfaceLocation {
    /// Channel in the workflow interface.
    Channel {
        /// Channel kind.
        kind: ChannelKind,
        /// Channel name.
        name: String,
    },
    /// Arguments supplied to the workflow on creation.
    Args,
}

impl fmt::Display for InterfaceLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Channel { kind, name } => {
                write!(formatter, "{} channel `{}`", kind, name)
            }
            Self::Args => write!(formatter, "arguments"),
        }
    }
}

/// Errors that can occur when accessing an element of a workflow [`Interface`].
#[derive(Debug)]
pub struct AccessError {
    kind: AccessErrorKind,
    location: Option<InterfaceLocation>,
}

impl fmt::Display for AccessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(location) = &self.location {
            write!(
                formatter,
                "cannot take handle for {}: {}",
                location, self.kind
            )
        } else {
            write!(formatter, "cannot take handle: {}", self.kind)
        }
    }
}

impl From<AccessErrorKind> for AccessError {
    fn from(kind: AccessErrorKind) -> Self {
        Self {
            kind,
            location: None,
        }
    }
}

impl error::Error for AccessError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match &self.kind {
            AccessErrorKind::Custom(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

impl AccessError {
    /// Returns the kind of this error.
    pub fn kind(&self) -> &AccessErrorKind {
        &self.kind
    }

    /// Returns location of this error.
    pub fn location(&self) -> Option<&InterfaceLocation> {
        self.location.as_ref()
    }
}

/// Specification of an inbound channel in the workflow [`Interface`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InboundChannelSpec {
    /// Human-readable channel description.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

/// Specification of an outbound workflow channel in the workflow [`Interface`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OutboundChannelSpec {
    /// Human-readable channel description.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Channel capacity, i.e., the number of messages that can be buffered locally before
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

/// Specification of the arguments in the workflow [`Interface`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ArgsSpec {
    /// Human-readable arguments description.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

/// Newtype for indexing inbound channels, e.g., in an [`Interface`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InboundChannel<'a>(pub &'a str);

impl fmt::Display for InboundChannel<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "inbound channel `{}`", self.0)
    }
}

impl From<InboundChannel<'_>> for InterfaceLocation {
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

impl From<OutboundChannel<'_>> for InterfaceLocation {
    fn from(channel: OutboundChannel<'_>) -> Self {
        Self::Channel {
            kind: ChannelKind::Outbound,
            name: channel.0.to_owned(),
        }
    }
}

/// Specification of a workflow interface. Contains info about inbound / outbound channels,
/// arguments etc.
///
/// # Examples
///
/// ```
/// # use tardigrade_shared::interface::*;
/// # const INTERFACE_BYTES: &[u8] = br#"{
/// #     "v": 0,
/// #     "in": { "commands": {} },
/// #     "out": { "events": {} }
/// # }"#;
/// let interface: Interface<()> = // ...
/// #     Interface::from_bytes(INTERFACE_BYTES);
///
/// let spec = interface.inbound_channel("commands").unwrap();
/// println!("{}", spec.description);
///
/// assert!(interface
///     .outbound_channels()
///     .all(|(_, spec)| spec.capacity == Some(1)));
/// // Indexing is also possible using newtype wrappers from the module
/// let commands = &interface[InboundChannel("commands")];
/// println!("{}", commands.description);
/// ```
#[derive(Serialize, Deserialize)]
pub struct Interface<W: ?Sized> {
    #[serde(rename = "v")]
    version: u32,
    #[serde(rename = "in", default, skip_serializing_if = "HashMap::is_empty")]
    inbound_channels: HashMap<String, InboundChannelSpec>,
    #[serde(rename = "out", default, skip_serializing_if = "HashMap::is_empty")]
    outbound_channels: HashMap<String, OutboundChannelSpec>,
    #[serde(rename = "args", default)]
    args: ArgsSpec,
    #[serde(skip, default)]
    _workflow: PhantomData<fn(W)>,
}

impl<W: ?Sized> fmt::Debug for Interface<W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Interface")
            .field("version", &self.version)
            .field("inbound_channels", &self.inbound_channels)
            .field("outbound_channels", &self.outbound_channels)
            .field("args", &self.args)
            .finish()
    }
}

impl<W: ?Sized> Clone for Interface<W> {
    fn clone(&self) -> Self {
        Self {
            version: self.version,
            inbound_channels: self.inbound_channels.clone(),
            outbound_channels: self.outbound_channels.clone(),
            args: self.args.clone(),
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
            args: ArgsSpec::default(),
            _workflow: PhantomData,
        }
    }
}

impl<W> Interface<W> {
    /// Returns the version of this interface definition.
    #[doc(hidden)]
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Returns spec for an inbound channel, or `None` if a channel with the specified `name`
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

    /// Returns spec for an outbound channel, or `None` if a channel with the specified `name`
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

    /// Returns spec for the arguments.
    pub fn args(&self) -> &ArgsSpec {
        &self.args
    }

    /// Erases the type of this interface.
    pub fn erase(self) -> Interface<()> {
        Interface {
            version: self.version,
            inbound_channels: self.inbound_channels,
            outbound_channels: self.outbound_channels,
            args: self.args,
            _workflow: PhantomData,
        }
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
    /// # Errors
    ///
    /// Returns an error if `bytes` do not represent a valid interface definition.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }

    /// Version of [`Self::try_from_bytes()`] that panics on error.
    ///
    /// # Panics
    ///
    /// Panics if `bytes` do not represent a valid interface definition.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self::try_from_bytes(bytes).unwrap_or_else(|err| panic!("Cannot deserialize spec: {}", err))
    }

    /// Serializes this interface.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("failed serializing `Interface`")
    }

    /// Tries to downcast this interface to a particular workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a mismatch between the interface of the workflow type
    /// and this interface, e.g., if the workflow type relies on a channel
    /// not present in this interface.
    pub fn downcast<W>(self) -> Result<Interface<W>, AccessError>
    where
        W: ValidateInterface<Id = ()>,
    {
        W::validate_interface(&self, &())?;
        Ok(Interface {
            version: self.version,
            inbound_channels: self.inbound_channels,
            outbound_channels: self.outbound_channels,
            args: self.args,
            _workflow: PhantomData,
        })
    }
}

/// Validates a workflow interface.
pub trait ValidateInterface {
    /// ID of the type within the interface, usually a `str` (for named handles)
    /// or `()` (for singleton handles).
    type Id: ?Sized;

    /// Performs validation.
    ///
    /// # Errors
    ///
    /// Returns an error if the interface is invalid.
    fn validate_interface(interface: &Interface<()>, id: &Self::Id) -> Result<(), AccessError>;
}

impl ValidateInterface for () {
    type Id = ();

    fn validate_interface(_interface: &Interface<()>, _id: &()) -> Result<(), AccessError> {
        Ok(()) // validation always succeeds
    }
}
