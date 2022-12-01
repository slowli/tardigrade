//! Types related to workflow interface definition.

#![allow(missing_docs, clippy::missing_errors_doc)] // FIXME

use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

use std::{error, fmt, ops};

pub use crate::path::{HandlePath, HandlePathBuf, ReceiverAt, SenderAt};

// FIXME: rename to `Handle`?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Resource<Rx, Sx = Rx> {
    Receiver(Rx),
    Sender(Sx),
}

impl<Rx, Sx> Resource<Rx, Sx> {
    #[inline]
    pub fn as_ref(&self) -> Resource<&Rx, &Sx> {
        match self {
            Self::Receiver(rx) => Resource::Receiver(rx),
            Self::Sender(sx) => Resource::Sender(sx),
        }
    }

    #[inline]
    pub fn as_mut(&mut self) -> Resource<&mut Rx, &mut Sx> {
        match self {
            Self::Receiver(rx) => Resource::Receiver(rx),
            Self::Sender(sx) => Resource::Sender(sx),
        }
    }

    #[inline]
    pub fn map_receiver<U>(self, mapping: impl FnOnce(Rx) -> U) -> Resource<U, Sx> {
        match self {
            Self::Receiver(rx) => Resource::Receiver(mapping(rx)),
            Self::Sender(sx) => Resource::Sender(sx),
        }
    }

    #[inline]
    pub fn map_sender<U>(self, mapping: impl FnOnce(Sx) -> U) -> Resource<Rx, U> {
        match self {
            Self::Receiver(rx) => Resource::Receiver(rx),
            Self::Sender(sx) => Resource::Sender(mapping(sx)),
        }
    }

    #[inline]
    fn into_receiver(self) -> Result<Rx, AccessErrorKind> {
        match self {
            Self::Receiver(rx) => Ok(rx),
            Self::Sender(_) => Err(AccessErrorKind::KindMismatch),
        }
    }

    #[inline]
    fn into_sender(self) -> Result<Sx, AccessErrorKind> {
        match self {
            Self::Sender(rx) => Ok(rx),
            Self::Receiver(_) => Err(AccessErrorKind::KindMismatch),
        }
    }
}

pub type HandleMap<Rx, Sx = Rx> = HashMap<HandlePathBuf, Resource<Rx, Sx>>;

/// Kind of a channel half (sender or receiver) in a workflow interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelHalf {
    /// Receiver.
    Receiver = 0,
    /// Sender.
    Sender = 1,
}

impl fmt::Display for ChannelHalf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Receiver => "receiver",
            Self::Sender => "sender",
        })
    }
}

/// Kind of an [`AccessError`].
#[derive(Debug)]
#[non_exhaustive]
pub enum AccessErrorKind {
    /// Channel was not registered in the workflow interface.
    Unknown,
    /// Mismatch between expected anc actual kind of a handle.
    KindMismatch,
    /// Custom error.
    Custom(Box<dyn error::Error + Send + Sync>),
}

impl fmt::Display for AccessErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => formatter.write_str("not registered in the workflow interface"),
            Self::KindMismatch => {
                formatter.write_str("mismatch between expected anc actual kind of a handle")
            }
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
    /// Channel handle in the workflow interface.
    Channel {
        /// Channel handle kind (sender or receiver).
        kind: Option<ChannelHalf>,
        /// Path to the channel handle.
        path: HandlePathBuf,
    },
    /// Arguments supplied to the workflow on creation.
    Args,
}

impl fmt::Display for InterfaceLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Channel { kind, path } => {
                if let Some(kind) = kind {
                    write!(formatter, "{kind} channel `{path}`")
                } else {
                    write!(formatter, "channel `{path}`")
                }
            }
            Self::Args => write!(formatter, "arguments"),
        }
    }
}

impl<'p, P: Into<HandlePath<'p>>> From<P> for InterfaceLocation {
    fn from(path: P) -> Self {
        Self::Channel {
            kind: None,
            path: path.into().to_owned(),
        }
    }
}

impl<'p, P: Into<HandlePath<'p>>> From<ReceiverAt<P>> for InterfaceLocation {
    fn from(path: ReceiverAt<P>) -> Self {
        Self::Channel {
            kind: Some(ChannelHalf::Receiver),
            path: path.0.into().to_owned(),
        }
    }
}

impl<'p, P: Into<HandlePath<'p>>> From<SenderAt<P>> for InterfaceLocation {
    fn from(path: SenderAt<P>) -> Self {
        Self::Channel {
            kind: Some(ChannelHalf::Sender),
            path: path.0.into().to_owned(),
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

pub trait HandleMapKey: Copy + fmt::Debug {
    type Output<Rx, Sx>;

    fn get<Rx, Sx>(self, map: &HandleMap<Rx, Sx>) -> Result<&Self::Output<Rx, Sx>, AccessError>;
    fn get_mut<Rx, Sx>(
        self,
        map: &mut HandleMap<Rx, Sx>,
    ) -> Result<&mut Self::Output<Rx, Sx>, AccessError>;
    fn remove<Rx, Sx>(
        self,
        map: &mut HandleMap<Rx, Sx>,
    ) -> Result<Self::Output<Rx, Sx>, AccessError>;
}

macro_rules! impl_handle_map_key {
    ($target:ty => $output:ty { $(path = $path:tt;)? filter = $($filter:ident)?; }) => {
        impl<'p, P> HandleMapKey for $target
        where
            P: Into<HandlePath<'p>> + Copy + fmt::Debug,
        {
            type Output<Rx, Sx> = $output;

            fn get<Rx, Sx>(
                self,
                map: &HandleMap<Rx, Sx>,
            ) -> Result<&Self::Output<Rx, Sx>, AccessError> {
                let result = map
                    .get(&self$(.$path)?.into())
                    .ok_or_else(|| AccessErrorKind::Unknown)
                    $(.and_then(|handle| handle.as_ref().$filter()))?;
                result.map_err(|err| err.with_location(self))
            }

            fn get_mut<Rx, Sx>(
                self,
                map: &mut HandleMap<Rx, Sx>,
            ) -> Result<&mut Self::Output<Rx, Sx>, AccessError> {
                let result = map
                    .get_mut(&self$(.$path)?.into())
                    .ok_or_else(|| AccessErrorKind::Unknown)
                    $(.and_then(|handle| handle.as_mut().$filter()))?;
                result.map_err(|err| err.with_location(self))
            }

            fn remove<Rx, Sx>(
                self,
                map: &mut HandleMap<Rx, Sx>,
            ) -> Result<Self::Output<Rx, Sx>, AccessError> {
                self.get(map)?;
                let handle = map.remove(&self$(.$path)?.into()).unwrap();
                $(let handle = handle.$filter().unwrap();)?
                Ok(handle)
            }
        }
    };
}

impl_handle_map_key! {
    P => Resource<Rx, Sx> {
        filter = ;
    }
}

impl_handle_map_key! {
    ReceiverAt<P> => Rx {
        path = 0;
        filter = into_receiver;
    }
}

impl_handle_map_key! {
    SenderAt<P> => Sx {
        path = 0;
        filter = into_sender;
    }
}

/// Specification of a channel receiver in the workflow [`Interface`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ReceiverSpec {
    /// Human-readable channel description.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

/// Specification of a channel sender in the workflow [`Interface`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SenderSpec {
    /// Human-readable channel description.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Channel capacity, i.e., the number of messages that can be buffered locally before
    /// the channel needs to be flushed. `None` means unbounded capacity.
    #[serde(default = "SenderSpec::default_capacity")]
    pub capacity: Option<usize>,
}

impl Default for SenderSpec {
    fn default() -> Self {
        Self {
            description: String::new(),
            capacity: Self::default_capacity(),
        }
    }
}

impl SenderSpec {
    #[allow(clippy::unnecessary_wraps)] // required by `serde`
    const fn default_capacity() -> Option<usize> {
        Some(1)
    }

    fn check_compatibility(&self, provided: &Self) -> Result<(), AccessErrorKind> {
        if self.capacity == provided.capacity {
            Ok(())
        } else {
            let expected = self.capacity;
            let provided = provided.capacity;
            let msg = format!(
                "channel sender capacity mismatch: expected {expected:?}, got {provided:?}"
            );
            Err(AccessErrorKind::custom(msg))
        }
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

pub type HandleSpec = Resource<ReceiverSpec, SenderSpec>;

/// Specification of a workflow interface. Contains info about channel senders / receivers,
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
/// let interface: Interface = // ...
/// #     Interface::from_bytes(INTERFACE_BYTES);
///
/// let spec = interface.receiver("commands").unwrap();
/// println!("{}", spec.description);
///
/// assert!(interface
///     .senders()
///     .all(|(_, spec)| spec.capacity == Some(1)));
/// // Indexing is also possible using newtype wrappers from the module
/// let commands = &interface[ReceiverAt("commands")];
/// println!("{}", commands.description);
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Interface {
    #[serde(rename = "v")]
    version: u32,
    #[serde(rename = "in", default, skip_serializing_if = "HandleMap::is_empty")]
    handles: HandleMap<ReceiverSpec, SenderSpec>,
    #[serde(rename = "args", default)]
    args: ArgsSpec,
}

impl Interface {
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

    /// Returns the version of this interface definition.
    #[doc(hidden)]
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Returns spec for a channel receiver, or `None` if a receiver with the specified `path`
    /// is not present in this interface.
    pub fn handle<K: HandleMapKey>(
        &self,
        key: K,
    ) -> Result<&K::Output<ReceiverSpec, SenderSpec>, AccessError> {
        key.get(&self.handles)
    }

    /// Lists all channel receivers in this interface.
    pub fn handles(&self) -> impl ExactSizeIterator<Item = (HandlePath<'_>, &HandleSpec)> + '_ {
        self.handles
            .iter()
            .map(|(path, spec)| (path.as_ref(), spec))
    }

    /// Returns spec for the arguments.
    pub fn args(&self) -> &ArgsSpec {
        &self.args
    }

    /// Checks the compatibility of this *expected* interface against the `provided` interface.
    /// The provided interface may contain more channels than is described by the expected
    /// interface, but not vice versa.
    ///
    /// # Errors
    ///
    /// Returns an error if the provided interface does not match expectations.
    pub fn check_compatibility(&self, provided: &Self) -> Result<(), AccessError> {
        self.handles.iter().try_fold((), |(), (path, spec)| {
            let provided = provided.handle(path)?;
            Self::check_spec_compatibility(spec, provided).map_err(|err| {
                let location: InterfaceLocation = match spec {
                    Resource::Receiver(_) => ReceiverAt(path).into(),
                    Resource::Sender(_) => SenderAt(path).into(),
                };
                err.with_location(location)
            })
        })
    }

    fn check_spec_compatibility(
        expected: &HandleSpec,
        provided: &HandleSpec,
    ) -> Result<(), AccessErrorKind> {
        match (expected, provided) {
            (Resource::Receiver(_), Resource::Receiver(_)) => Ok(()),
            (Resource::Sender(expected), Resource::Sender(provided)) => {
                expected.check_compatibility(provided)
            }
            _ => Err(AccessErrorKind::KindMismatch),
        }
    }
}

impl<K: HandleMapKey> ops::Index<K> for Interface {
    type Output = K::Output<ReceiverSpec, SenderSpec>;

    fn index(&self, index: K) -> &Self::Output {
        self.handle(index).unwrap_or_else(|err| panic!("{err}"))
    }
}

/// Builder of workflow [`Interface`].
#[derive(Debug)]
pub struct InterfaceBuilder {
    interface: Interface,
}

impl InterfaceBuilder {
    /// Creates a builder without any channels specified.
    pub fn new(args: ArgsSpec) -> Self {
        Self {
            interface: Interface {
                args,
                ..Interface::default()
            },
        }
    }

    /// Adds a channel receiver spec to this builder.
    pub fn insert_receiver(&mut self, path: impl Into<HandlePathBuf>, spec: ReceiverSpec) {
        self.interface
            .handles
            .insert(path.into(), Resource::Receiver(spec));
    }

    /// Adds a channel sender spec to this builder.
    pub fn insert_sender(&mut self, path: impl Into<HandlePathBuf>, spec: SenderSpec) {
        self.interface
            .handles
            .insert(path.into(), Resource::Sender(spec));
    }

    /// Builds an interface from this builder.
    pub fn build(self) -> Interface {
        self.interface
    }
}
