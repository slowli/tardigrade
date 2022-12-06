//! Types related to workflow interface definition.

use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

use std::{
    borrow::{Borrow, BorrowMut},
    error, fmt,
    marker::PhantomData,
    ops,
};

pub use crate::path::{HandlePath, HandlePathBuf, ReceiverAt, SenderAt};

/// Generic handle to a building block of a workflow interface (channel sender or receiver).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Handle<Rx, Sx = Rx> {
    /// Receiver handle.
    Receiver(Rx),
    /// Sender handle.
    Sender(Sx),
}

impl<Rx, Sx> Handle<Rx, Sx> {
    /// Converts a shared reference to this handle to a `Handle` containing the shared reference.
    #[inline]
    pub fn as_ref(&self) -> Handle<&Rx, &Sx> {
        match self {
            Self::Receiver(rx) => Handle::Receiver(rx),
            Self::Sender(sx) => Handle::Sender(sx),
        }
    }

    /// Converts an exclusive reference to this handle to a `Handle` containing
    /// the exclusive reference.
    #[inline]
    pub fn as_mut(&mut self) -> Handle<&mut Rx, &mut Sx> {
        match self {
            Self::Receiver(rx) => Handle::Receiver(rx),
            Self::Sender(sx) => Handle::Sender(sx),
        }
    }

    /// Maps the receiver type in this handle.
    #[inline]
    pub fn map_receiver<U>(self, mapping: impl FnOnce(Rx) -> U) -> Handle<U, Sx> {
        match self {
            Self::Receiver(rx) => Handle::Receiver(mapping(rx)),
            Self::Sender(sx) => Handle::Sender(sx),
        }
    }

    /// Maps the sender type of this handle.
    #[inline]
    pub fn map_sender<U>(self, mapping: impl FnOnce(Sx) -> U) -> Handle<Rx, U> {
        match self {
            Self::Receiver(rx) => Handle::Receiver(rx),
            Self::Sender(sx) => Handle::Sender(mapping(sx)),
        }
    }
}

impl<T> Handle<T> {
    /// Maps both types of this handle provided that they coincide.
    #[inline]
    pub fn map<U>(self, mapping: impl FnOnce(T) -> U) -> Handle<U> {
        match self {
            Self::Receiver(rx) => Handle::Receiver(mapping(rx)),
            Self::Sender(sx) => Handle::Sender(mapping(sx)),
        }
    }

    /// Factors the underlying type from this handle.
    #[inline]
    pub fn factor(self) -> T {
        match self {
            Self::Receiver(value) | Self::Sender(value) => value,
        }
    }
}

/// Map of handles keyed by [owned handle paths](HandlePathBuf).
pub type HandleMap<Rx, Sx = Rx> = HashMap<HandlePathBuf, Handle<Rx, Sx>>;

/// Wrapper for [`UntypedHandles`] allowing accessing them by [`HandleMapKey`].
#[derive(Debug)]
pub struct IndexingHandleMap<T, Rx, Sx> {
    inner: T,
    _ty: PhantomData<fn(Rx, Sx)>,
}

impl<T, Rx, Sx> IndexingHandleMap<T, Rx, Sx>
where
    T: BorrowMut<HandleMap<Rx, Sx>>,
{
    /// Removes an element with the specified index from this handle.
    ///
    /// # Errors
    ///
    /// Returns an error if the element is not present in the handle, or if it has an unexpected
    /// type (e.g., a sender instead of a receiver).
    pub fn remove<K: HandleMapKey>(&mut self, key: K) -> Result<K::Output<Rx, Sx>, AccessError> {
        key.remove(self.inner.borrow_mut())
    }
}

impl<T, Rx, Sx, K: HandleMapKey> ops::Index<K> for IndexingHandleMap<T, Rx, Sx>
where
    T: Borrow<HandleMap<Rx, Sx>>,
{
    type Output = K::Output<Rx, Sx>;

    fn index(&self, index: K) -> &Self::Output {
        index
            .get(self.inner.borrow())
            .unwrap_or_else(|err| panic!("{err}"))
    }
}

impl<T, Rx, Sx, K: HandleMapKey> ops::IndexMut<K> for IndexingHandleMap<T, Rx, Sx>
where
    T: BorrowMut<HandleMap<Rx, Sx>>,
{
    fn index_mut(&mut self, index: K) -> &mut Self::Output {
        index
            .get_mut(self.inner.borrow_mut())
            .unwrap_or_else(|err| panic!("{err}"))
    }
}

/// Converts [`HandleMap`] to the [indexing form](IndexingHandleMap).
pub trait WithIndexing<Rx, Sx>: Borrow<HandleMap<Rx, Sx>> + Sized {
    /// Performs the conversion.
    fn with_indexing(self) -> IndexingHandleMap<Self, Rx, Sx>;
}

impl<T, Rx, Sx> WithIndexing<Rx, Sx> for T
where
    T: Borrow<HandleMap<Rx, Sx>>,
{
    fn with_indexing(self) -> IndexingHandleMap<Self, Rx, Sx> {
        IndexingHandleMap {
            inner: self,
            _ty: PhantomData,
        }
    }
}

/// Kind of a channel half (sender or receiver) in a workflow interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelHalf {
    /// Receiver.
    Receiver,
    /// Sender.
    Sender,
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
    Missing,
    /// Mismatch between expected anc actual kind of a handle.
    KindMismatch,
    /// Custom error.
    Custom(Box<dyn error::Error + Send + Sync>),
}

impl fmt::Display for AccessErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => formatter.write_str("missing handle"),
            Self::KindMismatch => {
                formatter.write_str("mismatch between expected and actual kind of a handle")
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
                    write!(formatter, "channel {kind} `{path}`")
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
            write!(formatter, "[at {location}] {}", self.kind)
        } else {
            write!(formatter, "{}", self.kind)
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

    #[doc(hidden)]
    pub fn into_kind(self) -> AccessErrorKind {
        self.kind
    }

    /// Returns location of this error.
    pub fn location(&self) -> Option<&InterfaceLocation> {
        self.location.as_ref()
    }
}

/// Key type for accessing handles in a named collections, such as a [`HandleMap`]
/// or an [`Interface`].
pub trait HandleMapKey: Into<InterfaceLocation> + Copy + fmt::Debug {
    /// Output of the access operation. Parameterized by receiver and sender types
    /// (e.g., the corresponding specifications for [`Interface`]).
    type Output<Rx, Sx>;

    // This is quite ugly, but we cannot return `HandlePath<'_>` from a method
    // even if we introduce a surrogate lifetime by using the `&self` receiver.
    #[doc(hidden)]
    fn with_path<R>(self, action: impl FnOnce(HandlePath<'_>) -> R) -> R;

    #[doc(hidden)]
    fn from_handle<Rx, Sx>(handle: Handle<Rx, Sx>) -> Option<Self::Output<Rx, Sx>>;

    // We cannot only use `Self::from_handle()` with `<&Rx, &Sx>` type args because
    // we need a reference to be returned in some cases (e.g., for indexing).
    #[doc(hidden)]
    fn from_handle_ref<Rx, Sx>(handle: &Handle<Rx, Sx>) -> Option<&Self::Output<Rx, Sx>>;

    #[doc(hidden)]
    fn from_handle_mut<Rx, Sx>(handle: &mut Handle<Rx, Sx>) -> Option<&mut Self::Output<Rx, Sx>>;

    /// Returns a shared reference from the provided `map` using this key.
    ///
    /// # Errors
    ///
    /// Returns an error if a value with the specified key doesn't exist in the map,
    /// or if it has an unexpected type.
    fn get<Rx, Sx>(self, map: &HandleMap<Rx, Sx>) -> Result<&Self::Output<Rx, Sx>, AccessError> {
        let result = self
            .with_path(|path| map.get(&path))
            .ok_or(AccessErrorKind::Missing);
        result
            .and_then(|handle| Self::from_handle_ref(handle).ok_or(AccessErrorKind::KindMismatch))
            .map_err(|err| err.with_location(self))
    }

    /// Returns an exclusive reference from the provided `map` using this key.
    ///
    /// # Errors
    ///
    /// Returns an error if a value with the specified key doesn't exist in the map,
    /// or if it has an unexpected type.
    fn get_mut<Rx, Sx>(
        self,
        map: &mut HandleMap<Rx, Sx>,
    ) -> Result<&mut Self::Output<Rx, Sx>, AccessError> {
        let result = self
            .with_path(|path| map.get_mut(&path))
            .ok_or(AccessErrorKind::Missing);
        result
            .and_then(|handle| Self::from_handle_mut(handle).ok_or(AccessErrorKind::KindMismatch))
            .map_err(|err| err.with_location(self))
    }

    /// Removes a value from the provided `map` using this key.
    ///
    /// # Errors
    ///
    /// Returns an error if a value with the specified key doesn't exist in the map,
    /// or if it has an unexpected type. In the latter case, the value is *not* removed
    /// from the map (i.e., the type check is performed before any modifications).
    fn remove<Rx, Sx>(
        self,
        map: &mut HandleMap<Rx, Sx>,
    ) -> Result<Self::Output<Rx, Sx>, AccessError> {
        self.get(map)?;
        let handle = self.with_path(|path| map.remove(&path)).unwrap();
        Ok(Self::from_handle(handle).unwrap())
    }
}

impl<'p, P> HandleMapKey for P
where
    P: Into<HandlePath<'p>> + Copy + fmt::Debug,
{
    type Output<Rx, Sx> = Handle<Rx, Sx>;

    fn with_path<R>(self, action: impl FnOnce(HandlePath<'_>) -> R) -> R {
        action(self.into())
    }

    fn from_handle<Rx, Sx>(handle: Handle<Rx, Sx>) -> Option<Self::Output<Rx, Sx>> {
        Some(handle)
    }

    fn from_handle_ref<Rx, Sx>(handle: &Handle<Rx, Sx>) -> Option<&Self::Output<Rx, Sx>> {
        Some(handle)
    }

    fn from_handle_mut<Rx, Sx>(handle: &mut Handle<Rx, Sx>) -> Option<&mut Self::Output<Rx, Sx>> {
        Some(handle)
    }
}

impl<'p, P> HandleMapKey for ReceiverAt<P>
where
    P: 'p + Into<HandlePath<'p>> + Copy + fmt::Debug,
{
    type Output<Rx, Sx> = Rx;

    fn with_path<R>(self, action: impl FnOnce(HandlePath<'_>) -> R) -> R {
        action(self.0.into())
    }

    fn from_handle<Rx, Sx>(handle: Handle<Rx, Sx>) -> Option<Rx> {
        match handle {
            Handle::Receiver(rx) => Some(rx),
            Handle::Sender(_) => None,
        }
    }

    fn from_handle_ref<Rx, Sx>(handle: &Handle<Rx, Sx>) -> Option<&Rx> {
        match handle {
            Handle::Receiver(rx) => Some(rx),
            Handle::Sender(_) => None,
        }
    }

    fn from_handle_mut<Rx, Sx>(handle: &mut Handle<Rx, Sx>) -> Option<&mut Rx> {
        match handle {
            Handle::Receiver(rx) => Some(rx),
            Handle::Sender(_) => None,
        }
    }
}

impl<'p, P> HandleMapKey for SenderAt<P>
where
    P: 'p + Into<HandlePath<'p>> + Copy + fmt::Debug,
{
    type Output<Rx, Sx> = Sx;

    fn with_path<R>(self, action: impl FnOnce(HandlePath<'_>) -> R) -> R {
        action(self.0.into())
    }

    fn from_handle<Rx, Sx>(handle: Handle<Rx, Sx>) -> Option<Sx> {
        match handle {
            Handle::Sender(sx) => Some(sx),
            Handle::Receiver(_) => None,
        }
    }

    fn from_handle_ref<Rx, Sx>(handle: &Handle<Rx, Sx>) -> Option<&Sx> {
        match handle {
            Handle::Sender(sx) => Some(sx),
            Handle::Receiver(_) => None,
        }
    }

    fn from_handle_mut<Rx, Sx>(handle: &mut Handle<Rx, Sx>) -> Option<&mut Sx> {
        match handle {
            Handle::Sender(sx) => Some(sx),
            Handle::Receiver(_) => None,
        }
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

/// Specification for a handle in a workflow [`Interface`].
pub type HandleSpec = Handle<ReceiverSpec, SenderSpec>;

/// Specification of a workflow interface. Contains info about channel senders / receivers,
/// arguments etc.
///
/// # Examples
///
/// ```
/// # use tardigrade_shared::interface::*;
/// # const INTERFACE_BYTES: &[u8] = br#"{
/// #     "v": 0,
/// #     "handles": { "commands": { "receiver": {} } }
/// # }"#;
/// let interface: Interface = // ...
/// #     Interface::from_bytes(INTERFACE_BYTES);
///
/// let spec = interface.handle("commands").unwrap();
/// println!("{spec:?}");
///
/// for (path, spec) in interface.handles() {
///     println!("{path}: {spec:?}");
/// }
/// // Indexing is also possible using newtype wrappers from the module
/// let commands = &interface[ReceiverAt("commands")];
/// println!("{}", commands.description);
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Interface {
    #[serde(rename = "v")]
    version: u32,
    #[serde(
        rename = "handles",
        default,
        skip_serializing_if = "HandleMap::is_empty"
    )]
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

    /// Returns spec for a handle defined in this interface.
    ///
    /// # Errors
    ///
    /// Returns an error if a spec with the specified key doesn't exist in the interfaces,
    /// or if it has an unexpected type.
    pub fn handle<K: HandleMapKey>(
        &self,
        key: K,
    ) -> Result<&K::Output<ReceiverSpec, SenderSpec>, AccessError> {
        key.get(&self.handles)
    }

    /// Lists all handle specifications in this interface.
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
                    Handle::Receiver(_) => ReceiverAt(path).into(),
                    Handle::Sender(_) => SenderAt(path).into(),
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
            (Handle::Receiver(_), Handle::Receiver(_)) => Ok(()),
            (Handle::Sender(expected), Handle::Sender(provided)) => {
                expected.check_compatibility(provided)
            }
            _ => Err(AccessErrorKind::KindMismatch),
        }
    }

    /// Checks that the shape of the provided `handles` corresponds to this interface.
    /// If the `exact` flag is set, the shape match must be exact; otherwise, there might be
    /// extra handles not specified by this interface.
    ///
    /// # Errors
    ///
    /// Returns an error on shape mismatch.
    pub fn check_shape<Rx, Sx>(
        &self,
        handles: &HandleMap<Rx, Sx>,
        exact: bool,
    ) -> Result<(), AccessError> {
        self.handles
            .iter()
            .try_fold((), |(), (path, spec)| match spec {
                Handle::Receiver(_) => ReceiverAt(path).get(handles).map(drop),
                Handle::Sender(_) => SenderAt(path).get(handles).map(drop),
            })?;

        if exact {
            for path in handles.keys() {
                if !self.handles.contains_key(path) {
                    let err = AccessErrorKind::custom("extra handle");
                    return Err(err.with_location(path));
                }
            }
        }
        Ok(())
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
            .insert(path.into(), Handle::Receiver(spec));
    }

    /// Adds a channel sender spec to this builder.
    pub fn insert_sender(&mut self, path: impl Into<HandlePathBuf>, spec: SenderSpec) {
        self.interface
            .handles
            .insert(path.into(), Handle::Sender(spec));
    }

    /// Builds an interface from this builder.
    pub fn build(self) -> Interface {
        self.interface
    }
}
