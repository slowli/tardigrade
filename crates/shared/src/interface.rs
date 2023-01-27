//! Types related to workflow interface definition.

use serde::{Deserialize, Serialize};

use std::ops;

use crate::{
    handle::{AccessError, AccessErrorKind, Handle, HandleLocation, HandleMap},
    helpers::{HandleMapKey, ReceiverAt, SenderAt},
    path::{HandlePath, HandlePathBuf},
};

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
    /// Worker to connect the sender to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
}

impl Default for SenderSpec {
    fn default() -> Self {
        Self {
            description: String::new(),
            capacity: Self::default_capacity(),
            worker: None,
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
/// # use tardigrade_shared::{handle::ReceiverAt, interface::Interface};
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
    #[serde(default, skip_serializing_if = "HandleMap::is_empty")]
    handles: HandleMap<ReceiverSpec, SenderSpec>,
    #[serde(default)]
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
        Self::try_from_bytes(bytes).unwrap_or_else(|err| panic!("Cannot deserialize spec: {err}"))
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
        key.get_from(&self.handles)
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
                let location: HandleLocation = match spec {
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
                Handle::Receiver(_) => ReceiverAt(path).get_from(handles).map(drop),
                Handle::Sender(_) => SenderAt(path).get_from(handles).map(drop),
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
