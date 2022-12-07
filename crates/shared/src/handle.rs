//! [`Handle`] management.

use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

use std::{error, fmt};

pub use crate::{
    helpers::{HandleMapKey, IndexingHandleMap, ReceiverAt, SenderAt, WithIndexing},
    path::{HandlePath, HandlePathBuf},
};

// region:Handle and HandleMap

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

impl fmt::Display for Handle<()> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Receiver(()) => "receiver",
            Self::Sender(()) => "sender",
        })
    }
}

/// Map of handles keyed by [owned handle paths](HandlePathBuf).
pub type HandleMap<Rx, Sx = Rx> = HashMap<HandlePathBuf, Handle<Rx, Sx>>;

// endregion:Handle and HandleMap
// region:AccessError

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
    pub fn with_location(self, location: impl Into<HandleLocation>) -> AccessError {
        AccessError {
            kind: self,
            location: Some(location.into()),
        }
    }
}

/// Location of a [`Handle`] in a [map](HandleMap).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HandleLocation {
    /// Channel handle kind (sender or receiver).
    pub kind: Option<Handle<()>>,
    /// Path to the channel handle.
    pub path: HandlePathBuf,
}

impl fmt::Display for HandleLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = &self.path;
        if let Some(kind) = &self.kind {
            write!(formatter, "channel {kind} `{path}`")
        } else {
            write!(formatter, "channel `{path}`")
        }
    }
}

impl<'p, P: Into<HandlePath<'p>>> From<P> for HandleLocation {
    fn from(path: P) -> Self {
        Self {
            kind: None,
            path: path.into().to_owned(),
        }
    }
}

impl<'p, P: Into<HandlePath<'p>>> From<ReceiverAt<P>> for HandleLocation {
    fn from(path: ReceiverAt<P>) -> Self {
        Self {
            kind: Some(Handle::Receiver(())),
            path: path.0.into().to_owned(),
        }
    }
}

impl<'p, P: Into<HandlePath<'p>>> From<SenderAt<P>> for HandleLocation {
    fn from(path: SenderAt<P>) -> Self {
        Self {
            kind: Some(Handle::Sender(())),
            path: path.0.into().to_owned(),
        }
    }
}

/// Errors that can occur when accessing an element of a workflow [`Interface`].
#[derive(Debug)]
pub struct AccessError {
    kind: AccessErrorKind,
    location: Option<HandleLocation>,
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
    pub fn location(&self) -> Option<&HandleLocation> {
        self.location.as_ref()
    }
}
// endregion:AccessError
