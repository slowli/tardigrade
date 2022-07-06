//! Handle-related logic.

use std::{error, fmt};

/// Kind of a channel in a workflow interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelKind {
    /// Inbound channel.
    Inbound,
    /// Outbound channel.
    Outbound,
}

impl fmt::Display for ChannelKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        })
    }
}

/// Kind of a [`HandleError`].
#[derive(Debug)]
#[non_exhaustive]
pub enum HandleErrorKind {
    /// Channel was not registered in the workflow interface.
    Unknown,
    /// An inbound channel was already acquired by the workflow.
    AlreadyAcquired,
    /// Custom error.
    Custom(Box<dyn error::Error + Send + Sync>),
}

impl fmt::Display for HandleErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => formatter.write_str("not registered in the workflow interface"),
            Self::AlreadyAcquired => formatter.write_str("already acquired"),
            Self::Custom(err) => fmt::Display::fmt(err, formatter),
        }
    }
}

impl HandleErrorKind {
    /// Creates a custom error with the provided description.
    pub fn custom(err: impl Into<String>) -> Self {
        Self::Custom(err.into().into())
    }

    /// Adds a location to this error kind, converting it to a [`HandleError`].
    pub fn for_handle(self, location: impl Into<HandleLocation>) -> HandleError {
        HandleError {
            kind: self,
            location: Some(location.into()),
        }
    }
}

/// Location of handle in a workflow [`Interface`](super::Interface).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum HandleLocation {
    /// Channel in the workflow interface.
    Channel {
        /// Channel kind.
        kind: ChannelKind,
        /// Channel name.
        name: String,
    },
    /// Data input with the specified name.
    DataInput(String),
}

impl fmt::Display for HandleLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Channel { kind, name } => {
                write!(formatter, "{} channel `{}`", kind, name)
            }
            Self::DataInput(name) => write!(formatter, "data input `{}`", name),
        }
    }
}

/// Errors that can occur when [taking a handle](TakeHandle) from a certain environment.
#[derive(Debug)]
pub struct HandleError {
    kind: HandleErrorKind,
    location: Option<HandleLocation>,
}

impl fmt::Display for HandleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(id) = &self.location {
            write!(formatter, "cannot take handle for {}: {}", id, self.kind)
        } else {
            write!(formatter, "cannot take handle: {}", self.kind)
        }
    }
}

impl error::Error for HandleError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match &self.kind {
            HandleErrorKind::Custom(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

impl HandleError {
    /// Returns the kind of this error.
    pub fn kind(&self) -> &HandleErrorKind {
        &self.kind
    }

    /// Returns location of this error.
    pub fn location(&self) -> Option<&HandleLocation> {
        self.location.as_ref()
    }
}

/// Type a handle of which can be taken in a certain environment.
///
/// This trait is implemented for elements of the workflow interface (channels, data inputs),
/// and can be derived for workflow types using the corresponding macro.
pub trait TakeHandle<Env> {
    /// ID of the handle, usually a `str` (for named handles) or `()` (for singleton handles).
    type Id: ?Sized;
    /// Type of the handle in a particular environment.
    type Handle;

    /// Takes a handle from the environment using the specified `id`.
    ///
    /// # Errors
    ///
    /// Returns an error if a handle with this ID is missing from the environment.
    fn take_handle(env: &mut Env, id: &Self::Id) -> Result<Self::Handle, HandleError>;
}

/// Handle in a particular environment.
pub type Handle<T, Env> = <T as TakeHandle<Env>>::Handle;
