//! Handle-related logic.

use std::{error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelKind {
    Inbound,
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
    pub fn custom(err: impl Into<String>) -> Self {
        Self::Custom(err.into().into())
    }

    pub fn for_handle(self, handle_id: impl Into<HandleId>) -> HandleError {
        HandleError {
            kind: self,
            handle_id: Some(handle_id.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum HandleId {
    Channel { kind: ChannelKind, name: String },
    DataInput(String),
}

impl fmt::Display for HandleId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Channel { kind, name } => {
                write!(formatter, "{} channel `{}`", kind, name)
            }
            Self::DataInput(name) => write!(formatter, "data input `{}`", name),
        }
    }
}

#[derive(Debug)]
pub struct HandleError {
    kind: HandleErrorKind,
    handle_id: Option<HandleId>,
}

impl fmt::Display for HandleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(id) = &self.handle_id {
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
    pub fn kind(&self) -> &HandleErrorKind {
        &self.kind
    }

    pub fn handle_id(&self) -> Option<&HandleId> {
        self.handle_id.as_ref()
    }
}

pub trait TakeHandle<Env> {
    type Id: ?Sized;
    type Handle;

    fn take_handle(env: &mut Env, id: &Self::Id) -> Result<Self::Handle, HandleError>;
}

pub type Handle<T, Env> = <T as TakeHandle<Env>>::Handle;
