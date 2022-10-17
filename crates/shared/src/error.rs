//! Task errors.

use serde::{Deserialize, Serialize};

use std::{borrow::Cow, error, fmt, panic::Location};

/// Location of a panic or error in the workflow code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorLocation {
    /// Name of the file where a panic has occurred.
    pub filename: Cow<'static, str>,
    /// Line number in the file.
    pub line: u32,
    /// Column number on the line.
    pub column: u32,
}

impl fmt::Display for ErrorLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}:{}", self.filename, self.line, self.column)
    }
}

impl ErrorLocation {
    /// Unknown error location.
    pub const UNKNOWN: Self = Self {
        filename: Cow::Borrowed("(unknown)"),
        line: 1,
        column: 1,
    };

    fn new(source: &'static Location<'static>) -> Self {
        Self {
            filename: Cow::Borrowed(source.file()),
            line: source.line(),
            column: source.column(),
        }
    }
}

/// Business error raised by a workflow task.
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskError {
    #[serde(with = "serde_error")]
    cause: Box<dyn error::Error + Send + Sync>,
    location: ErrorLocation,
}

impl fmt::Display for TaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.location, self.cause)
    }
}

impl<E: error::Error + Send + Sync + 'static> From<E> for TaskError {
    #[track_caller]
    fn from(err: E) -> Self {
        Self {
            cause: Box::new(err),
            location: ErrorLocation::new(Location::caller()),
        }
    }
}

impl TaskError {
    /// Creates a new error with the specified message.
    #[track_caller]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            cause: message.into().into(),
            location: ErrorLocation::new(Location::caller()),
        }
    }

    #[doc(hidden)] // used by runtime
    pub fn from_parts(message: String, location: ErrorLocation) -> Self {
        Self {
            cause: message.into(),
            location,
        }
    }

    /// Returns the error cause.
    pub fn cause(&self) -> &(dyn error::Error + 'static) {
        self.cause.as_ref()
    }

    /// Returns the error location.
    pub fn location(&self) -> &ErrorLocation {
        &self.location
    }

    /// Clones this error by replacing the cause with its message.
    #[doc(hidden)]
    #[must_use]
    pub fn clone_boxed(&self) -> Self {
        Self {
            cause: self.cause.to_string().into(),
            location: self.location.clone(),
        }
    }
}

mod serde_error {
    use serde::{
        de::{Error as DeError, Visitor},
        Deserializer, Serializer,
    };

    use std::{error, fmt};

    #[allow(clippy::borrowed_box)] // function signature required by serde
    pub fn serialize<S: Serializer>(
        err: &Box<dyn error::Error + Send + Sync>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&err.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Box<dyn error::Error + Send + Sync>, D::Error> {
        struct StringVisitor;

        impl<'de> Visitor<'de> for StringVisitor {
            type Value = Box<dyn error::Error + Send + Sync>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("error message")
            }

            fn visit_str<E: DeError>(self, value: &str) -> Result<Self::Value, E> {
                Ok(value.to_owned().into())
            }
        }

        deserializer.deserialize_str(StringVisitor)
    }
}

/// Result of executing a workflow task.
pub type TaskResult<T = ()> = Result<T, TaskError>;

/// Errors that can occur when joining a task (i.e., waiting for its completion).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinError {
    /// An error has occurred during task execution.
    Err(TaskError),
    /// The task was aborted.
    Aborted,
}

impl JoinError {
    /// Unwraps this error into an underlying [`TaskError`].
    ///
    /// # Panics
    ///
    /// Panics if this is not a task error.
    pub fn unwrap_task_error(self) -> TaskError {
        match self {
            Self::Err(err) => err,
            Self::Aborted => panic!("not a task error"),
        }
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Aborted => formatter.write_str("task was aborted"),
            Self::Err(err) => write!(formatter, "task failed at {}", err),
        }
    }
}

impl error::Error for JoinError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Err(err) => Some(err.cause()),
            Self::Aborted => None,
        }
    }
}

/// Errors that can occur when sending a message over a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SendError {
    /// The channel is full.
    Full,
    /// The channel is closed.
    Closed,
}

impl fmt::Display for SendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => formatter.write_str("channel is full"),
            Self::Closed => formatter.write_str("channel is closed"),
        }
    }
}

impl error::Error for SendError {}

/// Errors that can occur when spawning a workflow.
// TODO: generalize as a trap?
#[derive(Debug)]
pub struct SpawnError {
    message: String,
}

impl SpawnError {
    /// Creates an error with the specified `message`.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "error has occurred during workflow instantiation: {}",
            self.message
        )
    }
}

impl error::Error for SpawnError {}
