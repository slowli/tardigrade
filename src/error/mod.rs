//! Task errors.

use serde::{Deserialize, Serialize};

use std::{borrow::Cow, error, fmt, panic::Location};

#[cfg(test)]
mod tests;

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

/// Additional context for a [`TaskError`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorContext {
    message: String,
    location: ErrorLocation,
}

impl ErrorContext {
    /// Returns human-readable contextual message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the source code location associated with this context.
    pub fn location(&self) -> &ErrorLocation {
        &self.location
    }
}

impl fmt::Display for ErrorContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.location, self.message)
    }
}

/// Business error raised by a workflow task.
///
/// A `TaskError` can be created [`From`] any type implementing the [`Error`](error::Error) trait,
/// or using [`Self::new()`]. Another way to create errors is by using the [`ErrorContextExt`]
/// trait implemented for [`Result`]s and [`Option`]s (just like the `Context` trait from
/// [`anyhow`]).
///
/// A `TaskError` encapsulates at least the caller source code [location](Self::location()) and
/// the underlying [cause](Self::cause()). If the error crosses the host-workflow boundary,
/// the cause if just a string message. Otherwise (i.e., within a workflow), it is the original
/// error cause provided when creating it; thus, it can be downcast etc.
///
/// An error is associated with zero or more additional [`ErrorContext`]s.
/// Contexts can be added via [`Self::context()`], or using the [`ErrorContextExt`] extension
/// trait.
///
/// [`anyhow`]: https://crates.io/crates/anyhow
///
/// # Examples
///
/// ```
/// # use tardigrade::task::TaskError;
/// let err = TaskError::new("(error message)");
/// let err = err.context("(more high-level context)");
///
/// println!("{err}"); // one-line error message
/// println!("{err:#}"); // multiline message including all contexts
///
/// assert_eq!(err.cause().to_string(), "(error message)");
/// assert_eq!(err.contexts().len(), 1);
/// assert_eq!(err.contexts()[0].message(), "(more high-level context)");
/// ```
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskError {
    #[serde(with = "serde_error")]
    cause: Box<dyn error::Error + Send + Sync>,
    location: ErrorLocation,
    contexts: Vec<ErrorContext>,
}

impl fmt::Display for TaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if formatter.alternate() && !self.contexts.is_empty() {
            let (last_context, contexts) = self.contexts.split_last().unwrap();
            writeln!(
                formatter,
                "{}: {}",
                last_context.location, last_context.message
            )?;
            writeln!(formatter, "Caused by:")?;
            for context in contexts.iter().rev() {
                writeln!(formatter, "    {context}")?;
            }
            writeln!(formatter, "    {}: {}", self.location, self.cause)
        } else if let Some(context) = self.contexts.last() {
            if self.contexts.len() == 1 {
                write!(formatter, "{context} (+1 cause)")
            } else {
                write!(formatter, "{context} (+{} cause)", self.contexts.len())
            }
        } else {
            write!(formatter, "{}: {}", self.location, self.cause)
        }
    }
}

impl<E: error::Error + Send + Sync + 'static> From<E> for TaskError {
    #[track_caller]
    fn from(err: E) -> Self {
        Self {
            cause: Box::new(err),
            location: ErrorLocation::new(Location::caller()),
            contexts: Vec::new(),
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
            contexts: Vec::new(),
        }
    }

    #[doc(hidden)] // used by runtime
    pub fn from_parts(message: String, location: ErrorLocation) -> Self {
        Self {
            cause: message.into(),
            location,
            contexts: Vec::new(),
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

    /// Returns the additional context(s) associated with this error.
    pub fn contexts(&self) -> &[ErrorContext] {
        &self.contexts
    }

    /// Adds an additional context to this error.
    #[track_caller]
    #[must_use]
    pub fn context(mut self, message: impl Into<String>) -> Self {
        self.contexts.push(ErrorContext {
            message: message.into(),
            location: ErrorLocation::new(Location::caller()),
        });
        self
    }

    #[doc(hidden)] // used by runtime
    pub fn push_context_from_parts(&mut self, message: String, location: ErrorLocation) {
        self.contexts.push(ErrorContext { message, location });
    }

    /// Clones this error by replacing the cause with its message.
    #[doc(hidden)] // used by runtime
    #[must_use]
    pub fn clone_boxed(&self) -> Self {
        Self {
            cause: self.cause.to_string().into(),
            location: self.location.clone(),
            contexts: self.contexts.clone(),
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

/// Extension trait to convert [`Result`]s and [`Option`]s to [`TaskResult`]s.
///
/// # Examples
///
/// ```
/// # use std::fmt;
/// # use tardigrade::task::{ErrorContextExt, TaskResult};
/// #[derive(Debug)]
/// pub struct CustomError { /* ... */ }
///
/// # impl fmt::Display for CustomError {
/// #     fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
/// #         formatter.write_str("custom error")
/// #     }
/// # }
/// impl std::error::Error for CustomError {}
///
/// // Fallible function (can be defined either in the workflow code
/// // or externally).
/// fn fallible(value: u32) -> Result<u32, CustomError> {
///     // snipped...
/// #   Err(CustomError {})
/// }
///
/// // Part of the workflow logic.
/// fn computation(value: u32) -> TaskResult {
///     let output = fallible(value).with_context(|| {
///         format!("computations for {value}")
///     })?;
///     // other logic...
///     Ok(())
/// }
///
/// fn high_level_logic() -> TaskResult {
///     computation(42).context("high-level logic")?;
///     // other logic...
///     Ok(())
/// }
/// ```
// **NB.** Implementations must avoid using `Result::map_err()` etc. since they lead to incorrect
// caller tracking.
#[allow(clippy::missing_errors_doc)] // doesn't make sense semantically
pub trait ErrorContextExt<T> {
    /// Adds additional context to an error.
    #[track_caller]
    fn context<C: Into<String>>(self, context: C) -> TaskResult<T>;

    /// Adds additional, lazily evaluated context to an error.
    #[track_caller]
    fn with_context<C, F>(self, lazy_context: F) -> TaskResult<T>
    where
        C: Into<String>,
        F: FnOnce() -> C;
}

impl<T, E> ErrorContextExt<T> for Result<T, E>
where
    E: error::Error + Send + Sync + 'static,
{
    fn context<C: Into<String>>(self, context: C) -> TaskResult<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(TaskError::from(err).context(context)),
        }
    }

    fn with_context<C, F>(self, lazy_context: F) -> TaskResult<T>
    where
        C: Into<String>,
        F: FnOnce() -> C,
    {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(TaskError::from(err).context(lazy_context())),
        }
    }
}

impl<T> ErrorContextExt<T> for TaskResult<T> {
    fn context<C: Into<String>>(self, context: C) -> TaskResult<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(err.context(context)),
        }
    }

    fn with_context<C, F>(self, lazy_context: F) -> TaskResult<T>
    where
        C: Into<String>,
        F: FnOnce() -> C,
    {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(err.context(lazy_context())),
        }
    }
}

impl<T> ErrorContextExt<T> for Option<T> {
    fn context<C: Into<String>>(self, context: C) -> TaskResult<T> {
        match self {
            Some(value) => Ok(value),
            None => Err(TaskError::new(context)),
        }
    }

    fn with_context<C, F>(self, lazy_context: F) -> TaskResult<T>
    where
        C: Into<String>,
        F: FnOnce() -> C,
    {
        match self {
            Some(value) => Ok(value),
            None => Err(TaskError::new(lazy_context())),
        }
    }
}

/// Errors that can occur when joining a task or a previously spawned workflow (i.e.,
/// waiting for its completion).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinError {
    /// An error has occurred during task / workflow execution.
    Err(TaskError),
    /// The task / workflow was aborted.
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

/// Errors generated on the host side and sent to a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostError {
    message: String,
}

impl HostError {
    /// Creates an error with the specified `message`.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for HostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl error::Error for HostError {}
