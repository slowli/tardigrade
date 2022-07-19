//! Handle-related logic.

use std::{any::Any, collections::HashMap};

use tardigrade_shared::interface::{AccessError, AccessErrorKind, InterfaceLocation};

/// Type with a handle in a certain environment.
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
    fn take_handle(env: &mut Env, id: &Self::Id) -> Result<Self::Handle, AccessError>;
}

/// Handle in a particular environment.
pub type Handle<T, Env> = <T as TakeHandle<Env>>::Handle;

/// Data that can be put into [`EnvExtensions`].
pub trait ExtendEnv: Any + Send {
    /// Returns a unique string ID of the data.
    fn id(&self) -> String;
}

/// Extensions for a workflow [environment](TakeHandle) allowing to store arbitrary data associated
/// with a workflow.
#[derive(Debug, Default)]
pub struct EnvExtensions {
    inner: HashMap<String, Box<dyn Any + Send>>,
}

impl EnvExtensions {
    /// Inserts an extension into the environment.
    pub fn insert(&mut self, extension: impl ExtendEnv) {
        self.inner.insert(extension.id(), Box::new(extension));
    }

    /// Takes an extension from the environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the extension data is present, but has an incorrect type
    /// (not `T`).
    pub fn take<T: ExtendEnv>(&mut self, id: &str) -> Result<Option<T>, AccessError> {
        let state = match self.inner.remove(id) {
            Some(state) => state,
            None => return Ok(None),
        };
        state
            .downcast::<T>()
            .map(|boxed| Some(*boxed))
            .map_err(|_| {
                AccessErrorKind::custom("type mismatch for extension")
                    .with_location(InterfaceLocation::Extension(id.to_owned()))
            })
    }
}
