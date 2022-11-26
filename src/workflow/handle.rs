//! Handle-related logic.

#![allow(missing_docs, clippy::missing_errors_doc)]

use std::borrow::Cow;

use crate::{
    interface::{AccessError, Interface},
    Decode, Encode,
};

pub trait WorkflowEnv {
    type Receiver<T, C: Encode<T> + Decode<T>>;
    type Sender<T, C: Encode<T> + Decode<T>>;

    fn take_receiver<T, C: Encode<T> + Decode<T>>(
        &mut self,
        id: &str,
    ) -> Result<Self::Receiver<T, C>, AccessError>;
    fn take_sender<T, C: Encode<T> + Decode<T>>(
        &mut self,
        id: &str,
    ) -> Result<Self::Sender<T, C>, AccessError>;
}

pub trait DescriptiveEnv: WorkflowEnv {
    fn interface(&self) -> Cow<'_, Interface>;
}

/// Type with a handle in a certain environment.
pub trait WithHandle {
    /// ID of the handle, usually a `str` (for named handles) or `()` (for singleton handles).
    type Id: ?Sized;
    /// Type of the handle in a particular environment.
    type Handle<Env: WorkflowEnv>;
}

/// Type with a handle in a certain environment.
///
/// This trait is implemented for elements of the workflow interface, such as channels,
/// and can be derived for workflow types using the corresponding macro.
pub trait TakeHandle<Env: WorkflowEnv>: WithHandle {
    /// Takes a handle from the environment using the specified `id`.
    ///
    /// # Errors
    ///
    /// Returns an error if a handle with this ID is missing from the environment.
    fn take_handle(env: &mut Env, id: &Self::Id) -> Result<Self::Handle<Env>, AccessError>;
}

/// Handle in a particular environment.
pub type Handle<T, Env> = <T as WithHandle>::Handle<Env>;
