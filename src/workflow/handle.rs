//! Handle-related logic.

use std::borrow::Cow;

use crate::{
    interface::{AccessError, HandlePath, Interface},
    Decode, Encode,
};

/// Workflow environment containing its elements (channel senders and receivers).
pub trait WorkflowEnv {
    /// Receiver handle in this environment.
    type Receiver<T, C: Encode<T> + Decode<T>>;
    /// Sender handle in this environment.
    type Sender<T, C: Encode<T> + Decode<T>>;

    /// Obtains a receiver handle with the specified ID from this environment.
    ///
    /// # Errors
    ///
    /// Returns an error if a receiver with this ID is missing from the environment.
    fn take_receiver<T, C: Encode<T> + Decode<T>>(
        &mut self,
        path: HandlePath<'_>,
    ) -> Result<Self::Receiver<T, C>, AccessError>;

    /// Obtains a sender handle with the specified ID from this environment.
    ///
    /// # Errors
    ///
    /// Returns an error if a sender with this ID is missing from the environment.
    fn take_sender<T, C: Encode<T> + Decode<T>>(
        &mut self,
        path: HandlePath<'_>,
    ) -> Result<Self::Sender<T, C>, AccessError>;
}

/// [Workflow environment](WorkflowEnv) the interface of which can be summarized
/// as an [`Interface`].
pub trait DescribeEnv: WorkflowEnv {
    /// Returns the interface describing this environment.
    fn interface(&self) -> Cow<'_, Interface>;
}

/// Type with a handle in workflow [environments](WorkflowEnv).
pub trait WithHandle {
    /// Type of the handle in a particular environment.
    type Handle<Env: WorkflowEnv>;
}

/// Type the [handle](WithHandle) of which can be obtained in a certain environment.
///
/// This trait is implemented for elements of the workflow interface, such as channels,
/// and can be derived for workflow types using the corresponding macro.
pub trait TakeHandle<Env: WorkflowEnv>: WithHandle {
    /// Takes a handle from the environment using the specified `id`.
    ///
    /// # Errors
    ///
    /// Returns an error if a handle with this ID is missing from the environment.
    fn take_handle(env: &mut Env, id: HandlePath<'_>) -> Result<Self::Handle<Env>, AccessError>;
}

/// Handle in a particular environment.
pub type InEnv<T, Env> = <T as WithHandle>::Handle<Env>;
