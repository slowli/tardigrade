//! Handle-related logic.

use tardigrade_shared::interface::AccessError;

/// Type with a handle in a certain environment.
///
/// This trait is implemented for elements of the workflow interface, such as channels,
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
