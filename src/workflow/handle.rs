//! Handle-related logic.

use std::{convert::Infallible, marker::PhantomData};

use super::untyped::UntypedHandles;
use crate::{
    handle::{AccessError, Handle, HandlePath, HandlePathBuf},
    Codec,
};

/// Conversion from a [raw handle](HandleFormat::RawReceiver).
pub trait TryFromRaw<T>: Sized {
    /// Conversion error.
    type Error: ToString;

    /// Attempts to perform the conversion.
    ///
    /// # Errors
    ///
    /// Returns an error if conversion fails (e.g., due to dynamically checked type mismatch).
    fn try_from_raw(raw: T) -> Result<Self, Self::Error>;
}

impl TryFromRaw<()> for () {
    type Error = Infallible;

    fn try_from_raw(_raw: ()) -> Result<Self, Self::Error> {
        Ok(())
    }
}

/// Conversion into a [raw handle](HandleFormat::RawReceiver).
pub trait IntoRaw<T> {
    /// Performs the conversion.
    fn into_raw(self) -> T;
}

impl IntoRaw<()> for () {
    fn into_raw(self) {
        // do nothing
    }
}

/// Format of handles (senders and receivers).
pub trait HandleFormat {
    /// Raw receiver handle.
    type RawReceiver;
    /// Receiver handle in this format.
    type Receiver<T, C: Codec<T>>: IntoRaw<Self::RawReceiver> + TryFromRaw<Self::RawReceiver>;
    /// Raw sender handle.
    type RawSender;
    /// Sender handle in this format.
    type Sender<T, C: Codec<T>>: IntoRaw<Self::RawSender> + TryFromRaw<Self::RawSender>;
}

impl HandleFormat for () {
    type RawReceiver = ();
    type Receiver<T, C: Codec<T>> = ();
    type RawSender = ();
    type Sender<T, C: Codec<T>> = ();
}

/// Inverse wrapper for a [`HandleFormat`] that swaps handles for senders and receivers.
#[derive(Debug)]
pub struct Inverse<Fmt>(PhantomData<Fmt>);

impl<Fmt: HandleFormat> HandleFormat for Inverse<Fmt> {
    type RawReceiver = Fmt::RawSender;
    type Receiver<T, C: Codec<T>> = Fmt::Sender<T, C>;
    type RawSender = Fmt::RawReceiver;
    type Sender<T, C: Codec<T>> = Fmt::Receiver<T, C>;
}

/// Collection of handles in a certain format that can be taken from.
pub trait TakeHandles<Fmt: HandleFormat> {
    /// Takes a raw receiver handle at the specified `path` from this collection.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no receiver with the specified `path`.
    fn take_receiver(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawReceiver, AccessError>;

    /// Takes a raw sender handle at the specified `path` from this collection.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no sender with the specified `path`.
    fn take_sender(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawSender, AccessError>;

    /// Drains this collection into untyped handles.
    fn drain(&mut self) -> UntypedHandles<Fmt>;
}

/// Accumulator of handles in a certain format.
pub trait BuildHandles<Fmt: HandleFormat> {
    /// Inserts a handle into this accumulator.
    fn insert_handle(
        &mut self,
        path: HandlePathBuf,
        handle: Handle<Fmt::RawReceiver, Fmt::RawSender>,
    );

    /// Inserts all `handles` into this accumulator.
    fn insert_handles(&mut self, path: HandlePath<'_>, handles: UntypedHandles<Fmt>) {
        default_insert_handles(self, path, handles);
    }
}

pub(super) fn default_insert_handles<Fmt, T>(
    target: &mut T,
    path: HandlePath<'_>,
    handles: UntypedHandles<Fmt>,
) where
    Fmt: HandleFormat,
    T: BuildHandles<Fmt> + ?Sized,
{
    for (suffix, handle) in handles {
        let mut path = path.to_owned();
        path.extend(suffix);
        target.insert_handle(path, handle);
    }
}

/// Type with a handle in workflow [environments](WorkflowEnv).
pub trait WithHandle {
    /// Type of the handle in a particular environment.
    type Handle<Fmt: HandleFormat>;

    /// Produces the handle from an `untyped` collection of handles.
    ///
    /// # Errors
    ///
    /// Returns an error if the collection does not have the necessary shape.
    fn take_from_untyped<Fmt: HandleFormat>(
        untyped: &mut dyn TakeHandles<Fmt>,
        path: HandlePath<'_>,
    ) -> Result<Self::Handle<Fmt>, AccessError>;

    /// Produces the handle from an `untyped` collection of handles. This is a higher-level
    /// alternative to [`Self::take_from_untyped()`].
    ///
    /// # Errors
    ///
    /// Returns an error in the same situations as [`Self::take_from_untyped()`].
    fn try_from_untyped<Fmt: HandleFormat>(
        mut untyped: UntypedHandles<Fmt>,
    ) -> Result<Self::Handle<Fmt>, AccessError> {
        Self::take_from_untyped(&mut untyped, HandlePath::EMPTY)
    }

    /// Inserts the handle into an `untyped` accumulator.
    fn insert_into_untyped<Fmt: HandleFormat>(
        handle: Self::Handle<Fmt>,
        untyped: &mut dyn BuildHandles<Fmt>,
        path: HandlePath<'_>,
    );

    /// Converts this handle into [`UntypedHandles`]. This is a higher-level alternative to
    /// [`Self::insert_into_untyped()`].
    fn into_untyped<Fmt: HandleFormat>(handle: Self::Handle<Fmt>) -> UntypedHandles<Fmt> {
        let mut untyped = UntypedHandles::<Fmt>::default();
        Self::insert_into_untyped(handle, &mut untyped, HandlePath::EMPTY);
        untyped
    }
}

/// Handle in a particular format.
pub type InEnv<T, Fmt> = <T as WithHandle>::Handle<Fmt>;
