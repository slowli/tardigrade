//! Untyped workflow handle.

use std::{fmt, mem, ops};
use tardigrade_shared::interface::{Handle, HandlePathBuf};

use super::{handle::default_insert_handles, BuildHandles, HandleFormat, TakeHandles, WithHandle};
use crate::interface::{
    AccessError, AccessErrorKind, HandleMap, HandleMapKey, HandlePath, ReceiverAt, SenderAt,
};

/// Dynamically-typed handle to a workflow containing handles to its channels.
pub struct UntypedHandles<Fmt: HandleFormat> {
    pub(crate) handles: HandleMap<Fmt::RawReceiver, Fmt::RawSender>,
}

impl<Fmt: HandleFormat> Default for UntypedHandles<Fmt> {
    fn default() -> Self {
        Self {
            handles: HandleMap::new(),
        }
    }
}

impl<Fmt: HandleFormat> fmt::Debug for UntypedHandles<Fmt>
where
    Fmt::RawReceiver: fmt::Debug,
    Fmt::RawSender: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.handles, formatter)
    }
}

type KeyHandle<K, Fmt> = <K as HandleMapKey>::Output<
    <Fmt as HandleFormat>::RawReceiver,
    <Fmt as HandleFormat>::RawSender,
>;

impl<Fmt: HandleFormat> UntypedHandles<Fmt> {
    /// Removes an element with the specified index from this handle.
    ///
    /// # Errors
    ///
    /// Returns an error if the element is not present in the handle, or if it has an unexpected
    /// type (e.g., a sender instead of a receiver).
    pub fn remove<K: HandleMapKey>(&mut self, key: K) -> Result<KeyHandle<K, Fmt>, AccessError> {
        key.remove(&mut self.handles)
    }
}

impl<Fmt: HandleFormat> TakeHandles<Fmt> for UntypedHandles<Fmt> {
    #[inline]
    fn take_receiver(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawReceiver, AccessError> {
        ReceiverAt(path).remove(&mut self.handles)
    }

    #[inline]
    fn take_sender(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawSender, AccessError> {
        SenderAt(path).remove(&mut self.handles)
    }

    #[inline]
    fn drain(&mut self) -> UntypedHandles<Fmt> {
        mem::take(self)
    }
}

impl<Fmt: HandleFormat> BuildHandles<Fmt> for UntypedHandles<Fmt> {
    #[inline]
    fn insert_handle(
        &mut self,
        path: HandlePathBuf,
        handle: Handle<Fmt::RawReceiver, Fmt::RawSender>,
    ) {
        self.handles.insert(path, handle);
    }

    #[inline]
    fn insert_handles(&mut self, path: HandlePath<'_>, handles: UntypedHandles<Fmt>) {
        if path.is_empty() {
            self.handles.extend(handles.handles);
        } else {
            default_insert_handles(self, path, handles);
        }
    }
}

impl WithHandle for () {
    type Handle<Fmt: HandleFormat> = UntypedHandles<Fmt>;

    fn take_from_untyped<Fmt: HandleFormat>(
        untyped: &mut dyn TakeHandles<Fmt>,
        path: HandlePath<'_>,
    ) -> Result<Self::Handle<Fmt>, AccessError> {
        if path.is_empty() {
            Ok(untyped.drain())
        } else {
            let message = "untyped handle can only obtained from an empty path";
            Err(AccessErrorKind::custom(message).with_location(path))
        }
    }

    fn insert_into_untyped<Fmt: HandleFormat>(
        handle: Self::Handle<Fmt>,
        untyped: &mut dyn BuildHandles<Fmt>,
        path: HandlePath<'_>,
    ) {
        untyped.insert_handles(path, handle);
    }
}

impl<Fmt: HandleFormat, K: HandleMapKey> ops::Index<K> for UntypedHandles<Fmt> {
    type Output = KeyHandle<K, Fmt>;

    fn index(&self, index: K) -> &Self::Output {
        index
            .get(&self.handles)
            .unwrap_or_else(|err| panic!("{err}"))
    }
}

impl<Fmt: HandleFormat, K: HandleMapKey> ops::IndexMut<K> for UntypedHandles<Fmt> {
    fn index_mut(&mut self, index: K) -> &mut Self::Output {
        index
            .get_mut(&mut self.handles)
            .unwrap_or_else(|err| panic!("{err}"))
    }
}
