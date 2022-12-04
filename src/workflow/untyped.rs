//! Untyped workflow handle.

use std::mem;

use super::{handle::default_insert_handles, BuildHandles, HandleFormat, TakeHandles, WithHandle};
use crate::interface::{
    AccessError, AccessErrorKind, Handle, HandleMap, HandleMapKey, HandlePath, HandlePathBuf,
    ReceiverAt, SenderAt,
};

/// Dynamically-typed handle to a workflow containing handles to its channels.
pub type UntypedHandles<Fmt> =
    HandleMap<<Fmt as HandleFormat>::RawReceiver, <Fmt as HandleFormat>::RawSender>;

impl<Fmt: HandleFormat> TakeHandles<Fmt> for UntypedHandles<Fmt> {
    #[inline]
    fn take_receiver(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawReceiver, AccessError> {
        ReceiverAt(path).remove(self)
    }

    #[inline]
    fn take_sender(&mut self, path: HandlePath<'_>) -> Result<Fmt::RawSender, AccessError> {
        SenderAt(path).remove(self)
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
        self.insert(path, handle);
    }

    #[inline]
    fn insert_handles(&mut self, path: HandlePath<'_>, handles: UntypedHandles<Fmt>) {
        if path.is_empty() {
            self.extend(handles);
        } else {
            default_insert_handles::<Fmt, _>(self, path, handles);
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
