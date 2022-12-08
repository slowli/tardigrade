//! Misc helpers for `Handle`s.

use std::{
    borrow::{Borrow, BorrowMut},
    fmt,
    marker::PhantomData,
    ops,
};

pub use crate::{
    handle::{AccessError, AccessErrorKind, Handle, HandleLocation, HandleMap},
    path::{HandlePath, HandlePathBuf},
};

// region:HandleMapKey

/// Newtype for indexing channel receivers, e.g., in an [`Interface`].
///
/// [`Interface`]: crate::interface::Interface
///
/// # Examples
///
/// See [`HandleMapKey`] and [`WithIndexing`] docs for examples of usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReceiverAt<T>(pub T);

impl<T: fmt::Display> fmt::Display for ReceiverAt<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "channel receiver `{}`", self.0)
    }
}

/// Newtype for indexing channel senders, e.g., in an [`Interface`].
///
/// [`Interface`]: crate::interface::Interface
///
/// # Examples
///
/// See [`HandleMapKey`] and [`WithIndexing`] docs for examples of usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SenderAt<T>(pub T);

impl<T: fmt::Display> fmt::Display for SenderAt<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "channel sender `{}`", self.0)
    }
}

/// Key type for accessing handles in a named collections, such as a [`HandleMap`]
/// or an [`Interface`].
///
/// The methods of this trait rarely need to be used directly; rather, you'd see `HandleMapKey`
/// as a bound on the access methods of handle collections, e.g. [`Interface::handle()`].
///
/// [`Interface`]: crate::interface::Interface
/// [`Interface::handle()`]: crate::interface::Interface::handle()
///
/// # Examples
///
/// ```
/// # use tardigrade_shared::handle::{
/// #     AccessError, AccessErrorKind, Handle, HandleMap, HandleMapKey, SenderAt, ReceiverAt,
/// # };
/// let mut map: HandleMap<u64> = HandleMap::from_iter([
///     ("sx".into(), Handle::Sender(42)),
///     ("rx".into(), Handle::Receiver(555)),
/// ]);
///
/// assert_eq!("sx".get_from(&map)?, &Handle::Sender(42));
/// *ReceiverAt("rx").get_mut_from(&mut map)? = 777;
/// assert_eq!(SenderAt("sx").remove_from(&mut map)?, 42);
///
/// let err = SenderAt("rx").get_from(&map).unwrap_err();
/// assert!(matches!(err.kind(), AccessErrorKind::KindMismatch));
/// # Ok::<_, AccessError>(())
/// ```
pub trait HandleMapKey: Into<HandleLocation> + Copy + fmt::Debug {
    /// Output of the access operation. Parameterized by receiver and sender types
    /// (e.g., the corresponding specifications for [`Interface`]).
    ///
    /// [`Interface`]: crate::interface::Interface
    type Output<Rx, Sx>;

    // This is quite ugly, but we cannot return `HandlePath<'_>` from a method
    // even if we introduce a surrogate lifetime by using the `&self` receiver.
    #[doc(hidden)]
    fn with_path<R>(self, action: impl FnOnce(HandlePath<'_>) -> R) -> R;

    #[doc(hidden)]
    fn from_handle<Rx, Sx>(handle: Handle<Rx, Sx>) -> Option<Self::Output<Rx, Sx>>;

    // We cannot only use `Self::from_handle()` with `<&Rx, &Sx>` type args because
    // we need a reference to be returned in some cases (e.g., for indexing).
    #[doc(hidden)]
    fn from_handle_ref<Rx, Sx>(handle: &Handle<Rx, Sx>) -> Option<&Self::Output<Rx, Sx>>;

    #[doc(hidden)]
    fn from_handle_mut<Rx, Sx>(handle: &mut Handle<Rx, Sx>) -> Option<&mut Self::Output<Rx, Sx>>;

    /// Returns a shared reference from the provided `map` using this key.
    ///
    /// # Errors
    ///
    /// Returns an error if a value with the specified key doesn't exist in the map,
    /// or if it has an unexpected type.
    fn get_from<Rx, Sx>(
        self,
        map: &HandleMap<Rx, Sx>,
    ) -> Result<&Self::Output<Rx, Sx>, AccessError> {
        let result = self
            .with_path(|path| map.get(&path))
            .ok_or(AccessErrorKind::Missing);
        result
            .and_then(|handle| Self::from_handle_ref(handle).ok_or(AccessErrorKind::KindMismatch))
            .map_err(|err| err.with_location(self))
    }

    /// Returns an exclusive reference from the provided `map` using this key.
    ///
    /// # Errors
    ///
    /// Returns an error if a value with the specified key doesn't exist in the map,
    /// or if it has an unexpected type.
    fn get_mut_from<Rx, Sx>(
        self,
        map: &mut HandleMap<Rx, Sx>,
    ) -> Result<&mut Self::Output<Rx, Sx>, AccessError> {
        let result = self
            .with_path(|path| map.get_mut(&path))
            .ok_or(AccessErrorKind::Missing);
        result
            .and_then(|handle| Self::from_handle_mut(handle).ok_or(AccessErrorKind::KindMismatch))
            .map_err(|err| err.with_location(self))
    }

    /// Removes a value from the provided `map` using this key.
    ///
    /// # Errors
    ///
    /// Returns an error if a value with the specified key doesn't exist in the map,
    /// or if it has an unexpected type. In the latter case, the value is *not* removed
    /// from the map (i.e., the type check is performed before any modifications).
    fn remove_from<Rx, Sx>(
        self,
        map: &mut HandleMap<Rx, Sx>,
    ) -> Result<Self::Output<Rx, Sx>, AccessError> {
        self.get_from(map)?;
        let handle = self.with_path(|path| map.remove(&path)).unwrap();
        Ok(Self::from_handle(handle).unwrap())
    }
}

impl<'p, P> HandleMapKey for P
where
    P: Into<HandlePath<'p>> + Copy + fmt::Debug,
{
    type Output<Rx, Sx> = Handle<Rx, Sx>;

    fn with_path<R>(self, action: impl FnOnce(HandlePath<'_>) -> R) -> R {
        action(self.into())
    }

    fn from_handle<Rx, Sx>(handle: Handle<Rx, Sx>) -> Option<Self::Output<Rx, Sx>> {
        Some(handle)
    }

    fn from_handle_ref<Rx, Sx>(handle: &Handle<Rx, Sx>) -> Option<&Self::Output<Rx, Sx>> {
        Some(handle)
    }

    fn from_handle_mut<Rx, Sx>(handle: &mut Handle<Rx, Sx>) -> Option<&mut Self::Output<Rx, Sx>> {
        Some(handle)
    }
}

impl<'p, P> HandleMapKey for ReceiverAt<P>
where
    P: 'p + Into<HandlePath<'p>> + Copy + fmt::Debug,
{
    type Output<Rx, Sx> = Rx;

    fn with_path<R>(self, action: impl FnOnce(HandlePath<'_>) -> R) -> R {
        action(self.0.into())
    }

    fn from_handle<Rx, Sx>(handle: Handle<Rx, Sx>) -> Option<Rx> {
        match handle {
            Handle::Receiver(rx) => Some(rx),
            Handle::Sender(_) => None,
        }
    }

    fn from_handle_ref<Rx, Sx>(handle: &Handle<Rx, Sx>) -> Option<&Rx> {
        match handle {
            Handle::Receiver(rx) => Some(rx),
            Handle::Sender(_) => None,
        }
    }

    fn from_handle_mut<Rx, Sx>(handle: &mut Handle<Rx, Sx>) -> Option<&mut Rx> {
        match handle {
            Handle::Receiver(rx) => Some(rx),
            Handle::Sender(_) => None,
        }
    }
}

impl<'p, P> HandleMapKey for SenderAt<P>
where
    P: 'p + Into<HandlePath<'p>> + Copy + fmt::Debug,
{
    type Output<Rx, Sx> = Sx;

    fn with_path<R>(self, action: impl FnOnce(HandlePath<'_>) -> R) -> R {
        action(self.0.into())
    }

    fn from_handle<Rx, Sx>(handle: Handle<Rx, Sx>) -> Option<Sx> {
        match handle {
            Handle::Sender(sx) => Some(sx),
            Handle::Receiver(_) => None,
        }
    }

    fn from_handle_ref<Rx, Sx>(handle: &Handle<Rx, Sx>) -> Option<&Sx> {
        match handle {
            Handle::Sender(sx) => Some(sx),
            Handle::Receiver(_) => None,
        }
    }

    fn from_handle_mut<Rx, Sx>(handle: &mut Handle<Rx, Sx>) -> Option<&mut Sx> {
        match handle {
            Handle::Sender(sx) => Some(sx),
            Handle::Receiver(_) => None,
        }
    }
}

// endregion:HandleMapKey
// region:WithIndexing

/// Wrapper for [`HandleMap`] allowing accessing the map elements by [`HandleMapKey`].
/// This wrapper is produced by the [`WithIndexing`] trait.
///
/// # Examples
///
/// See [`WithIndexing`](WithIndexing#examples) for an example of usage.
#[derive(Debug)]
pub struct IndexingHandleMap<T, Rx, Sx> {
    /// Contained map.
    pub inner: T,
    _ty: PhantomData<fn(Rx, Sx)>,
}

impl<T, Rx, Sx> IndexingHandleMap<T, Rx, Sx>
where
    T: BorrowMut<HandleMap<Rx, Sx>>,
{
    /// Removes an element with the specified index from this handle.
    ///
    /// # Errors
    ///
    /// Returns an error if the element is not present in the handle, or if it has an unexpected
    /// type (e.g., a sender instead of a receiver).
    pub fn remove<K: HandleMapKey>(&mut self, key: K) -> Result<K::Output<Rx, Sx>, AccessError> {
        key.remove_from(self.inner.borrow_mut())
    }
}

impl<T, Rx, Sx, K: HandleMapKey> ops::Index<K> for IndexingHandleMap<T, Rx, Sx>
where
    T: Borrow<HandleMap<Rx, Sx>>,
{
    type Output = K::Output<Rx, Sx>;

    fn index(&self, index: K) -> &Self::Output {
        index
            .get_from(self.inner.borrow())
            .unwrap_or_else(|err| panic!("{err}"))
    }
}

impl<T, Rx, Sx, K: HandleMapKey> ops::IndexMut<K> for IndexingHandleMap<T, Rx, Sx>
where
    T: BorrowMut<HandleMap<Rx, Sx>>,
{
    fn index_mut(&mut self, index: K) -> &mut Self::Output {
        index
            .get_mut_from(self.inner.borrow_mut())
            .unwrap_or_else(|err| panic!("{err}"))
    }
}

/// Converts [`HandleMap`] to the [indexing form](IndexingHandleMap). This allows accessing
/// contained [`Handle`]s by any type implementing [`HandleMapKey`], e.g., [`SenderAt`] /
/// [`ReceiverAt`].
///
/// # Examples
///
/// ```
/// # use tardigrade_shared::handle::{
/// #     AccessError, Handle, HandleMap, SenderAt, ReceiverAt, WithIndexing,
/// # };
/// let map: HandleMap<u64> = HandleMap::from_iter([
///     ("sx".into(), Handle::Sender(42)),
///     ("rx".into(), Handle::Receiver(555)),
/// ]);
/// let mut map = map.with_indexing();
/// assert_eq!(map["sx"], Handle::Sender(42));
/// assert_eq!(map[SenderAt("sx")], 42);
/// assert_eq!(map[ReceiverAt("rx")], 555);
///
/// let mut map = (&mut map.inner).with_indexing();
/// map["sx"] = Handle::Receiver(23);
/// ```
pub trait WithIndexing<Rx, Sx>: Borrow<HandleMap<Rx, Sx>> + Sized {
    /// Performs the conversion.
    fn with_indexing(self) -> IndexingHandleMap<Self, Rx, Sx>;
}

impl<T, Rx, Sx> WithIndexing<Rx, Sx> for T
where
    T: Borrow<HandleMap<Rx, Sx>>,
{
    fn with_indexing(self) -> IndexingHandleMap<Self, Rx, Sx> {
        IndexingHandleMap {
            inner: self,
            _ty: PhantomData,
        }
    }
}

// endregion:WithIndexing
