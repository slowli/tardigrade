//! Untyped workflow handle.

use std::{collections::HashMap, fmt, ops};

use super::TakeHandle;
use crate::{
    channel::{RawReceiver, RawSender},
    interface::{AccessError, InboundChannel, Interface, OutboundChannel},
};

/// Dynamically-typed handle to a workflow containing handles to its inputs
/// and channels.
pub struct UntypedHandle<Env>
where
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    inbound_channels: HashMap<String, <RawReceiver as TakeHandle<Env>>::Handle>,
    outbound_channels: HashMap<String, <RawSender as TakeHandle<Env>>::Handle>,
}

#[allow(clippy::type_repetition_in_bounds, clippy::trait_duplication_in_bounds)] // false positive
impl<Env> fmt::Debug for UntypedHandle<Env>
where
    RawReceiver: TakeHandle<Env, Id = str>,
    <RawReceiver as TakeHandle<Env>>::Handle: fmt::Debug,
    RawSender: TakeHandle<Env, Id = str>,
    <RawSender as TakeHandle<Env>>::Handle: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UntypedHandle")
            .field("inbound_channels", &self.inbound_channels)
            .field("outbound_channels", &self.outbound_channels)
            .finish()
    }
}

impl<Env> UntypedHandle<Env>
where
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    /// Removes an element with the specified index from this handle. Returns `None` if
    /// the element is not present in the handle.
    pub fn remove<I>(&mut self, index: I) -> Option<I::Output>
    where
        I: UntypedHandleIndex<Env>,
    {
        index.remove_from(self)
    }
}

impl<Env> TakeHandle<Env> for UntypedHandle<Env>
where
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
    Interface<()>: TakeHandle<Env, Id = (), Handle = Interface<()>>,
{
    type Id = ();
    type Handle = Self;

    fn take_handle(env: &mut Env, _id: &()) -> Result<Self, AccessError> {
        let interface = Interface::<()>::take_handle(env, &())?;

        let inbound_channels = interface
            .inbound_channels()
            .map(|(name, _)| Ok((name.to_owned(), RawReceiver::take_handle(&mut *env, name)?)))
            .collect::<Result<_, AccessError>>()?;
        let outbound_channels = interface
            .outbound_channels()
            .map(|(name, _)| Ok((name.to_owned(), RawSender::take_handle(&mut *env, name)?)))
            .collect::<Result<_, AccessError>>()?;

        Ok(Self {
            inbound_channels,
            outbound_channels,
        })
    }
}

/// Types that can be used for indexing [`UntypedHandle`].
pub trait UntypedHandleIndex<Env>: Copy + fmt::Display
where
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    /// Output type for the indexing operation.
    type Output;

    #[doc(hidden)]
    fn get_from(self, handle: &UntypedHandle<Env>) -> Option<&Self::Output>;

    #[doc(hidden)]
    fn get_mut_from(self, handle: &mut UntypedHandle<Env>) -> Option<&mut Self::Output>;

    #[doc(hidden)]
    fn remove_from(self, handle: &mut UntypedHandle<Env>) -> Option<Self::Output>;
}

macro_rules! impl_index {
    ($target:ty => $raw:ty, $field:ident) => {
        impl<Env> UntypedHandleIndex<Env> for $target
        where
            RawReceiver: TakeHandle<Env, Id = str>,
            RawSender: TakeHandle<Env, Id = str>,
        {
            type Output = <$raw as TakeHandle<Env>>::Handle;

            fn get_from(self, handle: &UntypedHandle<Env>) -> Option<&Self::Output> {
                handle.$field.get(self.0)
            }

            fn get_mut_from(self, handle: &mut UntypedHandle<Env>) -> Option<&mut Self::Output> {
                handle.$field.get_mut(self.0)
            }

            fn remove_from(self, handle: &mut UntypedHandle<Env>) -> Option<Self::Output> {
                handle.$field.remove(self.0)
            }
        }
    };
}

impl_index!(InboundChannel<'_> => RawReceiver, inbound_channels);
impl_index!(OutboundChannel<'_> => RawSender, outbound_channels);

impl<Env, I> ops::Index<I> for UntypedHandle<Env>
where
    I: UntypedHandleIndex<Env>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        index
            .get_from(self)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<Env, I> ops::IndexMut<I> for UntypedHandle<Env>
where
    I: UntypedHandleIndex<Env>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        index
            .get_mut_from(self)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}
