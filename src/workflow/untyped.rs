//! Untyped workflow handle.

use std::{fmt, ops};

use super::{DescribeEnv, Handle, TakeHandle, WithHandle, WorkflowEnv};
use crate::{
    channel::{RawReceiver, RawSender},
    interface::{AccessError, HandleMap, HandlePath, ReceiverAt, SenderAt},
};

/// Dynamically-typed handle to a workflow containing handles to its channels.
pub struct UntypedHandle<Env: WorkflowEnv> {
    pub(crate) receivers: HandleMap<Handle<RawReceiver, Env>>,
    pub(crate) senders: HandleMap<Handle<RawSender, Env>>,
}

impl<Env: WorkflowEnv> Default for UntypedHandle<Env> {
    fn default() -> Self {
        Self {
            receivers: HandleMap::new(),
            senders: HandleMap::new(),
        }
    }
}

impl<Env: WorkflowEnv> fmt::Debug for UntypedHandle<Env>
where
    Handle<RawReceiver, Env>: fmt::Debug,
    Handle<RawSender, Env>: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UntypedHandle")
            .field("receivers", &self.receivers)
            .field("senders", &self.senders)
            .finish()
    }
}

impl<Env: WorkflowEnv> UntypedHandle<Env> {
    /// Removes an element with the specified index from this handle. Returns `None` if
    /// the element is not present in the handle.
    pub fn remove<I>(&mut self, index: I) -> Option<I::Output>
    where
        I: UntypedHandleIndex<Env>,
    {
        index.remove_from(self)
    }
}

impl WithHandle for () {
    type Handle<Env: WorkflowEnv> = UntypedHandle<Env>;
}

impl<Env: DescribeEnv> TakeHandle<Env> for () {
    // FIXME: incorrect; `path` arg should be taken into account
    fn take_handle(
        env: &mut Env,
        _path: HandlePath<'_>,
    ) -> Result<UntypedHandle<Env>, AccessError> {
        let interface = env.interface().into_owned();

        let receivers = interface
            .receivers()
            .map(|(path, _)| Ok((path.to_owned(), RawReceiver::take_handle(&mut *env, path)?)))
            .collect::<Result<_, AccessError>>()?;
        let senders = interface
            .senders()
            .map(|(path, _)| Ok((path.to_owned(), RawSender::take_handle(&mut *env, path)?)))
            .collect::<Result<_, AccessError>>()?;

        Ok(UntypedHandle { receivers, senders })
    }
}

/// Types that can be used for indexing [`UntypedHandle`].
pub trait UntypedHandleIndex<Env: WorkflowEnv>: Copy + fmt::Display {
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
        impl<'a, P, Env> UntypedHandleIndex<Env> for $target
        where
            P: Into<HandlePath<'a>> + fmt::Display + Copy,
            Env: WorkflowEnv,
        {
            type Output = Handle<$raw, Env>;

            fn get_from(self, handle: &UntypedHandle<Env>) -> Option<&Self::Output> {
                handle.$field.get(&self.0.into())
            }

            fn get_mut_from(self, handle: &mut UntypedHandle<Env>) -> Option<&mut Self::Output> {
                handle.$field.get_mut(&self.0.into())
            }

            fn remove_from(self, handle: &mut UntypedHandle<Env>) -> Option<Self::Output> {
                handle.$field.remove(&self.0.into())
            }
        }
    };
}

impl_index!(ReceiverAt<P> => RawReceiver, receivers);
impl_index!(SenderAt<P> => RawSender, senders);

impl<Env: WorkflowEnv, I> ops::Index<I> for UntypedHandle<Env>
where
    I: UntypedHandleIndex<Env>,
{
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        index
            .get_from(self)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<Env: WorkflowEnv, I> ops::IndexMut<I> for UntypedHandle<Env>
where
    I: UntypedHandleIndex<Env>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        index
            .get_mut_from(self)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}
