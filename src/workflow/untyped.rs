//! Untyped workflow handle.

use std::{fmt, ops};

use super::{DescribeEnv, InEnv, TakeHandle, WithHandle, WorkflowEnv};
use crate::{
    channel::{RawReceiver, RawSender},
    interface::{AccessError, Handle, HandleMap, HandleMapKey, HandlePath},
};

/// Dynamically-typed handle to a workflow containing handles to its channels.
pub struct UntypedHandle<Env: WorkflowEnv> {
    pub(crate) handles: HandleMap<InEnv<RawReceiver, Env>, InEnv<RawSender, Env>>,
}

impl<Env: WorkflowEnv> Default for UntypedHandle<Env> {
    fn default() -> Self {
        Self {
            handles: HandleMap::new(),
        }
    }
}

impl<Env: WorkflowEnv> fmt::Debug for UntypedHandle<Env>
where
    InEnv<RawReceiver, Env>: fmt::Debug,
    InEnv<RawSender, Env>: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.handles, formatter)
    }
}

type KeyHandle<K, Env> =
    <K as HandleMapKey>::Output<InEnv<RawReceiver, Env>, InEnv<RawSender, Env>>;

impl<Env: WorkflowEnv> UntypedHandle<Env> {
    /// Removes an element with the specified index from this handle.
    ///
    /// # Errors
    ///
    /// Returns an error if the element is not present in the handle, or if it has an unexpected
    /// type (e.g., a sender instead of a receiver).
    pub fn remove<K: HandleMapKey>(&mut self, key: K) -> Result<KeyHandle<K, Env>, AccessError> {
        key.remove(&mut self.handles)
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

        let handles = interface
            .handles()
            .map(|(path, spec)| {
                let handle = match spec {
                    Handle::Receiver(_) => {
                        Handle::Receiver(RawReceiver::take_handle(&mut *env, path)?)
                    }
                    Handle::Sender(_) => Handle::Sender(RawSender::take_handle(&mut *env, path)?),
                };
                Ok((path.to_owned(), handle))
            })
            .collect::<Result<_, AccessError>>()?;

        Ok(UntypedHandle { handles })
    }
}

impl<Env: WorkflowEnv, K: HandleMapKey> ops::Index<K> for UntypedHandle<Env> {
    type Output = KeyHandle<K, Env>;

    fn index(&self, index: K) -> &Self::Output {
        index
            .get(&self.handles)
            .unwrap_or_else(|err| panic!("{err}"))
    }
}

impl<Env: WorkflowEnv, K: HandleMapKey> ops::IndexMut<K> for UntypedHandle<Env> {
    fn index_mut(&mut self, index: K) -> &mut Self::Output {
        index
            .get_mut(&mut self.handles)
            .unwrap_or_else(|err| panic!("{err}"))
    }
}
