//! Handles for workflows in a [`WorkflowManager`] and their components (e.g., channels).
//!
//! The root of the handle hierarchy is [`StorageRef`], a thin wrapper around a [`Storage`].
//! Its methods allow accessing workflows and channels contained in the storage.
//!
//! # Examples
//!
//! ```
//! # use tardigrade::{handle::{ReceiverAt, SenderAt, WithIndexing}, workflow::TryFromRaw, Json};
//! # use tardigrade_rt::{handle::{MessageSender, StorageRef}, storage::Storage};
//! # async fn storage<S: Storage>(storage: StorageRef<'_, S>) -> anyhow::Result<()> {
//! let storage: StorageRef<_> = // ...
//! #   storage;
//! let workflow = storage.workflow(1).await.unwrap();
//! // Workflow channels can be accessed via the `workflow` handle:
//! let handle = workflow.handle().await.with_indexing();
//! handle[ReceiverAt("commands")].send(b"test".to_vec()).await?;
//! let message = handle[SenderAt("events")].receive_message(0).await?;
//!
//! // Channel handles also can be accessed directly:
//! let sender = storage.sender(1).await.unwrap();
//! let sender = MessageSender::<u64, Json, _>::try_from_raw(sender)?;
//! sender.send_all([42, 555]).await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`WorkflowManager`]: crate::manager::WorkflowManager

use async_trait::async_trait;
use futures::{Future, FutureExt};

mod channel;
mod workflow;

pub use self::{
    channel::{
        MessageReceiver, MessageSender, RawMessageReceiver, RawMessageSender, ReceivedMessage,
    },
    workflow::{
        AnyWorkflowHandle, CompletedWorkflowHandle, ErroneousMessage, ErroredWorkflowHandle,
        WorkflowHandle,
    },
};
pub use crate::storage::helper::ConcurrencyError;

use crate::storage::{
    helper, ChannelRecord, ReadonlyStorageTransaction, Storage, StorageTransaction, WorkflowState,
};
use tardigrade::{
    spawn::CreateChannel,
    workflow::{HandleFormat, InEnv, Inverse},
    ChannelId, Codec, WorkflowId,
};

type Channel<'a, S> = (RawMessageSender<&'a S>, RawMessageReceiver<&'a S>);

/// Thin wrapper around a [`Storage`] that provides methods for accessing and manipulating
/// channels and workflows.
///
/// Also acts as a [`HandleFormat`] for the returned handles.
#[derive(Debug)]
pub struct StorageRef<'a, S> {
    inner: &'a S,
}

impl<S: Storage> Clone for StorageRef<'_, S> {
    fn clone(&self) -> Self {
        Self { inner: self.inner }
    }
}

impl<S: Storage> Copy for StorageRef<'_, S> {}

impl<'a, S: Storage> HandleFormat for StorageRef<'a, S> {
    type RawReceiver = RawMessageReceiver<&'a S>;
    type Receiver<T, C: Codec<T>> = MessageReceiver<T, C, &'a S>;
    type RawSender = RawMessageSender<&'a S>;
    type Sender<T, C: Codec<T>> = MessageSender<T, C, &'a S>;
}

impl<'a, S: Storage> From<&'a S> for StorageRef<'a, S> {
    fn from(storage: &'a S) -> Self {
        Self { inner: storage }
    }
}

impl<S: Storage> AsRef<S> for StorageRef<'_, S> {
    fn as_ref(&self) -> &S {
        self.inner
    }
}

// The convoluted implementations for methods are required to properly infer the `Send` bound
// for the `async fn` futures for all `S: Storage`. If done normally, this bound is only inferred
// for `S: 'static` (probably related to the current GAT limitations).
impl<'a, S: Storage> StorageRef<'a, S> {
    /// Returns current information about the channel with the specified ID, or `None` if a channel
    /// with this ID does not exist.
    pub async fn channel(&self, channel_id: ChannelId) -> Option<ChannelRecord> {
        #[allow(clippy::manual_async_fn)] // manual impl is required because of the `Send` bound
        #[inline]
        fn get_channel<'t, T: 't + ReadonlyStorageTransaction>(
            transaction: T,
            channel_id: ChannelId,
        ) -> impl Future<Output = Option<ChannelRecord>> + Send + 't {
            async move { transaction.channel(channel_id).await }
        }

        self.inner
            .readonly_transaction()
            .then(move |transaction| get_channel(transaction, channel_id))
            .await
    }

    /// Returns a sender handle for the specified channel, or `None` if the channel does not exist.
    pub async fn sender(&self, channel_id: ChannelId) -> Option<RawMessageSender<&'a S>> {
        let record = self.channel(channel_id).await?;
        Some(RawMessageSender::new(self.inner, channel_id, record))
    }

    /// Returns a receiver handle for the specified channel, or `None` if the channel does not exist.
    pub async fn receiver(&self, channel_id: ChannelId) -> Option<RawMessageReceiver<&'a S>> {
        let record = self.channel(channel_id).await?;
        Some(RawMessageReceiver::new(self.inner, channel_id, record))
    }

    /// Returns a handle to an active workflow with the specified ID. If the workflow is
    /// not active or does not exist, returns `None`.
    pub async fn workflow(&self, workflow_id: WorkflowId) -> Option<WorkflowHandle<(), &'a S>> {
        let handle = self.any_workflow(workflow_id).await?;
        match handle {
            AnyWorkflowHandle::Active(handle) => Some(*handle),
            _ => None,
        }
    }

    /// Returns a handle to a workflow with the specified ID. The workflow may have any state.
    /// If the workflow does not exist, returns `None`.
    pub async fn any_workflow(&self, workflow_id: WorkflowId) -> Option<AnyWorkflowHandle<&'a S>> {
        let storage = self.inner;
        storage
            .readonly_transaction()
            .then(move |transaction| Self::get_workflow(storage, transaction, workflow_id))
            .await
    }

    #[allow(clippy::manual_async_fn)] // manual impl is required because of the `Send` bound
    fn get_workflow<T: ReadonlyStorageTransaction + 'a>(
        storage: &'a S,
        transaction: T,
        workflow_id: WorkflowId,
    ) -> impl Future<Output = Option<AnyWorkflowHandle<&'a S>>> + Send + 'a {
        async move {
            let record = transaction.workflow(workflow_id).await?;
            let (record, state) = record.split();
            match state {
                WorkflowState::Active(state) => {
                    let mut module = transaction.module(&record.module_id).await?;
                    let interface = module.definitions.remove(&record.name_in_module)?.interface;
                    let handle = WorkflowHandle::new(storage, record, *state, interface);
                    Some(handle.into())
                }

                WorkflowState::Errored(state) => {
                    let handle = ErroredWorkflowHandle::new(storage, record, state);
                    Some(handle.into())
                }

                WorkflowState::Completed(state) => {
                    let handle = CompletedWorkflowHandle::new(record, state);
                    Some(handle.into())
                }
            }
        }
    }

    /// Returns the number of active workflows.
    pub async fn workflow_count(&self) -> usize {
        #[allow(clippy::manual_async_fn)] // manual impl is required because of the `Send` bound
        #[inline]
        fn count_workflows<'t, T: 't + ReadonlyStorageTransaction>(
            transaction: T,
        ) -> impl Future<Output = usize> + Send + 't {
            async move { transaction.count_active_workflows().await }
        }

        self.inner
            .readonly_transaction()
            .then(count_workflows)
            .await
    }

    fn create_channel(self) -> impl Future<Output = Channel<'a, S>> + Send + 'a {
        #[inline]
        async fn do_create_channel<T: StorageTransaction>(
            mut transaction: T,
        ) -> (ChannelId, ChannelRecord) {
            let (channel_id, record) = helper::commit_channel(None, &mut transaction).await;
            transaction.commit().await;
            (channel_id, record)
        }

        let storage = self.inner;
        storage
            .transaction()
            .then(do_create_channel)
            .map(move |(channel_id, record)| {
                (
                    RawMessageSender::new(storage, channel_id, record.clone()),
                    RawMessageReceiver::new(storage, channel_id, record),
                )
            })
    }
}

/// Host handles of a shape specified by a workflow [`Interface`] and provided
/// by a [`Storage`].
///
/// [`Interface`]: tardigrade::interface::Interface
pub type HostHandles<'a, W, S> = InEnv<W, Inverse<StorageRef<'a, S>>>;

#[async_trait]
impl<'a, S: Storage> CreateChannel for StorageRef<'a, S> {
    type Fmt = Self;

    #[tracing::instrument(level = "debug", skip_all)]
    fn closed_receiver(&self) -> RawMessageReceiver<&'a S> {
        RawMessageReceiver::closed(self.inner)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn closed_sender(&self) -> RawMessageSender<&'a S> {
        RawMessageSender::closed(self.inner)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn new_channel(&self) -> (RawMessageSender<&'a S>, RawMessageReceiver<&'a S>) {
        let (sx, rx) = self.create_channel().await;
        tracing::debug!(ret.id = sx.channel_id());
        (sx, rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>(value: T) -> T {
        value
    }

    #[allow(dead_code)]
    async fn storage_ref_futures_are_send<S: Storage>(storage: StorageRef<'_, S>) {
        assert_send(storage.channel(1)).await;
        assert_send(storage.sender(1)).await;
        assert_send(storage.receiver(1)).await;
        assert_send(storage.any_workflow(1)).await;
        assert_send(storage.workflow(1)).await;
        assert_send(storage.workflow_count()).await;
    }
}
