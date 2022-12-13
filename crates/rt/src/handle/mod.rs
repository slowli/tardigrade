//! Handles for workflows in a `WorkflowManager` and their components (e.g., channels).

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

/// [`HandleFormat`] for handles returned by a [`WorkflowManager`].
///
/// [`WorkflowManager`]: crate::manager::WorkflowManager
#[derive(Debug)]
pub struct StorageRef<'a, S> {
    inner: &'a S,
}

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

// The convoluted implementations for methods are required to properly infer the `Send` bound
// for the `async fn` futures for all `S: Storage`. If done normally, this bound is only inferred
// for `S: 'static` (probably related to GAT limitations).
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
            match record.state {
                WorkflowState::Active(state) => {
                    let module = transaction.module(&record.module_id).await?;
                    let interface = &module.definitions.get(&record.name_in_module)?.interface;
                    let handle =
                        WorkflowHandle::new(storage, workflow_id, interface.clone(), *state);
                    Some(handle.into())
                }

                WorkflowState::Errored(state) => {
                    let handle = ErroredWorkflowHandle::new(
                        storage,
                        workflow_id,
                        state.error,
                        state.erroneous_messages,
                    );
                    Some(handle.into())
                }

                WorkflowState::Completed(state) => {
                    let handle = CompletedWorkflowHandle::new(workflow_id, state);
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

    fn create_channel(&self) -> impl Future<Output = Channel<'a, S>> + Send + 'a {
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
/// by a [`WorkflowManager`].
///
/// [`WorkflowManager`]: crate::manager::WorkflowManager
pub type HostHandles<'a, W, S> = InEnv<W, Inverse<StorageRef<'a, S>>>;

#[async_trait]
impl<'a, S: Storage> CreateChannel for StorageRef<'a, S> {
    type Fmt = Self;

    fn closed_receiver(&self) -> RawMessageReceiver<&'a S> {
        RawMessageReceiver::closed(self.inner)
    }

    fn closed_sender(&self) -> RawMessageSender<&'a S> {
        RawMessageSender::closed(self.inner)
    }

    async fn new_channel(&self) -> (RawMessageSender<&'a S>, RawMessageReceiver<&'a S>) {
        self.create_channel().await
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
