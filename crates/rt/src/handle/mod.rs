//! Handles for workflows in a `WorkflowManager` and their components (e.g., channels).

use async_trait::async_trait;

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
    helper, ChannelRecord, ReadChannels, ReadModules, ReadWorkflows, Storage, StorageTransaction,
    WorkflowState,
};
use tardigrade::{
    spawn::ManageChannels,
    workflow::{HandleFormat, InEnv, Inverse},
    ChannelId, Codec, WorkflowId,
};

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

impl<'a, S: Storage> StorageRef<'a, S> {
    /// Returns current information about the channel with the specified ID, or `None` if a channel
    /// with this ID does not exist.
    pub async fn channel(&self, channel_id: ChannelId) -> Option<ChannelRecord> {
        let transaction = self.inner.readonly_transaction().await;
        let record = transaction.channel(channel_id).await?;
        Some(record)
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
        let transaction = self.inner.readonly_transaction().await;
        let record = transaction.workflow(workflow_id).await?;
        match record.state {
            WorkflowState::Active(state) => {
                let module = transaction.module(&record.module_id).await?;
                let interface = &module.definitions.get(&record.name_in_module)?.interface;
                let handle =
                    WorkflowHandle::new(self.inner, workflow_id, interface.clone(), *state);
                Some(handle.into())
            }

            WorkflowState::Errored(state) => {
                let handle = ErroredWorkflowHandle::new(
                    self.inner,
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

    /// Returns the number of active workflows.
    pub async fn workflow_count(&self) -> usize {
        let transaction = self.inner.readonly_transaction().await;
        transaction.count_active_workflows().await
    }
}

/// Host handles of a shape specified by a workflow [`Interface`] and provided
/// by a [`WorkflowManager`].
///
/// [`WorkflowManager`]: crate::manager::WorkflowManager
pub type HostHandles<'a, W, S> = InEnv<W, Inverse<StorageRef<'a, S>>>;

#[async_trait]
impl<'a, S> ManageChannels for StorageRef<'a, S>
where
    S: Storage + 'static, // TODO: can this bound be relaxed? (may be introduced by `async_trait`)
{
    type Fmt = Self;

    fn closed_receiver(&self) -> RawMessageReceiver<&'a S> {
        RawMessageReceiver::closed(self.inner)
    }

    fn closed_sender(&self) -> RawMessageSender<&'a S> {
        RawMessageSender::closed(self.inner)
    }

    async fn create_channel(&self) -> (RawMessageSender<&'a S>, RawMessageReceiver<&'a S>) {
        let mut transaction = self.inner.transaction().await;
        let (channel_id, record) = helper::commit_channel(None, &mut transaction).await;
        transaction.commit().await;
        (
            RawMessageSender::new(self.inner, channel_id, record.clone()),
            RawMessageReceiver::new(self.inner, channel_id, record),
        )
    }
}
