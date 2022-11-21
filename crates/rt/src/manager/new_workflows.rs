//! Transactions for `WorkflowManager`.

use async_trait::async_trait;
use futures::{stream, Stream, StreamExt};
use tracing_tunnel::PersistedSpans;

use std::{borrow::Cow, collections::HashMap, mem};

use super::{Shared, WorkflowAndChannelIds, WorkflowHandle, WorkflowManager, WorkflowSpawners};
use crate::{
    module::{Clock, Services, StashWorkflow},
    storage::{
        ActiveWorkflowState, ChannelRecord, Storage, StorageTransaction, WorkflowRecord,
        WorkflowWaker,
    },
    workflow::{ChannelIds, PersistedWorkflow},
};
use tardigrade::{
    interface::Interface,
    spawn::{
        ChannelSpawnConfig, ChannelsConfig, HostError, ManageInterfaces, ManageWorkflows,
        SpecifyWorkflowChannels,
    },
    workflow::WorkflowFn,
    ChannelId, WorkflowId,
};

impl ChannelRecord {
    fn new(
        sender_workflow_id: Option<WorkflowId>,
        receiver_workflow_id: Option<WorkflowId>,
    ) -> Self {
        Self {
            receiver_workflow_id,
            sender_workflow_ids: sender_workflow_id.into_iter().collect(),
            has_external_sender: sender_workflow_id.is_none(),
            is_closed: false,
            received_messages: 0,
        }
    }
}

impl ChannelIds {
    pub(crate) async fn new(
        channels: ChannelsConfig<ChannelId>,
        new_channel_ids: impl Stream<Item = ChannelId>,
    ) -> Self {
        futures::pin_mut!(new_channel_ids);
        Self {
            inbound: Self::map_channels(channels.inbound, &mut new_channel_ids).await,
            outbound: Self::map_channels(channels.outbound, &mut new_channel_ids).await,
        }
    }

    async fn map_channels(
        config: HashMap<String, ChannelSpawnConfig<ChannelId>>,
        mut new_channel_ids: impl Stream<Item = ChannelId> + Unpin,
    ) -> HashMap<String, ChannelId> {
        let mut channel_ids = HashMap::with_capacity(config.len());
        for (name, spec) in config {
            let channel_id = match spec {
                ChannelSpawnConfig::New => new_channel_ids.next().await.unwrap(),
                ChannelSpawnConfig::Closed => 0,
                ChannelSpawnConfig::Existing(id) => id,
            };
            channel_ids.insert(name, channel_id);
        }
        channel_ids
    }
}

#[derive(Debug)]
struct WorkflowStub {
    definition_id: String,
    args: Vec<u8>,
    channels: ChannelsConfig<ChannelId>,
}

impl WorkflowStub {
    // **NB.** Should be called only once per instance because `args` are taken out.
    async fn spawn<T: StorageTransaction>(
        &mut self,
        shared: Shared<'_>,
        transaction: &mut T,
    ) -> anyhow::Result<(PersistedWorkflow, ChannelIds)> {
        let spawner = shared.spawners.for_full_id(&self.definition_id).unwrap();
        let services = Services {
            clock: shared.clock,
            workflows: None,
            tracer: None,
        };
        let new_channel_ids = stream::unfold(transaction, |persistence| async {
            let id = persistence.allocate_channel_id().await;
            Some((id, persistence))
        });
        let channel_ids = ChannelIds::new(self.channels.clone(), new_channel_ids).await;
        let args = mem::take(&mut self.args);
        let workflow = spawner.spawn(args, &channel_ids, services)?;
        let persisted = workflow.persist();
        Ok((persisted, channel_ids))
    }
}

#[derive(Debug)]
pub(super) struct NewWorkflows<'a> {
    executing_workflow_id: Option<WorkflowId>,
    shared: Shared<'a>,
    new_workflows: HashMap<WorkflowId, WorkflowStub>,
}

impl<'a> NewWorkflows<'a> {
    pub fn new(executing_workflow_id: Option<WorkflowId>, shared: Shared<'a>) -> Self {
        Self {
            executing_workflow_id,
            shared,
            new_workflows: HashMap::new(),
        }
    }

    pub async fn commit<T: StorageTransaction>(
        self,
        transaction: &mut T,
        parent: &mut PersistedWorkflow,
    ) {
        let executed_workflow_id = self.executing_workflow_id;
        // Create new channels and write outbound messages for them when appropriate.
        for (stub_id, mut child_stub) in self.new_workflows {
            let result = Self::commit_child(
                executed_workflow_id,
                self.shared,
                transaction,
                &mut child_stub,
            );
            match result.await {
                Ok(ids) => {
                    parent.notify_on_child_init(
                        stub_id,
                        ids.workflow_id,
                        &child_stub.channels,
                        ids.channel_ids,
                    );
                }
                Err(err) => {
                    tracing::warn!(%err, "spawning workflow failed");
                    parent.notify_on_child_spawn_error(stub_id, HostError::new(err.to_string()));
                }
            }
        }
    }

    async fn commit_external<T: StorageTransaction>(
        self,
        transaction: &mut T,
    ) -> anyhow::Result<WorkflowId> {
        debug_assert!(self.executing_workflow_id.is_none());
        debug_assert_eq!(self.new_workflows.len(), 1);
        let (_, mut child_stub) = self.new_workflows.into_iter().next().unwrap();
        let child = Self::commit_child(None, self.shared, transaction, &mut child_stub);
        Ok(child.await?.workflow_id)
    }

    async fn commit_child<T: StorageTransaction>(
        executed_workflow_id: Option<WorkflowId>,
        shared: Shared<'_>,
        transaction: &mut T,
        child_stub: &mut WorkflowStub,
    ) -> anyhow::Result<WorkflowAndChannelIds> {
        let (mut persisted, channel_ids) = child_stub.spawn(shared, transaction).await?;
        let child_id = transaction.allocate_workflow_id().await;

        tracing::debug!(?channel_ids, "handling channels for new workflow");
        for (name, &channel_id) in &channel_ids.inbound {
            let state = ChannelRecord::new(executed_workflow_id, Some(child_id));
            let state = transaction.get_or_insert_channel(channel_id, state).await;
            if state.is_closed {
                persisted.close_inbound_channel(None, name);
            }
            tracing::debug!(name, channel_id, ?state, "prepared inbound channel");
        }
        for (name, &channel_id) in &channel_ids.outbound {
            let state = ChannelRecord::new(Some(child_id), executed_workflow_id);
            let state = transaction.get_or_insert_channel(channel_id, state).await;
            if state.is_closed {
                persisted.close_outbound_channel(None, name);
            } else {
                transaction
                    .manipulate_channel(channel_id, |channel| {
                        channel.sender_workflow_ids.insert(child_id);
                    })
                    .await;
            }
            tracing::debug!(name, channel_id, ?state, "prepared outbound channel");
        }

        let (module_id, name_in_module) =
            WorkflowSpawners::split_full_id(&child_stub.definition_id).unwrap();
        let state = ActiveWorkflowState {
            persisted,
            tracing_spans: PersistedSpans::default(),
        };
        let child_workflow = WorkflowRecord {
            id: child_id,
            parent_id: executed_workflow_id,
            module_id: module_id.to_owned(),
            name_in_module: name_in_module.to_owned(),
            state: state.into(),
        };
        transaction.insert_workflow(child_workflow).await;
        transaction
            .insert_waker(child_id, WorkflowWaker::Internal)
            .await;

        Ok(WorkflowAndChannelIds {
            workflow_id: child_id,
            channel_ids,
        })
    }
}

impl ManageInterfaces for NewWorkflows<'_> {
    fn interface(&self, id: &str) -> Option<Cow<'_, Interface>> {
        Some(Cow::Borrowed(
            self.shared.spawners.for_full_id(id)?.interface(),
        ))
    }
}

impl StashWorkflow for NewWorkflows<'_> {
    #[tracing::instrument(skip(self, args), fields(args.len = args.len()))]
    fn stash_workflow(
        &mut self,
        stub_id: WorkflowId,
        id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<ChannelId>,
    ) {
        debug_assert!(
            self.shared.spawners.for_full_id(id).is_some(),
            "workflow with ID `{id}` is not defined"
        );

        self.new_workflows.insert(
            stub_id,
            WorkflowStub {
                definition_id: id.to_string(),
                args,
                channels,
            },
        );
    }
}

impl<C: Clock, S> ManageInterfaces for WorkflowManager<C, S> {
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        Some(Cow::Borrowed(
            self.spawners.for_full_id(definition_id)?.interface(),
        ))
    }
}

impl<C: Clock, S> SpecifyWorkflowChannels for WorkflowManager<C, S> {
    type Inbound = ChannelId;
    type Outbound = ChannelId;
}

#[async_trait]
impl<'a, W, C, S> ManageWorkflows<'a, W> for WorkflowManager<C, S>
where
    W: WorkflowFn,
    C: Clock,
    S: for<'s> Storage<'s>,
{
    type Handle = WorkflowHandle<'a, W, Self>;
    type Error = anyhow::Error;

    async fn create_workflow(
        &'a self,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<ChannelId>,
    ) -> Result<Self::Handle, Self::Error> {
        let mut new_workflows = NewWorkflows::new(None, self.shared());
        new_workflows.stash_workflow(0, definition_id, args, channels);
        let mut transaction = self.storage.transaction().await;
        let workflow_id = new_workflows.commit_external(&mut transaction).await?;
        transaction.commit().await;
        Ok(self
            .workflow(workflow_id)
            .await
            .unwrap()
            .downcast_unchecked())
    }
}
