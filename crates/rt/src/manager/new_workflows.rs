//! Transactions for `WorkflowManager`.

use async_trait::async_trait;
use futures::{stream, Stream, StreamExt};
use tracing_tunnel::PersistedSpans;

use std::{borrow::Cow, collections::HashMap, mem, sync::Arc};

use super::{
    Clock, Services, Shared, StashWorkflow, WorkflowAndChannelIds, WorkflowDefinitions,
    WorkflowHandle, WorkflowManager,
};
use crate::{
    data::WorkflowData,
    engine::{DefineWorkflow, WorkflowEngine},
    storage::{
        ActiveWorkflowState, ChannelRecord, Storage, StorageTransaction, WorkflowRecord,
        WorkflowWaker,
    },
    workflow::{ChannelIds, PersistedWorkflow, Workflow},
};
use tardigrade::interface::Resource;
use tardigrade::{
    interface::{HandleMap, Interface},
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

async fn new_channel_ids(
    config: ChannelsConfig<ChannelId>,
    id_stream: impl Stream<Item = ChannelId>,
) -> ChannelIds {
    futures::pin_mut!(id_stream);

    let mut channel_ids = HandleMap::with_capacity(config.len());
    for (name, config) in config {
        let channel_id = match config.factor() {
            ChannelSpawnConfig::New => id_stream.next().await.unwrap(),
            ChannelSpawnConfig::Closed => 0,
            ChannelSpawnConfig::Existing(id) => id,
        };
        let channel_id = config.map(|_| channel_id);
        channel_ids.insert(name, channel_id);
    }
    channel_ids
}

#[derive(Debug)]
struct WorkflowStub {
    definition_id: String,
    args: Vec<u8>,
    channels: ChannelsConfig<ChannelId>,
}

impl WorkflowStub {
    // **NB.** Should be called only once per instance because `args` are taken out.
    async fn spawn<D: DefineWorkflow, T: StorageTransaction>(
        &mut self,
        shared: &Shared<D>,
        transaction: &mut T,
    ) -> anyhow::Result<(PersistedWorkflow, ChannelIds)> {
        let definition = shared.definitions.for_full_id(&self.definition_id).unwrap();
        let services = Services {
            clock: Arc::clone(&shared.clock),
            workflows: None,
            tracer: None,
        };
        let id_stream = stream::unfold(transaction, |persistence| async {
            let id = persistence.allocate_channel_id().await;
            Some((id, persistence))
        });
        let channel_ids = new_channel_ids(self.channels.clone(), id_stream).await;
        let args = mem::take(&mut self.args);
        let data = WorkflowData::new(definition.interface(), channel_ids.clone(), services);
        let mut workflow = Workflow::new(definition, data, Some(args.into()))?;
        let persisted = workflow.persist();
        Ok((persisted, channel_ids))
    }
}

#[derive(Debug)]
pub(super) struct NewWorkflows<S> {
    executing_workflow_id: Option<WorkflowId>,
    shared: Shared<S>,
    new_workflows: HashMap<WorkflowId, WorkflowStub>,
}

impl<S: DefineWorkflow> NewWorkflows<S> {
    pub fn new(executing_workflow_id: Option<WorkflowId>, shared: Shared<S>) -> Self {
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
                &self.shared,
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
        let child = Self::commit_child(None, &self.shared, transaction, &mut child_stub);
        Ok(child.await?.workflow_id)
    }

    async fn commit_child<T: StorageTransaction>(
        executed_workflow_id: Option<WorkflowId>,
        shared: &Shared<S>,
        transaction: &mut T,
        child_stub: &mut WorkflowStub,
    ) -> anyhow::Result<WorkflowAndChannelIds> {
        let (mut persisted, channel_ids) = child_stub.spawn(shared, transaction).await?;
        let child_id = transaction.allocate_workflow_id().await;

        tracing::debug!(?channel_ids, "handling channels for new workflow");
        for (path, &id_res) in &channel_ids {
            let channel_id = id_res.factor();
            let state = match id_res {
                Resource::Receiver(_) => ChannelRecord::new(executed_workflow_id, Some(child_id)),
                Resource::Sender(_) => ChannelRecord::new(Some(child_id), executed_workflow_id),
            };
            let state = transaction.get_or_insert_channel(channel_id, state).await;
            if state.is_closed {
                persisted.close_channel(id_res);
            } else if matches!(id_res, Resource::Sender(_)) {
                transaction
                    .manipulate_channel(channel_id, |channel| {
                        channel.sender_workflow_ids.insert(child_id);
                    })
                    .await;
            }
            tracing::debug!(?path, channel_id, ?state, "prepared channel");
        }

        let (module_id, name_in_module) =
            WorkflowDefinitions::<S>::split_full_id(&child_stub.definition_id).unwrap();
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

impl<S: DefineWorkflow> ManageInterfaces for NewWorkflows<S> {
    fn interface(&self, id: &str) -> Option<Cow<'_, Interface>> {
        Some(Cow::Borrowed(
            self.shared.definitions.for_full_id(id)?.interface(),
        ))
    }
}

impl<S: DefineWorkflow> StashWorkflow for NewWorkflows<S> {
    #[tracing::instrument(skip(self, args), fields(args.len = args.len()))]
    fn stash_workflow(
        &mut self,
        stub_id: WorkflowId,
        id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<ChannelId>,
    ) {
        debug_assert!(
            self.shared.definitions.for_full_id(id).is_some(),
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

impl<E: WorkflowEngine, C, S> ManageInterfaces for WorkflowManager<E, C, S> {
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        Some(Cow::Borrowed(
            self.definitions.for_full_id(definition_id)?.interface(),
        ))
    }
}

impl<E: WorkflowEngine, C, S> SpecifyWorkflowChannels for WorkflowManager<E, C, S> {
    type Receiver = ChannelId;
    type Sender = ChannelId;
}

#[async_trait]
impl<W, E, C, S> ManageWorkflows<W> for WorkflowManager<E, C, S>
where
    W: WorkflowFn,
    E: WorkflowEngine,
    C: Clock,
    S: Storage,
{
    type Handle<'s> = WorkflowHandle<'s, W, Self>;
    type Error = anyhow::Error;

    async fn create_workflow(
        &self,
        definition_id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<ChannelId>,
    ) -> Result<Self::Handle<'_>, Self::Error> {
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
