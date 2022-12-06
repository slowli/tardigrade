//! Transactions for `WorkflowManager`.

use anyhow::anyhow;
use async_trait::async_trait;
use tracing_tunnel::PersistedSpans;

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    mem,
    sync::Arc,
};

use super::{
    Clock, Host, HostChannelId, Services, Shared, StashStub, WorkflowDefinitions, WorkflowHandle,
    WorkflowManager,
};
use crate::{
    data::WorkflowData,
    engine::{DefineWorkflow, RunWorkflow, WorkflowEngine},
    receipt::Receipt,
    storage::{
        ActiveWorkflowState, ChannelRecord, Storage, StorageTransaction, WorkflowRecord,
        WorkflowWaker,
    },
    workflow::{ChannelIds, PersistedWorkflow, Workflow, WorkflowAndChannelIds},
};
use tardigrade::{
    interface::{Handle, Interface},
    spawn::{HostError, ManageChannels, ManageInterfaces, ManageWorkflows},
    workflow::{GetInterface, UntypedHandles, WorkflowFn},
    ChannelId, WorkflowId,
};

impl ChannelRecord {
    fn owned_by_host() -> Self {
        Self {
            receiver_workflow_id: None,
            sender_workflow_ids: HashSet::new(),
            has_external_sender: true,
            is_closed: false,
            received_messages: 0,
        }
    }

    fn owned_by_workflow(workflow_id: WorkflowId) -> Self {
        Self {
            receiver_workflow_id: Some(workflow_id),
            sender_workflow_ids: HashSet::from_iter([workflow_id]),
            has_external_sender: false,
            is_closed: false,
            received_messages: 0,
        }
    }
}

async fn commit_channel<T: StorageTransaction>(
    executed_workflow_id: Option<WorkflowId>,
    transaction: &mut T,
) -> ChannelId {
    let channel_id = transaction.allocate_channel_id().await;
    let state = executed_workflow_id.map_or_else(
        ChannelRecord::owned_by_host,
        ChannelRecord::owned_by_workflow,
    );
    transaction.insert_channel(channel_id, state).await;
    channel_id
}

#[derive(Debug)]
struct WorkflowStub {
    definition_id: String,
    args: Vec<u8>,
    channel_ids: ChannelIds,
}

impl WorkflowStub {
    // **NB.** Should be called only once per instance because `args` are taken out.
    fn spawn<D: DefineWorkflow>(
        &mut self,
        shared: &Shared<D>,
    ) -> anyhow::Result<(PersistedWorkflow, ChannelIds)> {
        let definition = shared.definitions.for_full_id(&self.definition_id).unwrap();
        let services = Services {
            clock: Arc::clone(&shared.clock),
            stubs: None,
            tracer: None,
        };

        let args = mem::take(&mut self.args);
        let channel_ids = mem::take(&mut self.channel_ids);
        let data = WorkflowData::new(definition.interface(), channel_ids.clone(), services);
        let mut workflow = Workflow::new(definition, data, Some(args.into()))?;
        let persisted = workflow.persist();
        Ok((persisted, channel_ids))
    }
}

/// Storage for channel / workflow stubs created by a transaction.
#[derive(Debug)]
pub(super) struct Stubs<S> {
    executing_workflow_id: Option<WorkflowId>,
    shared: Shared<S>,
    new_workflows: HashMap<WorkflowId, WorkflowStub>,
    new_channels: HashSet<ChannelId>,
}

impl<S: DefineWorkflow> Stubs<S> {
    pub fn new(executing_workflow_id: Option<WorkflowId>, shared: Shared<S>) -> Self {
        Self {
            executing_workflow_id,
            shared,
            new_workflows: HashMap::new(),
            new_channels: HashSet::new(),
        }
    }

    pub async fn commit<T, E>(
        self,
        transaction: &mut T,
        parent: &mut Workflow<E>,
        receipt: &mut Receipt,
    ) where
        T: StorageTransaction,
        E: RunWorkflow,
    {
        let executed_workflow_id = self.executing_workflow_id;

        for local_id in self.new_channels {
            let channel_id = commit_channel(executed_workflow_id, transaction).await;
            parent.notify_on_channel_init(local_id, channel_id, receipt);
        }

        for (stub_id, mut child_stub) in self.new_workflows {
            let result = Self::commit_child(
                executed_workflow_id,
                &self.shared,
                transaction,
                &mut child_stub,
            );
            let result = result.await.map_err(|err| HostError::new(err.to_string()));
            parent.notify_on_child_init(stub_id, result, receipt);
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
        let (mut persisted, channel_ids) = child_stub.spawn(shared)?;
        let child_id = transaction.allocate_workflow_id().await;

        // FIXME: check there are no duplicate receivers with ID != 0

        tracing::debug!(?channel_ids, "handling channels for new workflow");
        for (path, &id_res) in &channel_ids {
            let channel_id = id_res.factor();
            let state = transaction.channel(channel_id).await;
            let state = state.ok_or_else(|| {
                anyhow!("channel {channel_id} specified at {path} does not exist")
            })?;

            if state.is_closed {
                persisted.close_channel(id_res);
            } else {
                let action = |channel: &mut ChannelRecord| {
                    match id_res {
                        Handle::Sender(_) => {
                            channel.sender_workflow_ids.insert(child_id);
                        }
                        Handle::Receiver(_) => {
                            // FIXME: is this enough? (when does rx ownership change?)
                            channel.receiver_workflow_id = Some(child_id);
                        }
                    }
                };
                transaction.manipulate_channel(channel_id, action).await;
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

impl<S: DefineWorkflow> ManageInterfaces for Stubs<S> {
    type Fmt = Host;

    fn interface(&self, id: &str) -> Option<Cow<'_, Interface>> {
        Some(Cow::Borrowed(
            self.shared.definitions.for_full_id(id)?.interface(),
        ))
    }
}

impl<S: DefineWorkflow> StashStub for Stubs<S> {
    #[tracing::instrument(skip(self, args), fields(args.len = args.len()))]
    fn stash_workflow(
        &mut self,
        stub_id: WorkflowId,
        id: &str,
        args: Vec<u8>,
        channel_ids: ChannelIds,
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
                channel_ids,
            },
        );
    }

    #[tracing::instrument(skip(self))]
    fn stash_channel(&mut self, stub_id: ChannelId) {
        self.new_channels.insert(stub_id);
    }
}

impl<E: WorkflowEngine, C, S> ManageInterfaces for WorkflowManager<E, C, S> {
    type Fmt = Host;

    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        Some(Cow::Borrowed(
            self.definitions.for_full_id(definition_id)?.interface(),
        ))
    }
}

#[async_trait]
impl<E, C, S> ManageChannels for WorkflowManager<E, C, S>
where
    E: WorkflowEngine,
    C: Clock,
    S: Storage,
{
    fn closed_receiver(&self) -> HostChannelId {
        HostChannelId::closed()
    }

    fn closed_sender(&self) -> HostChannelId {
        HostChannelId::closed()
    }

    async fn create_channel(&self) -> (HostChannelId, HostChannelId) {
        let mut transaction = self.storage.transaction().await;
        let channel_id = commit_channel(None, &mut transaction).await;
        transaction.commit().await;
        (
            HostChannelId::unchecked(channel_id),
            HostChannelId::unchecked(channel_id),
        )
    }
}

#[async_trait]
impl<E, C, S> ManageWorkflows for WorkflowManager<E, C, S>
where
    E: WorkflowEngine,
    C: Clock,
    S: Storage,
{
    type Spawned<'s, W: WorkflowFn + GetInterface> = WorkflowHandle<'s, W, Self>;
    type Error = anyhow::Error;

    async fn new_workflow_raw(
        &self,
        definition_id: &str,
        args: Vec<u8>,
        handles: UntypedHandles<Host>,
    ) -> Result<Self::Spawned<'_, ()>, Self::Error> {
        let channel_ids = handles.into_iter().map(|(path, handle)| {
            let id = handle
                .map_receiver(ChannelId::from)
                .map_sender(ChannelId::from);
            (path, id)
        });
        let mut stubs = Stubs::new(None, self.shared());
        stubs.stash_workflow(0, definition_id, args, channel_ids.collect());
        let mut transaction = self.storage.transaction().await;
        let workflow_id = stubs.commit_external(&mut transaction).await?;
        transaction.commit().await;
        Ok(self.workflow(workflow_id).await.unwrap())
    }

    fn downcast<'sp, W: WorkflowFn + GetInterface>(
        &self,
        spawned: Self::Spawned<'sp, ()>,
    ) -> Self::Spawned<'sp, W> {
        spawned.downcast_unchecked()
    }
}
