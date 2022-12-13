//! Transactions for `WorkflowManager`.

use anyhow::{anyhow, ensure, Context as _};
use async_trait::async_trait;
use futures::future::{BoxFuture, Future, FutureExt};
use tracing_tunnel::PersistedSpans;

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt::Write as _,
    mem,
    sync::Arc,
};

use super::{Clock, Services, Shared, StashStub, WorkflowDefinitions, WorkflowManager};
use crate::{
    data::WorkflowData,
    engine::{DefineWorkflow, RunWorkflow, WorkflowEngine},
    handle::{RawMessageReceiver, RawMessageSender, StorageRef, WorkflowHandle},
    receipt::Receipt,
    storage::{
        helper, ActiveWorkflowState, ChannelRecord, Storage, StorageTransaction, WorkflowRecord,
        WorkflowWaker,
    },
    workflow::{ChannelIds, PersistedWorkflow, Workflow, WorkflowAndChannelIds},
};
use tardigrade::{
    handle::{Handle, HandlePath, HandlePathBuf},
    interface::Interface,
    spawn::{CreateChannel, CreateWorkflow, HostError, ManageInterfaces},
    workflow::{InsertHandles, WithHandle, WorkflowFn},
    ChannelId, Codec, WorkflowId,
};

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
            let (channel_id, _) = helper::commit_channel(executed_workflow_id, transaction).await;
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

    #[inline]
    async fn do_commit_external<T: StorageTransaction>(
        self,
        mut transaction: T,
    ) -> anyhow::Result<WorkflowId> {
        debug_assert!(self.executing_workflow_id.is_none());
        debug_assert_eq!(self.new_workflows.len(), 1);
        let (_, mut child_stub) = self.new_workflows.into_iter().next().unwrap();
        let child = Self::commit_child(None, &self.shared, &mut transaction, &mut child_stub);
        let child = child.await.map(|ids| ids.workflow_id);
        if child.is_ok() {
            transaction.commit().await;
        }
        child
    }

    fn commit_external<'a, T: 'a + StorageTransaction>(
        self,
        transaction: T,
    ) -> impl Future<Output = anyhow::Result<WorkflowId>> + Send + 'a {
        self.do_commit_external(transaction)
    }

    async fn commit_child<T: StorageTransaction>(
        executed_workflow_id: Option<WorkflowId>,
        shared: &Shared<S>,
        transaction: &mut T,
        child_stub: &mut WorkflowStub,
    ) -> anyhow::Result<WorkflowAndChannelIds> {
        let (mut persisted, channel_ids) = child_stub.spawn(shared)?;
        let child_id = transaction.allocate_workflow_id().await;

        Self::validate_duplicate_channels(&channel_ids)?;

        tracing::debug!(?channel_ids, "handling channels for new workflow");
        for (path, &id_res) in &channel_ids {
            let channel_id = id_res.factor();
            let state = transaction.channel(channel_id).await;
            let state = state.ok_or_else(|| {
                anyhow!("channel {channel_id} specified at `{path}` does not exist")
            })?;
            Self::validate_channel_ownership(path, id_res, &state, executed_workflow_id)?;

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

    fn validate_duplicate_channels(channel_ids: &ChannelIds) -> anyhow::Result<()> {
        let mut unique_receiver_ids: HashMap<_, usize> =
            HashMap::with_capacity(channel_ids.len() / 2);
        for &id_handle in channel_ids.values() {
            if let Handle::Receiver(id) = id_handle {
                if id != 0 {
                    // The 0th channel can be aliased as closed.
                    *unique_receiver_ids.entry(id).or_default() += 1;
                }
            }
        }

        let mut bogus_ids = unique_receiver_ids
            .into_iter()
            .filter_map(|(id, count)| (count > 1).then_some(id))
            .fold(String::new(), |mut acc, id| {
                write!(&mut acc, "{id}, ").unwrap();
                acc
            });
        if bogus_ids.is_empty() {
            Ok(())
        } else {
            bogus_ids.truncate(bogus_ids.len() - 2); // truncate the trailing ", "
            Err(anyhow!(
                "receivers are aliased for channel(s) {{{bogus_ids}}}"
            ))
        }
    }

    fn validate_channel_ownership(
        path: &HandlePathBuf,
        id: Handle<ChannelId>,
        state: &ChannelRecord,
        executed_workflow_id: Option<WorkflowId>,
    ) -> anyhow::Result<()> {
        match id {
            Handle::Receiver(id) if id != 0 => {
                ensure!(
                    state.receiver_workflow_id == executed_workflow_id,
                    "receiver for channel {id} at `{path}` is not owned by requester"
                );
            }
            Handle::Sender(id) if id != 0 => {
                let is_owned = executed_workflow_id
                    .map_or(state.has_external_sender, |workflow_id| {
                        state.sender_workflow_ids.contains(&workflow_id)
                    });
                ensure!(
                    is_owned,
                    "sender for channel {id} at `{path}` is not owned by requester"
                );
            }
            _ => { /* 0th channel doesn't have "real" ownership */ }
        }
        Ok(())
    }
}

impl<S: DefineWorkflow> ManageInterfaces for Stubs<S> {
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
        definition_id: &str,
        args: Vec<u8>,
        channel_ids: ChannelIds,
    ) -> anyhow::Result<()> {
        let Some(definition) = self.shared.definitions.for_full_id(definition_id) else {
            return Err(anyhow!("definition with ID `{definition_id}` is not defined"));
        };
        definition
            .interface()
            .check_shape(&channel_ids, true)
            .context("invalid shape of provided handles")?;

        self.new_workflows.insert(
            stub_id,
            WorkflowStub {
                definition_id: definition_id.to_owned(),
                args,
                channel_ids,
            },
        );
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn stash_channel(&mut self, stub_id: ChannelId) {
        self.new_channels.insert(stub_id);
    }
}

impl<E, C, S> ManageInterfaces for &WorkflowManager<E, C, S>
where
    E: WorkflowEngine,
    C: Clock,
    S: Storage,
{
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        let definition = self.definitions.for_full_id(definition_id)?;
        Some(Cow::Borrowed(definition.interface()))
    }
}

#[async_trait]
impl<'a, E, C, S> CreateChannel for &'a WorkflowManager<E, C, S>
where
    E: WorkflowEngine,
    C: Clock,
    S: Storage,
{
    type Fmt = StorageRef<'a, S>;

    fn closed_receiver(&self) -> RawMessageReceiver<&'a S> {
        self.storage().closed_receiver()
    }

    fn closed_sender(&self) -> RawMessageSender<&'a S> {
        self.storage().closed_sender()
    }

    async fn new_channel(&self) -> (RawMessageSender<&'a S>, RawMessageReceiver<&'a S>) {
        self.storage().new_channel().await
    }
}

impl<'a, E, C, S> CreateWorkflow for &'a WorkflowManager<E, C, S>
where
    E: WorkflowEngine,
    C: Clock,
    S: Storage,
{
    type Spawned<W: WorkflowFn + WithHandle> = WorkflowHandle<W, &'a S>;
    type Error = anyhow::Error;

    fn new_workflow_unchecked<W: WorkflowFn + WithHandle>(
        &self,
        definition_id: &str,
        args: W::Args,
        handles: W::Handle<Self::Fmt>,
    ) -> BoxFuture<'_, Result<Self::Spawned<W>, Self::Error>> {
        let raw_args = <W::Codec>::encode_value(args);
        let mut channel_ids = ChannelIdsCollector::default();
        W::insert_into_untyped(handles, &mut channel_ids, HandlePath::EMPTY);

        let mut stubs = Stubs::new(None, self.shared());
        let result = stubs.stash_workflow(0, definition_id, raw_args, channel_ids.0);
        async move {
            result?;
            let workflow_id = self
                .storage
                .transaction()
                .then(|transaction| stubs.commit_external(transaction))
                .await?;
            Ok(self
                .storage()
                .workflow(workflow_id)
                .await
                .unwrap()
                .downcast_unchecked())
        }
        .boxed()
    }
}

#[derive(Debug, Default)]
struct ChannelIdsCollector(ChannelIds);

impl<'a, S: Storage> InsertHandles<StorageRef<'a, S>> for ChannelIdsCollector {
    fn insert_handle(
        &mut self,
        path: HandlePathBuf,
        handle: Handle<RawMessageReceiver<&'a S>, RawMessageSender<&'a S>>,
    ) {
        let id = handle
            .map_receiver(|handle| handle.channel_id())
            .map_sender(|handle| handle.channel_id());
        self.0.insert(path, id);
    }
}
