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

use super::{AsManager, CachedDefinitions, Definitions, StashStub};
use crate::{
    data::WorkflowData,
    engine::{DefineWorkflow, WorkflowEngine},
    handle::{RawMessageReceiver, RawMessageSender, StorageRef, WorkflowHandle},
    receipt::Receipt,
    storage::{
        helper::{self, ChannelSide, StorageHelper},
        ActiveWorkflowState, ChannelRecord, ReadModules, ReadonlyStorageTransaction, Storage,
        StorageTransaction, WorkflowRecord, WorkflowWaker,
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
    async fn spawn<E: WorkflowEngine>(
        mut self,
        definitions: &mut Definitions<'_, E>,
        transaction: &impl ReadModules,
    ) -> anyhow::Result<(PersistedWorkflow, ChannelIds)> {
        let (module_id, name_in_module) =
            CachedDefinitions::split_full_id(&self.definition_id).unwrap();
        let definition = definitions
            .get(module_id, name_in_module, transaction)
            .await
            .ok_or_else(|| anyhow!("definition `{}` not found", self.definition_id))?;

        definition
            .interface()
            .check_shape(&self.channel_ids, true)
            .context("invalid shape of provided handles")?;

        let args = mem::take(&mut self.args);
        let channel_ids = mem::take(&mut self.channel_ids);
        let data = WorkflowData::new(definition.interface(), channel_ids.clone());
        let mut workflow = Workflow::new(definition.as_ref(), data, Some(args.into()))?;
        let persisted = workflow.persist();
        Ok((persisted, channel_ids))
    }
}

/// Storage for channel / workflow stubs created by a transaction.
#[derive(Debug)]
pub(super) struct Stubs {
    executing_workflow_id: Option<WorkflowId>,
    definition_requests: HashMap<u64, String>,
    new_workflows: HashMap<WorkflowId, WorkflowStub>,
    new_channels: HashSet<ChannelId>,
}

impl Stubs {
    pub fn new(executing_workflow_id: Option<WorkflowId>) -> Self {
        Self {
            executing_workflow_id,
            definition_requests: HashMap::new(),
            new_workflows: HashMap::new(),
            new_channels: HashSet::new(),
        }
    }

    #[tracing::instrument(level = "debug", skip(definitions, transaction))]
    async fn resolve_definition<E, T>(
        stub_id: u64,
        definition_id: &str,
        definitions: &mut Definitions<'_, E>,
        transaction: &T,
    ) -> Option<Interface>
    where
        E: WorkflowEngine,
        T: StorageTransaction,
    {
        let Some((module_id, name_in_module)) = CachedDefinitions::split_full_id(definition_id) else {
            tracing::warn!(definition_id, "malformed definition ID");
            return None;
        };
        let interface = definitions
            .get(module_id, name_in_module, transaction)
            .await
            .map(|def| def.interface().clone());
        tracing::debug!(ret.is_some = interface.is_some());
        interface
    }

    pub async fn commit<E, T>(
        self,
        mut definitions: Definitions<'_, E>,
        transaction: &mut T,
        parent: &mut Workflow<E::Instance>,
        receipt: &mut Receipt,
    ) where
        E: WorkflowEngine,
        T: StorageTransaction,
    {
        for (stub_id, def) in self.definition_requests {
            let result =
                Self::resolve_definition(stub_id, &def, &mut definitions, transaction).await;
            parent.notify_on_definition_resolved(stub_id, result, receipt);
        }

        let executed_workflow_id = self.executing_workflow_id;
        for stub_id in self.new_channels {
            let (channel_id, _) = helper::commit_channel(executed_workflow_id, transaction).await;
            parent.notify_on_channel_init(stub_id, channel_id, receipt);
        }

        for (stub_id, child_stub) in self.new_workflows {
            let result = Self::commit_child(
                executed_workflow_id,
                &mut definitions,
                transaction,
                child_stub,
            );
            let result = result.await.map_err(|err| HostError::new(err.to_string()));
            parent.notify_on_child_init(stub_id, result, receipt);
        }
    }

    #[inline]
    async fn do_commit_external<E, T>(
        self,
        mut definitions: Definitions<'_, E>,
        mut transaction: T,
        senders_to_close: Vec<ChannelId>,
    ) -> anyhow::Result<WorkflowId>
    where
        E: WorkflowEngine,
        T: StorageTransaction,
    {
        debug_assert!(self.executing_workflow_id.is_none());
        debug_assert_eq!(self.new_workflows.len(), 1);

        let (_, child_stub) = self.new_workflows.into_iter().next().unwrap();
        let child = Self::commit_child(None, &mut definitions, &mut transaction, child_stub);
        let child = child.await.map(|ids| ids.workflow_id);

        if child.is_ok() {
            for channel_id in senders_to_close {
                StorageHelper::new(&mut transaction)
                    .close_channel_side(channel_id, ChannelSide::HostSender)
                    .await;
            }
            transaction.commit().await;
        }
        child
    }

    fn commit_external<'a, E, T>(
        self,
        definitions: Definitions<'a, E>,
        transaction: T,
        senders_to_close: Vec<ChannelId>,
    ) -> impl Future<Output = anyhow::Result<WorkflowId>> + Send + 'a
    where
        E: WorkflowEngine,
        T: 'a + StorageTransaction,
    {
        self.do_commit_external(definitions, transaction, senders_to_close)
    }

    async fn commit_child<E, T>(
        executed_workflow_id: Option<WorkflowId>,
        definitions: &mut Definitions<'_, E>,
        transaction: &mut T,
        child_stub: WorkflowStub,
    ) -> anyhow::Result<WorkflowAndChannelIds>
    where
        E: WorkflowEngine,
        T: StorageTransaction,
    {
        let (module_id, name_in_module) =
            CachedDefinitions::split_full_id(&child_stub.definition_id).ok_or_else(|| {
                anyhow!(
                    "malformed definition ID `{}`; expected one like `module_id::name_in_module`",
                    child_stub.definition_id
                )
            })?;
        let module_id = module_id.to_owned();
        let name_in_module = name_in_module.to_owned();

        let (mut persisted, channel_ids) = child_stub.spawn(definitions, transaction).await?;
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

        let state = ActiveWorkflowState {
            persisted,
            tracing_spans: PersistedSpans::default(),
        };
        let child_workflow = WorkflowRecord {
            id: child_id,
            parent_id: executed_workflow_id,
            module_id,
            name_in_module,
            execution_count: 0,
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

impl StashStub for Stubs {
    #[tracing::instrument(skip(self))]
    fn stash_definition(&mut self, stub_id: u64, definition_id: &str) {
        self.definition_requests
            .insert(stub_id, definition_id.to_owned());
    }

    #[tracing::instrument(skip(self, args), fields(args.len = args.len()))]
    fn stash_workflow(
        &mut self,
        stub_id: WorkflowId,
        definition_id: &str,
        args: Vec<u8>,
        channel_ids: ChannelIds,
    ) {
        self.new_workflows.insert(
            stub_id,
            WorkflowStub {
                definition_id: definition_id.to_owned(),
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

/// Specialized handle to a [`WorkflowManager`] allowing to spawn new workflows.
///
/// [`WorkflowManager`]: crate::manager::WorkflowManager
#[derive(Debug)]
pub struct ManagerSpawner<'a, M> {
    inner: &'a M,
    close_senders: bool,
}

impl<M> Clone for ManagerSpawner<'_, M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            close_senders: self.close_senders,
        }
    }
}

impl<M> Copy for ManagerSpawner<'_, M> {}

impl<'a, M: AsManager> ManagerSpawner<'a, M> {
    pub(super) fn new(inner: &'a M) -> Self {
        Self {
            inner,
            close_senders: false,
        }
    }

    /// Instructs the spawner to close senders from the host side for the channels supplied
    /// to the new workflows. By default, senders are copied to the workflow; i.e., the host
    /// retains a sender handle after a workflow is created.
    #[must_use]
    pub fn close_senders(mut self) -> Self {
        self.close_senders = true;
        self
    }
}

#[async_trait]
impl<M: AsManager> ManageInterfaces for ManagerSpawner<'_, M> {
    async fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>> {
        #[allow(clippy::manual_async_fn)]
        #[inline]
        fn get_definition<'a, E, T>(
            mut definitions: Definitions<'a, E>,
            module_id: &'a str,
            name_in_module: &'a str,
            transaction: T,
        ) -> impl Future<Output = Option<Arc<E::Definition>>> + Send + 'a
        where
            E: WorkflowEngine,
            T: ReadonlyStorageTransaction + 'a,
        {
            async move {
                definitions
                    .get(module_id, name_in_module, &transaction)
                    .await
            }
        }

        let (module_id, name_in_module) = CachedDefinitions::split_full_id(definition_id).unwrap();

        let manager = self.inner.as_manager();
        let definitions = manager.definitions().await;
        let definition = manager.storage.readonly_transaction().then(|transaction| {
            get_definition(definitions, module_id, name_in_module, transaction)
        });
        let definition = definition.await?;
        Some(Cow::Owned(definition.interface().clone()))
    }
}

#[async_trait]
impl<'a, M: AsManager> CreateChannel for ManagerSpawner<'a, M> {
    type Fmt = StorageRef<'a, M::Storage>;

    fn closed_receiver(&self) -> RawMessageReceiver<&'a M::Storage> {
        let manager = self.inner.as_manager();
        manager.storage().closed_receiver()
    }

    fn closed_sender(&self) -> RawMessageSender<&'a M::Storage> {
        let manager = self.inner.as_manager();
        manager.storage().closed_sender()
    }

    async fn new_channel(
        &self,
    ) -> (
        RawMessageSender<&'a M::Storage>,
        RawMessageReceiver<&'a M::Storage>,
    ) {
        let manager = self.inner.as_manager();
        manager.storage().new_channel().await
    }
}

impl<'a, M: AsManager> CreateWorkflow for ManagerSpawner<'a, M> {
    type Spawned<W: WorkflowFn + WithHandle> = WorkflowHandle<W, &'a M::Storage>;
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
        let channel_ids = channel_ids.0;

        let senders_to_close: Vec<_> = if self.close_senders {
            let senders_to_close = channel_ids.values().filter_map(|&handle| {
                if let Handle::Sender(id) = handle {
                    Some(id)
                } else {
                    None
                }
            });
            senders_to_close.collect()
        } else {
            vec![]
        };

        let manager = self.inner.as_manager();
        let mut stubs = Stubs::new(None);
        stubs.stash_workflow(0, definition_id, raw_args, channel_ids);
        async move {
            // TODO: This borrows cached definitions for too long
            let definitions = manager.definitions().await;
            let workflow_id = manager
                .storage
                .transaction()
                .then(|transaction| {
                    stubs.commit_external(definitions, transaction, senders_to_close)
                })
                .await?;

            Ok(manager
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
