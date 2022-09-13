//! Transactions for `WorkflowManager`.

use std::{borrow::Cow, collections::HashMap, sync::Mutex};

use super::{
    persistence::{PersistedWorkflows, WorkflowWithMeta},
    Shared,
};
use crate::{
    module::{NoOpWorkflowManager, Services, WorkflowAndChannelIds},
    workflow::{ChannelIds, Workflow},
};
use tardigrade::{
    interface::Interface,
    spawn::{ChannelHandles, ManageInterfaces, ManageWorkflows},
};
use tardigrade_shared::{ChannelId, SpawnError, WorkflowId};

#[derive(Debug)]
pub(super) struct TransactionInner {
    pub next_channel_id: ChannelId,
    pub next_workflow_id: WorkflowId,
    pub new_workflows: HashMap<WorkflowId, WorkflowWithMeta>,
}

impl TransactionInner {
    fn allocate_channel_id(&mut self) -> ChannelId {
        let channel_id = self.next_channel_id;
        self.next_channel_id += 1;
        channel_id
    }

    fn stash_workflow(
        &mut self,
        definition_id: String,
        parent_id: Option<WorkflowId>,
        mut workflow: Workflow,
    ) -> WorkflowId {
        debug_assert!(!workflow.is_initialized());

        let id = self.next_workflow_id;
        self.next_workflow_id += 1;
        self.new_workflows.insert(
            id,
            WorkflowWithMeta {
                definition_id,
                parent_id,
                workflow: workflow.persist().unwrap(),
            },
        );
        id
    }
}

#[derive(Debug)]
pub(super) struct Transaction {
    executing_workflow_id: Option<WorkflowId>,
    shared: Shared,
    inner: Mutex<TransactionInner>,
}

impl Transaction {
    pub fn new(
        persisted: &PersistedWorkflows,
        executing_workflow_id: Option<WorkflowId>,
        shared: Shared,
    ) -> Self {
        Self {
            executing_workflow_id,
            shared,
            inner: Mutex::new(TransactionInner {
                next_channel_id: persisted.next_channel_id,
                next_workflow_id: persisted.next_workflow_id,
                new_workflows: HashMap::new(),
            }),
        }
    }

    fn services(&self) -> Services<'_> {
        Services {
            clock: self.shared.clock.as_ref(),
            workflows: &NoOpWorkflowManager,
            // ^ `workflows` is not used during instantiation, so it's OK to provide
            // a no-op implementation.
        }
    }

    pub fn executing_workflow_id(&self) -> Option<WorkflowId> {
        self.executing_workflow_id
    }

    pub fn single_new_workflow_id(&self) -> Option<WorkflowId> {
        let state = self.inner.lock().unwrap();
        if state.new_workflows.len() == 1 {
            state.new_workflows.keys().next().copied()
        } else {
            None
        }
    }

    pub fn into_inner(self) -> TransactionInner {
        self.inner.into_inner().unwrap()
    }
}

impl ManageInterfaces for Transaction {
    fn interface(&self, id: &str) -> Option<Cow<'_, Interface<()>>> {
        Some(Cow::Borrowed(self.shared.spawners.get(id)?.interface()))
    }
}

impl ManageWorkflows<'_, ()> for Transaction {
    type Handle = WorkflowAndChannelIds;

    fn create_workflow(
        &self,
        id: &str,
        args: Vec<u8>,
        handles: &ChannelHandles,
    ) -> Result<Self::Handle, SpawnError> {
        let spawner = self
            .shared
            .spawners
            .get(id)
            .unwrap_or_else(|| panic!("workflow with ID `{}` is not defined", id));

        let channel_ids = {
            let mut state = self.inner.lock().unwrap();
            ChannelIds::new(handles, || state.allocate_channel_id())
        };
        let workflow = spawner
            .spawn(args, &channel_ids, self.services())
            .map_err(|err| SpawnError::new(err.to_string()))?;
        let workflow_id = self.inner.lock().unwrap().stash_workflow(
            id.to_owned(),
            self.executing_workflow_id,
            workflow,
        );
        Ok(WorkflowAndChannelIds {
            workflow_id,
            channel_ids,
        })
    }
}
