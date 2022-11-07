//! Transactions for `WorkflowManager`.

use tracing_tunnel::PersistedSpans;

use std::{borrow::Cow, collections::HashMap, mem};

use super::{Shared, WorkflowAndChannelIds};
use crate::{
    module::{Services, StashWorkflows},
    storage::{ChannelState, WorkflowRecord, WriteChannels, WriteWorkflows},
    workflow::{ChannelIds, PersistedWorkflow},
};
use tardigrade::{
    interface::Interface,
    spawn::{ChannelSpawnConfig, ChannelsConfig, HostError, ManageInterfaces},
    ChannelId, WorkflowId,
};

impl ChannelState {
    fn new(
        sender_workflow_id: Option<WorkflowId>,
        receiver_workflow_id: Option<WorkflowId>,
    ) -> Self {
        Self {
            receiver_workflow_id,
            sender_workflow_ids: sender_workflow_id.into_iter().collect(),
            has_external_sender: sender_workflow_id.is_none(),
            is_closed: false,
        }
    }
}

impl ChannelIds {
    async fn new<T: WriteChannels>(
        channels: ChannelsConfig<ChannelId>,
        persistence: &mut T,
    ) -> Self {
        Self {
            inbound: Self::map_channels(channels.inbound, persistence).await,
            outbound: Self::map_channels(channels.outbound, persistence).await,
        }
    }

    async fn map_channels<T: WriteChannels>(
        config: HashMap<String, ChannelSpawnConfig<ChannelId>>,
        persistence: &mut T,
    ) -> HashMap<String, ChannelId> {
        let mut channel_ids = HashMap::with_capacity(config.len());
        for (name, spec) in config {
            let channel_id = match spec {
                ChannelSpawnConfig::New => persistence.allocate_channel_id().await,
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
    async fn spawn<T: WriteChannels>(
        &mut self,
        shared: &Shared,
        persistence: &mut T,
    ) -> anyhow::Result<(PersistedWorkflow, ChannelIds)> {
        let spawner = &shared.spawners[&self.definition_id];
        let services = Services {
            clock: shared.clock.as_ref(),
            workflows: None,
            tracer: None,
        };
        let channel_ids = ChannelIds::new(self.channels.clone(), persistence).await;
        let args = mem::take(&mut self.args);
        let workflow = spawner.spawn(args, &channel_ids, services)?;
        let persisted = workflow.persist();
        Ok((persisted, channel_ids))
    }
}

#[derive(Debug)]
pub(super) struct NewWorkflows {
    executing_workflow_id: Option<WorkflowId>,
    shared: Shared,
    new_workflows: HashMap<WorkflowId, WorkflowStub>,
}

impl NewWorkflows {
    pub fn new(executing_workflow_id: Option<WorkflowId>, shared: Shared) -> Self {
        Self {
            executing_workflow_id,
            shared,
            new_workflows: HashMap::new(),
        }
    }

    pub async fn commit<T: WriteChannels + WriteWorkflows>(
        self,
        persistence: &mut T,
        parent: &mut PersistedWorkflow,
    ) {
        let executed_workflow_id = self.executing_workflow_id;
        // Create new channels and write outbound messages for them when appropriate.
        for (stub_id, mut child_stub) in self.new_workflows {
            let result = Self::commit_child(
                executed_workflow_id,
                &self.shared,
                persistence,
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

    pub async fn commit_external<T: WriteChannels + WriteWorkflows>(
        self,
        persistence: &mut T,
    ) -> anyhow::Result<WorkflowId> {
        debug_assert!(self.executing_workflow_id.is_none());
        debug_assert_eq!(self.new_workflows.len(), 1);
        let (_, mut child_stub) = self.new_workflows.into_iter().next().unwrap();
        let child = Self::commit_child(None, &self.shared, persistence, &mut child_stub);
        Ok(child.await?.workflow_id)
    }

    async fn commit_child<T: WriteChannels + WriteWorkflows>(
        executed_workflow_id: Option<WorkflowId>,
        shared: &Shared,
        persistence: &mut T,
        child_stub: &mut WorkflowStub,
    ) -> anyhow::Result<WorkflowAndChannelIds> {
        let (mut persisted, channel_ids) = child_stub.spawn(shared, persistence).await?;

        let child_id = persistence.allocate_workflow_id().await;

        tracing::debug!(?channel_ids, "handling channels for new workflow");
        for (name, &channel_id) in &channel_ids.inbound {
            let state = ChannelState::new(executed_workflow_id, Some(child_id));
            let state = persistence.get_or_insert_channel(channel_id, state).await;
            if state.is_closed {
                persisted.close_inbound_channel(None, name);
            }
            tracing::debug!(name, channel_id, ?state, "prepared inbound channel");
        }
        for (name, &channel_id) in &channel_ids.outbound {
            let state = ChannelState::new(Some(child_id), executed_workflow_id);
            let state = persistence.get_or_insert_channel(channel_id, state).await;
            if state.is_closed {
                persisted.close_outbound_channel(None, name);
            } else {
                persistence
                    .manipulate_channel(channel_id, |channel| {
                        channel.sender_workflow_ids.insert(child_id);
                    })
                    .await;
            }
            tracing::debug!(name, channel_id, ?state, "prepared outbound channel");
        }

        let child_workflow = WorkflowRecord {
            id: child_id,
            parent_id: executed_workflow_id,
            module_id: mem::take(&mut child_stub.definition_id),
            name_in_module: String::new(), // FIXME
            persisted,
            tracing_spans: PersistedSpans::default(),
        };
        persistence.create_workflow(child_workflow).await;

        Ok(WorkflowAndChannelIds {
            workflow_id: child_id,
            channel_ids,
        })
    }
}

impl ManageInterfaces for NewWorkflows {
    fn interface(&self, id: &str) -> Option<Cow<'_, Interface>> {
        Some(Cow::Borrowed(self.shared.spawners.get(id)?.interface()))
    }
}

impl StashWorkflows for NewWorkflows {
    #[tracing::instrument(skip(self, args), fields(args.len = args.len()))]
    fn stash_workflow(
        &mut self,
        stub_id: WorkflowId,
        id: &str,
        args: Vec<u8>,
        channels: ChannelsConfig<ChannelId>,
    ) {
        debug_assert!(
            self.shared.spawners.get(id).is_some(),
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
