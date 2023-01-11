//! Workflow persistence.

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{
    data::{PersistError, PersistedWorkflowData, ReceiverState, SenderState},
    engine::{DefineWorkflow, PersistWorkflow},
    manager::Services,
    utils::Message,
    workflow::Workflow,
};
use tardigrade::{handle::Handle, ChannelId};

/// Persisted version of a workflow containing the state of its external dependencies
/// (channels and timers), and its linear WASM memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedWorkflow {
    data: PersistedWorkflowData,
    engine_data: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    args: Option<Message>,
}

impl PersistedWorkflow {
    pub(super) fn new<T>(workflow: &mut Workflow<T>) -> Result<Self, PersistError>
    where
        T: PersistWorkflow,
    {
        workflow.inner.data().check_persistence()?;
        let engine_data = workflow.inner.persist();
        let engine_data = serde_json::to_value(&engine_data).expect("cannot serialize engine data");
        Ok(Self {
            data: workflow.inner.data().persist(),
            engine_data,
            args: workflow.args.clone(),
        })
    }

    pub(crate) fn data_mut(&mut self) -> &mut PersistedWorkflowData {
        &mut self.data
    }

    pub(crate) fn receivers(&self) -> impl Iterator<Item = (ChannelId, &ReceiverState)> + '_ {
        self.data.receivers()
    }

    pub(crate) fn receiver(&self, channel_id: ChannelId) -> Option<&ReceiverState> {
        self.data.receiver(channel_id)
    }

    pub(crate) fn senders(&self) -> impl Iterator<Item = (ChannelId, &SenderState)> + '_ {
        self.data.senders()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn drop_message_for_receiver(&mut self, channel_id: ChannelId) {
        self.data.drop_message_for_receiver(channel_id);
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn close_channel(&mut self, channel_id: Handle<ChannelId>) {
        match channel_id {
            Handle::Receiver(id) => self.data.close_receiver(id),
            Handle::Sender(id) => self.data.close_sender(id),
        }
    }

    /// Returns the engine-independent data persisted for this workflow.
    pub fn common(&self) -> &PersistedWorkflowData {
        &self.data
    }

    /// Checks whether the workflow is initialized.
    pub fn is_initialized(&self) -> bool {
        self.args.is_none()
    }

    /// Aborts the workflow by changing the result of its main task.
    pub(crate) fn abort(&mut self) {
        self.data.abort();
    }

    /// Restores a workflow from the persisted state and the `definition` of the workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow definition from `module` and the `persisted` state
    /// do not match (e.g., differ in defined channels).
    pub(crate) fn restore<D: DefineWorkflow>(
        self,
        definition: &D,
        services: Services,
    ) -> anyhow::Result<Workflow<D::Instance>> {
        let interface = definition.interface();
        let data = self
            .data
            .restore(interface, services)
            .context("failed restoring workflow state")?;

        let engine_data =
            serde_json::from_value(self.engine_data).context("failed deserializing engine data")?;
        let mut workflow = Workflow::new(definition, data, self.args)?;
        workflow.inner.restore(engine_data)?;
        Ok(workflow)
    }
}
