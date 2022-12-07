//! Mock execution engine.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use std::{
    collections::HashMap,
    iter, mem,
    sync::{Arc, Mutex},
    task::Poll,
};

use crate::{
    data::WorkflowData,
    engine::{
        AsWorkflowData, CreateWaker, DefineWorkflow, PersistWorkflow, RunWorkflow, WorkflowEngine,
        WorkflowModule,
    },
    storage::ModuleRecord,
    workflow::ChannelIds,
};
use tardigrade::{
    handle::Handle, interface::Interface, spawn::HostError, ChannelId, TaskId, WakerId, WorkflowId,
};

const INTERFACE: &[u8] = br#"{
    "v": 0,
    "handles": {
        "orders": { "receiver": {} },
        "events": { "sender": {} },
        "traces": { "sender": { "capacity": null } }
    }
}"#;

pub type MockPollFn = fn(&mut MockInstance) -> anyhow::Result<Poll<()>>;
pub type MockAnswers = mimicry::Answers<MockPollFn, TaskId>;
type SharedAnswers = Arc<Mutex<MockAnswers>>;

/// Mock workflow execution engine.
#[derive(Debug)]
pub struct MockEngine {
    poll_fns: SharedAnswers,
}

impl MockEngine {
    pub fn new(poll_fns: MockAnswers) -> Self {
        Self {
            poll_fns: Arc::new(Mutex::new(poll_fns)),
        }
    }
}

#[async_trait]
impl WorkflowEngine for MockEngine {
    type Instance = MockInstance;
    type Definition = MockDefinition;
    type Module = MockModule;

    async fn create_module(&self, _record: &ModuleRecord) -> anyhow::Result<Self::Module> {
        Ok(MockModule {
            poll_fns: Arc::clone(&self.poll_fns),
        })
    }
}

/// Mock workflow module.
#[derive(Debug)]
pub struct MockModule {
    poll_fns: SharedAnswers,
}

impl IntoIterator for MockModule {
    type Item = (String, MockDefinition);
    type IntoIter = iter::Once<(String, MockDefinition)>;

    fn into_iter(self) -> Self::IntoIter {
        let name = "TestWorkflow".to_owned();
        let interface = Interface::from_bytes(INTERFACE);
        let definition = MockDefinition {
            poll_fns: Arc::clone(&self.poll_fns),
            interface,
        };
        iter::once((name, definition))
    }
}

impl WorkflowModule for MockModule {
    type Definition = MockDefinition;

    fn bytes(&self) -> Arc<[u8]> {
        Arc::new([])
    }
}

/// Mock workflow definition.
#[derive(Debug)]
pub struct MockDefinition {
    poll_fns: SharedAnswers,
    interface: Interface,
}

impl MockDefinition {
    pub fn new(poll_fns: MockAnswers) -> Self {
        Self {
            poll_fns: Arc::new(Mutex::new(poll_fns)),
            interface: Interface::from_bytes(INTERFACE),
        }
    }
}

impl DefineWorkflow for MockDefinition {
    type Instance = MockInstance;

    fn interface(&self) -> &Interface {
        &self.interface
    }

    fn create_workflow(&self, data: WorkflowData) -> anyhow::Result<Self::Instance> {
        Ok(MockInstance::new(data, Arc::clone(&self.poll_fns)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChildStub {
    local_ids: ChannelIds,
    owning_task_id: TaskId,
}

/// Mock workflow instance.
#[derive(Debug)]
pub struct MockInstance {
    inner: WorkflowData,
    poll_fns: SharedAnswers,
    executing_task_id: Option<TaskId>,
    persisted: PersistedMockInstance,
}

impl MockInstance {
    pub const MOCK_MODULE_BYTES: &'static [u8] = b"\0asm\x01\0\0\0";

    fn new(data: WorkflowData, poll_fns: SharedAnswers) -> Self {
        Self {
            inner: data,
            poll_fns,
            executing_task_id: None,
            persisted: PersistedMockInstance::new(),
        }
    }

    /// Prepares new channels for a child workflow.
    ///
    /// # Errors
    ///
    /// Propagates error that might occur during channel creation.
    #[allow(clippy::missing_panics_doc)]
    pub fn create_channels_for_stub(
        &mut self,
        stub_id: WorkflowId,
        local_ids: ChannelIds,
    ) -> anyhow::Result<()> {
        assert!(!self.persisted.child_stubs.contains_key(&stub_id));

        for id_handle in local_ids.values() {
            let local_id = id_handle.as_ref().factor();
            self.inner.create_channel_stub(*local_id)?;
        }
        self.persisted.child_stubs.insert(
            stub_id,
            ChildStub {
                local_ids,
                owning_task_id: 0,
            },
        );
        Ok(())
    }

    /// Takes [previously created](Self::create_channels_for_stub()) channels so that
    /// can be used (perhaps, with closed / existing channels) to spawn a child workflow.
    #[allow(clippy::missing_panics_doc)]
    pub fn take_stub_channels(&mut self, stub_id: WorkflowId) -> ChannelIds {
        let stub = self.persisted.child_stubs.get_mut(&stub_id).unwrap();
        let local_ids = mem::take(&mut stub.local_ids);
        let mapping = &self.persisted.channel_mapping;
        let ids = local_ids.into_iter().map(|(path, id_handle)| {
            let mapped_id = id_handle.map(|local_id| mapping[&local_id]);
            // Emulate dropping the channels moved to the child workflows.
            let _wakers = match mapped_id {
                Handle::Receiver(id) => self.inner.receiver(id).drop(),
                Handle::Sender(id) => self.inner.sender(id).drop(),
            };
            (path, mapped_id)
        });
        ids.collect()
    }
}

impl AsWorkflowData for MockInstance {
    fn data(&self) -> &WorkflowData {
        &self.inner
    }

    fn data_mut(&mut self) -> &mut WorkflowData {
        &mut self.inner
    }
}

impl CreateWaker for MockInstance {
    fn create_waker(&mut self) -> anyhow::Result<WakerId> {
        let waker_id = self.persisted.next_waker_id;
        self.persisted.next_waker_id += 1;
        self.persisted
            .wakers
            .insert(waker_id, self.executing_task_id.unwrap());
        Ok(waker_id)
    }
}

impl RunWorkflow for MockInstance {
    fn create_main_task(&mut self, _raw_args: &[u8]) -> anyhow::Result<TaskId> {
        Ok(0)
    }

    fn poll_task(&mut self, task_id: TaskId) -> anyhow::Result<Poll<()>> {
        let poll_fn = self.poll_fns.lock().unwrap().next_for(task_id);
        self.executing_task_id = Some(task_id);
        let result = poll_fn(self);
        self.executing_task_id = None;
        result
    }

    fn drop_task(&mut self, _task_id: TaskId) -> anyhow::Result<()> {
        Ok(())
    }

    fn wake_waker(&mut self, waker_id: WakerId) -> anyhow::Result<()> {
        let owning_task_id = self.persisted.wakers.remove(&waker_id).unwrap();
        self.inner.task(owning_task_id).schedule_wakeup();
        Ok(())
    }

    fn initialize_child(&mut self, local_id: WorkflowId, _result: Result<WorkflowId, HostError>) {
        if let Some(stub) = self.persisted.child_stubs.get_mut(&local_id) {
            self.inner.task(stub.owning_task_id).schedule_wakeup();
        } else {
            // Assume that the manually created stub was created from the main (0th) task.
            self.inner.task(0).schedule_wakeup();
        }
    }

    fn initialize_channel(&mut self, local_id: ChannelId, channel_id: ChannelId) {
        self.persisted.channel_mapping.insert(local_id, channel_id);
        // Assume that the manually created stub was created from the main (0th) task.
        self.inner.task(0).schedule_wakeup();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedMockInstance {
    wakers: HashMap<WakerId, TaskId>,
    next_waker_id: WakerId,
    child_stubs: HashMap<WorkflowId, ChildStub>,
    /// Mapping from local to global channel IDs.
    channel_mapping: HashMap<ChannelId, ChannelId>,
}

impl PersistedMockInstance {
    fn new() -> Self {
        Self {
            wakers: HashMap::new(),
            next_waker_id: 0,
            child_stubs: HashMap::new(),
            channel_mapping: HashMap::new(),
        }
    }
}

impl PersistWorkflow for MockInstance {
    type Persisted = PersistedMockInstance;

    fn persist(&mut self) -> Self::Persisted {
        self.persisted.clone()
    }

    fn restore(&mut self, persisted: Self::Persisted) -> anyhow::Result<()> {
        self.persisted = persisted;
        Ok(())
    }
}
