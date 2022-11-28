//! Mock execution engine.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use std::{
    collections::HashMap,
    iter,
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
};
use tardigrade::{interface::Interface, TaskId, WakerId};

const INTERFACE: &[u8] = br#"{
    "v": 0,
    "in": { "orders": {} },
    "out": { "events": {}, "traces": { "capacity": null } }
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

/// Mock workflow instance.
#[derive(Debug)]
pub struct MockInstance {
    inner: WorkflowData,
    poll_fns: SharedAnswers,
    executing_task_id: Option<TaskId>,
    wakers: HashMap<WakerId, TaskId>,
    next_waker_id: WakerId,
}

impl MockInstance {
    pub const MOCK_MODULE_BYTES: &'static [u8] = b"\0asm\x01\0\0\0";

    fn new(data: WorkflowData, poll_fns: SharedAnswers) -> Self {
        Self {
            inner: data,
            poll_fns,
            executing_task_id: None,
            wakers: HashMap::new(),
            next_waker_id: 0,
        }
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
        let waker_id = self.next_waker_id;
        self.next_waker_id += 1;
        self.wakers
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
        let owning_task_id = self.wakers.remove(&waker_id).unwrap();
        self.inner.schedule_task_wakeup(owning_task_id)?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedMockInstance {
    wakers: HashMap<WakerId, TaskId>,
    next_waker_id: WakerId,
}

impl PersistWorkflow for MockInstance {
    type Persisted = PersistedMockInstance;

    fn persist(&mut self) -> Self::Persisted {
        PersistedMockInstance {
            wakers: self.wakers.clone(),
            next_waker_id: self.next_waker_id,
        }
    }

    fn restore(&mut self, persisted: Self::Persisted) -> anyhow::Result<()> {
        self.wakers = persisted.wakers;
        self.next_waker_id = persisted.next_waker_id;
        Ok(())
    }
}
