//! Spawning workflows.

use serde::{Deserialize, Serialize};
use tracing::field;
use wasmtime::{AsContextMut, ExternRef, StoreContextMut, Trap};

use std::{
    collections::{HashMap, HashSet},
    mem,
    sync::{Arc, Mutex},
    task::Poll,
};

use crate::{
    data::{
        channel::{ChannelStates, InboundChannelState, OutboundChannelState},
        helpers::{HostResource, WakeIfPending, WakerPlacement, WasmContext, WasmContextPtr},
        PersistedWorkflowData, WorkflowData,
    },
    receipt::{ResourceEventKind, ResourceId, WakeUpCause},
    utils::{self, WasmAllocator},
    workflow::ChannelIds,
};
use tardigrade::{
    abi::{IntoWasm, PollTask, TryFromWasm},
    interface::{ChannelKind, Interface},
    spawn::{ChannelSpawnConfig, ChannelsConfig, HostError},
    task::{JoinError, TaskError},
    ChannelId, WakerId, WorkflowId,
};

type ChannelHandles = ChannelsConfig<ChannelId>;
type PollStub = Poll<Result<WorkflowId, HostError>>;

#[derive(Debug, Clone)]
pub(super) struct SharedChannelHandles {
    inner: Arc<Mutex<ChannelHandles>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChildWorkflowStub {
    #[serde(with = "utils::serde_poll")]
    result: Poll<Result<WorkflowId, HostError>>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    wakes_on_init: HashSet<WakerId>,
}

impl Default for ChildWorkflowStub {
    fn default() -> Self {
        Self {
            result: Poll::Pending,
            wakes_on_init: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct ChildWorkflowStubs {
    stubs: HashMap<WorkflowId, ChildWorkflowStub>,
    next_stub_id: WorkflowId,
}

impl ChildWorkflowStubs {
    fn create(&mut self) -> WorkflowId {
        let stub_id = self.next_stub_id;
        self.stubs.insert(stub_id, ChildWorkflowStub::default());
        self.next_stub_id += 1;
        stub_id
    }

    pub fn insert_waker(&mut self, stub_id: WorkflowId, waker: WakerId) {
        let stub = self.stubs.get_mut(&stub_id).unwrap();
        stub.wakes_on_init.insert(waker);
    }
}

/// State of child workflow as viewed by its parent.
#[derive(Debug, Serialize, Deserialize)]
pub struct ChildWorkflowState {
    pub(super) channels: ChannelStates,
    #[serde(with = "utils::serde_poll")]
    completion_result: Poll<Result<(), JoinError>>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    wakes_on_completion: HashSet<WakerId>,
}

impl Clone for ChildWorkflowState {
    fn clone(&self) -> Self {
        Self {
            channels: self.channels.clone(),
            completion_result: utils::clone_completion_result(&self.completion_result),
            wakes_on_completion: self.wakes_on_completion.clone(),
        }
    }
}

impl ChildWorkflowState {
    fn new(channel_ids: &ChannelIds) -> Self {
        Self {
            // FIXME: what is the appropriate capacity?
            channels: ChannelStates::new(channel_ids, |_| Some(1)),
            completion_result: Poll::Pending,
            wakes_on_completion: HashSet::new(),
        }
    }

    /// This is needed to prevent the workflow from capturing non-captured channel handles.
    fn acquire_non_captured_channels(&mut self, channels: &ChannelsConfig<ChannelId>) {
        for (name, config) in &channels.inbound {
            if matches!(config, ChannelSpawnConfig::Existing(_)) {
                let state = self.channels.outbound.get_mut(name).unwrap();
                state.is_acquired = true;
            }
        }
        for (name, config) in &channels.outbound {
            if matches!(config, ChannelSpawnConfig::Existing(_)) {
                let state = self.channels.inbound.get_mut(name).unwrap();
                state.is_acquired = true;
            }
        }
    }

    /// Returns the current poll state of this workflow.
    pub fn result(&self) -> Poll<Result<(), &JoinError>> {
        match &self.completion_result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => Poll::Ready(res.as_ref().copied()),
        }
    }

    /// Returns the current state of a *local* inbound channel connected to the child workflow
    /// (i.e., the child has an outbound end of the channel).
    pub fn inbound_channel(&self, name: &str) -> Option<&InboundChannelState> {
        self.channels.inbound.get(name)
    }

    /// Returns the current state of a *local* outbound channel connected to the child workflow
    /// (i.e., the child has the inbound end of the channel).
    pub fn outbound_channel(&self, name: &str) -> Option<&OutboundChannelState> {
        self.channels.outbound.get(name)
    }

    pub(super) fn insert_waker(&mut self, waker_id: WakerId) {
        self.wakes_on_completion.insert(waker_id);
    }
}

impl PersistedWorkflowData {
    pub fn child_workflows(&self) -> impl Iterator<Item = (WorkflowId, &ChildWorkflowState)> + '_ {
        self.child_workflows.iter().map(|(id, state)| (*id, state))
    }

    pub fn child_workflow(&self, id: WorkflowId) -> Option<&ChildWorkflowState> {
        self.child_workflows.get(&id)
    }

    pub fn notify_on_child_init(
        &mut self,
        stub_id: WorkflowId,
        workflow_id: WorkflowId,
        channels: &ChannelsConfig<ChannelId>,
        mut channel_ids: ChannelIds,
    ) {
        let stub = self.child_workflow_stubs.stubs.get_mut(&stub_id).unwrap();
        stub.result = Poll::Ready(Ok(workflow_id));
        let wakers = mem::take(&mut stub.wakes_on_init);
        self.schedule_wakers(wakers, WakeUpCause::InitWorkflow { stub_id });

        mem::swap(&mut channel_ids.inbound, &mut channel_ids.outbound);
        let mut child_state = ChildWorkflowState::new(&channel_ids);
        child_state.acquire_non_captured_channels(channels);
        self.child_workflows.insert(workflow_id, child_state);
    }

    pub fn notify_on_child_spawn_error(&mut self, stub_id: WorkflowId, err: HostError) {
        let stub = self.child_workflow_stubs.stubs.get_mut(&stub_id).unwrap();
        stub.result = Poll::Ready(Err(err));
        let wakers = mem::take(&mut stub.wakes_on_init);
        self.schedule_wakers(wakers, WakeUpCause::InitWorkflow { stub_id });
    }

    pub fn notify_on_child_completion(&mut self, id: WorkflowId, result: Result<(), JoinError>) {
        let state = self.child_workflows.get_mut(&id).unwrap();
        debug_assert!(state.completion_result.is_pending());
        state.completion_result = Poll::Ready(result);
        let wakers = mem::take(&mut state.wakes_on_completion);
        self.schedule_wakers(wakers, WakeUpCause::CompletedWorkflow(id));
    }
}

impl WorkflowData<'_> {
    fn validate_handles(
        &self,
        definition_id: &str,
        channels: &ChannelsConfig<ChannelId>,
    ) -> Result<(), Trap> {
        let workflows = self.services.workflows.as_deref();
        let interface = workflows.and_then(|workflows| workflows.interface(definition_id));
        if let Some(interface) = interface {
            for (name, _) in interface.inbound_channels() {
                if !channels.inbound.contains_key(name) {
                    let message = format!("missing handle for inbound channel `{}`", name);
                    return Err(Trap::new(message));
                }
            }
            for (name, _) in interface.outbound_channels() {
                if !channels.outbound.contains_key(name) {
                    let message = format!("missing handle for outbound channel `{}`", name);
                    return Err(Trap::new(message));
                }
            }

            if channels.inbound.len() != interface.inbound_channels().len() {
                let err = Self::extra_handles_error(&interface, channels, ChannelKind::Inbound);
                return Err(err);
            }
            if channels.outbound.len() != interface.outbound_channels().len() {
                let err = Self::extra_handles_error(&interface, channels, ChannelKind::Outbound);
                return Err(err);
            }
            Ok(())
        } else {
            Err(Trap::new(format!(
                "workflow with ID `{}` is not defined",
                definition_id
            )))
        }
    }

    fn extra_handles_error(
        interface: &Interface,
        channels: &ChannelsConfig<ChannelId>,
        channel_kind: ChannelKind,
    ) -> Trap {
        use std::fmt::Write as _;

        let (closure_in, closure_out);
        let (handle_keys, channel_filter) = match channel_kind {
            ChannelKind::Inbound => {
                closure_in = |name| interface.inbound_channel(name).is_none();
                (channels.inbound.keys(), &closure_in as &dyn Fn(_) -> _)
            }
            ChannelKind::Outbound => {
                closure_out = |name| interface.outbound_channel(name).is_none();
                (channels.outbound.keys(), &closure_out as &dyn Fn(_) -> _)
            }
        };

        let mut extra_handles = handle_keys
            .filter(|name| channel_filter(name.as_str()))
            .fold(String::new(), |mut acc, name| {
                write!(acc, "`{}`, ", name).unwrap();
                acc
            });
        debug_assert!(!extra_handles.is_empty());
        extra_handles.truncate(extra_handles.len() - 2);
        Trap::new(format!("extra {} handles: {}", channel_kind, extra_handles))
    }

    #[tracing::instrument(skip(args), ret, err, fields(args.len = args.len()))]
    fn stash_workflow(
        &mut self,
        definition_id: &str,
        args: Vec<u8>,
        channels: &ChannelsConfig<ChannelId>,
    ) -> Result<WorkflowId, Trap> {
        let workflows = self
            .services
            .workflows
            .as_deref_mut()
            .ok_or_else(|| Trap::new("no capability to spawn workflows"))?;
        let stub_id = self.persisted.child_workflow_stubs.create();
        workflows.stash_workflow(stub_id, definition_id, args, channels.clone());

        self.current_execution().push_resource_event(
            ResourceId::WorkflowStub(stub_id),
            ResourceEventKind::Created,
        );
        Ok(stub_id)
    }

    fn poll_workflow_init(
        &mut self,
        stub_id: WorkflowId,
        cx: &mut WasmContext,
    ) -> Result<PollStub, Trap> {
        let stubs = &mut self.persisted.child_workflow_stubs.stubs;
        let stub = stubs
            .get(&stub_id)
            .ok_or_else(|| Trap::new(format!("stub {stub_id} polled after completion")))?;
        let poll_result = stub.result.clone();
        if poll_result.is_ready() {
            // We don't want to create aliased workflow resources; thus, we prevent repeated
            // stub polling after completion.
            stubs.remove(&stub_id);
        }

        self.current_execution().push_resource_event(
            ResourceId::WorkflowStub(stub_id),
            ResourceEventKind::Polled(utils::drop_value(&poll_result)),
        );
        if let Poll::Ready(Ok(workflow_id)) = &poll_result {
            self.current_execution().push_resource_event(
                ResourceId::Workflow(*workflow_id),
                ResourceEventKind::Created,
            );
        }
        Ok(poll_result.wake_if_pending(cx, || WakerPlacement::WorkflowInit(stub_id)))
    }

    fn poll_workflow_completion(
        &mut self,
        workflow_id: WorkflowId,
        cx: &mut WasmContext,
    ) -> PollTask {
        let poll_result = self.persisted.child_workflows[&workflow_id]
            .result()
            .map(utils::extract_task_poll_result);
        self.current_execution().push_resource_event(
            ResourceId::Workflow(workflow_id),
            ResourceEventKind::Polled(utils::drop_value(&poll_result)),
        );
        poll_result.wake_if_pending(cx, || WakerPlacement::WorkflowCompletion(workflow_id))
    }

    fn workflow_task_error(&self, workflow_id: WorkflowId) -> Option<&TaskError> {
        let result = self.persisted.child_workflows[&workflow_id].result();
        if let Poll::Ready(Err(JoinError::Err(err))) = result {
            Some(err)
        } else {
            None
        }
    }

    pub(super) fn handle_child_stub_drop(&mut self, stub_id: WorkflowId) -> HashSet<WakerId> {
        self.current_execution().push_resource_event(
            ResourceId::WorkflowStub(stub_id),
            ResourceEventKind::Dropped,
        );
        let stubs = &mut self.persisted.child_workflow_stubs.stubs;
        stubs
            .get_mut(&stub_id)
            .map_or_else(HashSet::default, |stub| mem::take(&mut stub.wakes_on_init))
    }

    /// Handles dropping the child workflow handle from the workflow side. Returns wakers
    /// that should be dropped.
    pub(super) fn handle_child_handle_drop(&mut self, workflow_id: WorkflowId) -> HashSet<WakerId> {
        self.current_execution().push_resource_event(
            ResourceId::Workflow(workflow_id),
            ResourceEventKind::Dropped,
        );
        let state = self
            .persisted
            .child_workflows
            .get_mut(&workflow_id)
            .unwrap();
        mem::take(&mut state.wakes_on_completion)
    }
}

#[derive(Debug)]
pub(crate) struct SpawnFunctions;

#[allow(clippy::needless_pass_by_value)]
impl SpawnFunctions {
    fn write_spawn_result(
        ctx: &mut StoreContextMut<'_, WorkflowData>,
        result: Result<(), HostError>,
        error_ptr: u32,
    ) -> Result<(), Trap> {
        let memory = ctx.data().exports().memory;
        let result_abi = result.into_wasm(&mut WasmAllocator::new(ctx.as_context_mut()))?;
        memory
            .write(ctx, error_ptr as usize, &result_abi.to_le_bytes())
            .map_err(|err| Trap::new(format!("cannot write to WASM memory: {}", err)))
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(id, interface.is_some))]
    pub fn workflow_interface(
        ctx: StoreContextMut<'_, WorkflowData>,
        id_ptr: u32,
        id_len: u32,
    ) -> Result<i64, Trap> {
        let memory = ctx.data().exports().memory;
        let id = utils::copy_string_from_wasm(&ctx, &memory, id_ptr, id_len)?;
        tracing::Span::current().record("id", &id);

        let workflows = ctx.data().services.workflows.as_deref();
        let interface = workflows
            .and_then(|workflows| workflows.interface(&id))
            .map(|interface| interface.to_bytes());
        tracing::Span::current().record("interface.is_some", interface.is_some());

        interface.into_wasm(&mut WasmAllocator::new(ctx))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    #[allow(clippy::unnecessary_wraps)] // required by wasmtime
    pub fn create_channel_handles() -> Option<ExternRef> {
        let resource = HostResource::from(SharedChannelHandles {
            inner: Arc::new(Mutex::new(ChannelsConfig::default())),
        });
        Some(resource.into_ref())
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(channel_kind, name, config))]
    pub fn set_channel_handle(
        ctx: StoreContextMut<'_, WorkflowData>,
        handles: Option<ExternRef>,
        channel_kind: i32,
        name_ptr: u32,
        name_len: u32,
        is_closed: i32,
    ) -> Result<(), Trap> {
        let channel_kind = ChannelKind::try_from_wasm(channel_kind).map_err(Trap::new)?;
        let channel_config = match is_closed {
            0 => ChannelSpawnConfig::New,
            1 => ChannelSpawnConfig::Closed,
            _ => return Err(Trap::new("invalid `is_closed` value; expected 0 or 1")),
        };
        let memory = ctx.data().exports().memory;
        let name = utils::copy_string_from_wasm(&ctx, &memory, name_ptr, name_len)?;

        tracing::Span::current()
            .record("channel_kind", field::debug(channel_kind))
            .record("name", &name)
            .record("config", field::debug(&channel_config));

        let handles = HostResource::from_ref(handles.as_ref())?.as_channel_handles()?;
        let mut handles = handles.inner.lock().unwrap();
        match channel_kind {
            ChannelKind::Inbound => {
                handles.inbound.insert(name, channel_config);
            }
            ChannelKind::Outbound => {
                handles.outbound.insert(name, channel_config);
            }
        }
        tracing::debug!(?handles, "inserted channel handle");
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(name, sender))]
    pub fn copy_sender_handle(
        ctx: StoreContextMut<'_, WorkflowData>,
        handles: Option<ExternRef>,
        name_ptr: u32,
        name_len: u32,
        sender: Option<ExternRef>,
    ) -> Result<(), Trap> {
        let channel_ref = HostResource::from_ref(sender.as_ref())?.as_outbound_channel()?;
        let channel_state = ctx.data().persisted.outbound_channel(channel_ref);
        let channel_id = channel_state.unwrap().id();
        let memory = ctx.data().exports().memory;
        let name = utils::copy_string_from_wasm(&ctx, &memory, name_ptr, name_len)?;

        tracing::Span::current()
            .record("name", &name)
            .record("sender", field::debug(channel_ref));

        let handles = HostResource::from_ref(handles.as_ref())?.as_channel_handles()?;
        let mut handles = handles.inner.lock().unwrap();
        handles
            .outbound
            .insert(name, ChannelSpawnConfig::Existing(channel_id));
        tracing::debug!(?handles, "inserted channel handle");
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(id, args.len = args_len, handles))]
    pub fn spawn(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        id_ptr: u32,
        id_len: u32,
        args_ptr: u32,
        args_len: u32,
        handles: Option<ExternRef>,
    ) -> Result<Option<ExternRef>, Trap> {
        let memory = ctx.data().exports().memory;
        let id = utils::copy_string_from_wasm(&ctx, &memory, id_ptr, id_len)?;
        let args = utils::copy_bytes_from_wasm(&ctx, &memory, args_ptr, args_len)?;
        let handles = HostResource::from_ref(handles.as_ref())?.as_channel_handles()?;
        let handles = handles.inner.lock().unwrap();

        tracing::Span::current()
            .record("id", &id)
            .record("handles", field::debug(&handles));

        ctx.data().validate_handles(&id, &handles)?;
        let stub_id = ctx.data_mut().stash_workflow(&id, args, &handles)?;

        Ok(Some(HostResource::WorkflowStub(stub_id).into_ref()))
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(stub_id))]
    pub fn poll_workflow_init(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        stub: Option<ExternRef>,
        poll_cx: WasmContextPtr,
        error_ptr: u32,
    ) -> Result<Option<ExternRef>, Trap> {
        let stub_id = HostResource::from_ref(stub.as_ref())?.as_workflow_stub()?;
        tracing::Span::current().record("stub_id", stub_id);

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx.data_mut().poll_workflow_init(stub_id, &mut poll_cx)?;
        tracing::debug!(result = ?poll_result);
        poll_cx.save_waker(&mut ctx)?;

        let mut workflow_id = None;
        let result = poll_result.map_ok(|id| {
            workflow_id = Some(id);
        });
        if let Poll::Ready(result) = result {
            Self::write_spawn_result(&mut ctx, result, error_ptr)?;
        }
        Ok(workflow_id.map(|id| HostResource::Workflow(id).into_ref()))
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(workflow_id))]
    pub fn poll_workflow_completion(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        workflow: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> Result<i64, Trap> {
        let workflow_id = HostResource::from_ref(workflow.as_ref())?.as_workflow()?;
        tracing::Span::current().record("workflow_id", workflow_id);

        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx
            .data_mut()
            .poll_workflow_completion(workflow_id, &mut poll_cx);
        tracing::debug!(result = ?poll_result);

        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(workflow_id, err))]
    pub fn completion_error(
        ctx: StoreContextMut<'_, WorkflowData>,
        workflow: Option<ExternRef>,
    ) -> Result<i64, Trap> {
        let workflow_id = HostResource::from_ref(workflow.as_ref())?.as_workflow()?;
        let maybe_err = ctx
            .data()
            .workflow_task_error(workflow_id)
            .map(TaskError::clone_boxed);

        tracing::Span::current()
            .record("workflow_id", workflow_id)
            .record("err", maybe_err.as_ref().map(field::debug));

        Ok(maybe_err
            .map(|err| err.into_wasm(&mut WasmAllocator::new(ctx)))
            .transpose()?
            .unwrap_or(0))
    }
}
