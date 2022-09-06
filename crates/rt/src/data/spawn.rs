//! Spawning workflows.

use wasmtime::{AsContextMut, ExternRef, StoreContextMut, Trap};

use std::{
    collections::HashSet,
    mem,
    sync::{Arc, Mutex},
    task::Poll,
};

use crate::{
    data::{
        channel::ChannelStates,
        helpers::{HostResource, WakeIfPending, WakerPlacement, WasmContext, WasmContextPtr},
        WorkflowData,
    },
    receipt::{ResourceEventKind, ResourceId},
    utils::{copy_bytes_from_wasm, copy_string_from_wasm, drop_value, WasmAllocator},
    workflow::ChannelIds,
};
use tardigrade::spawn::{ChannelHandles, ChannelSpawnConfig};
use tardigrade_shared::{
    abi::{IntoWasm, TryFromWasm},
    interface::{ChannelKind, Interface},
    JoinError, SpawnError, WakerId, WorkflowId,
};

#[derive(Debug, Clone)]
pub(super) struct SharedChannelHandles {
    inner: Arc<Mutex<ChannelHandles>>,
}

#[derive(Debug)]
pub struct ChildWorkflowState {
    pub(super) channels: ChannelStates,
    pub(super) completion_result: Poll<Result<(), JoinError>>,
    pub(super) wakes_on_completion: HashSet<WakerId>,
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

    pub(super) fn insert_waker(&mut self, waker_id: WakerId) {
        self.wakes_on_completion.insert(waker_id);
    }
}

impl WorkflowData {
    pub fn child_workflows(&self) -> impl Iterator<Item = (WorkflowId, &ChildWorkflowState)> + '_ {
        self.child_workflows.iter().map(|(id, state)| (*id, state))
    }

    pub fn child_workflow(&self, id: WorkflowId) -> Option<&ChildWorkflowState> {
        self.child_workflows.get(&id)
    }

    fn validate_handles(&self, definition_id: &str, handles: &ChannelHandles) -> Result<(), Trap> {
        if let Some(interface) = self.services.workflows.interface(definition_id) {
            for (name, _) in interface.inbound_channels() {
                if !handles.inbound.contains_key(name) {
                    let message = format!("missing handle for inbound channel `{}`", name);
                    return Err(Trap::new(message));
                }
            }
            for (name, _) in interface.outbound_channels() {
                if !handles.outbound.contains_key(name) {
                    let message = format!("missing handle for outbound channel `{}`", name);
                    return Err(Trap::new(message));
                }
            }

            if handles.inbound.len() != interface.inbound_channels().len() {
                let err = Self::extra_handles_error(&interface, handles, ChannelKind::Inbound);
                return Err(err);
            }
            if handles.outbound.len() != interface.outbound_channels().len() {
                let err = Self::extra_handles_error(&interface, handles, ChannelKind::Outbound);
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
        interface: &Interface<()>,
        handles: &ChannelHandles,
        channel_kind: ChannelKind,
    ) -> Trap {
        use std::fmt::Write as _;

        let (closure_in, closure_out);
        let (handle_keys, channel_filter) = match channel_kind {
            ChannelKind::Inbound => {
                closure_in = |name| interface.inbound_channel(name).is_none();
                (handles.inbound.keys(), &closure_in as &dyn Fn(_) -> _)
            }
            ChannelKind::Outbound => {
                closure_out = |name| interface.outbound_channel(name).is_none();
                (handles.outbound.keys(), &closure_out as &dyn Fn(_) -> _)
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

    fn spawn_workflow(
        &mut self,
        definition_id: &str,
        args: Vec<u8>,
        handles: &ChannelHandles,
    ) -> Result<WorkflowId, SpawnError> {
        let result = self
            .services
            .workflows
            .create_workflow(definition_id, args, handles);
        let result = result.map(|mut ids| {
            mem::swap(&mut ids.channel_ids.inbound, &mut ids.channel_ids.outbound);
            self.child_workflows
                .insert(ids.workflow_id, ChildWorkflowState::new(&ids.channel_ids));
            self.current_execution().push_resource_event(
                ResourceId::Workflow(ids.workflow_id),
                ResourceEventKind::Created,
            );
            ids.workflow_id
        });
        crate::log_result!(result, "Spawned workflow using `{}`", definition_id)
    }

    fn poll_workflow_completion(
        &mut self,
        workflow_id: WorkflowId,
        cx: &mut WasmContext,
    ) -> Poll<Result<(), JoinError>> {
        let poll_result = self.child_workflows[&workflow_id].completion_result.clone();
        self.current_execution().push_resource_event(
            ResourceId::Workflow(workflow_id),
            ResourceEventKind::Polled(drop_value(&poll_result)),
        );
        poll_result.wake_if_pending(cx, || WakerPlacement::WorkflowCompletion(workflow_id))
    }
}

#[derive(Debug)]
pub(crate) struct SpawnFunctions;

#[allow(clippy::needless_pass_by_value)]
impl SpawnFunctions {
    fn write_spawn_result(
        ctx: &mut StoreContextMut<'_, WorkflowData>,
        result: Result<(), SpawnError>,
        error_ptr: u32,
    ) -> Result<(), Trap> {
        let memory = ctx.data().exports().memory;
        let result_abi = result.into_wasm(&mut WasmAllocator::new(ctx.as_context_mut()))?;
        memory
            .write(ctx, error_ptr as usize, &result_abi.to_le_bytes())
            .map_err(|err| Trap::new(format!("cannot write to WASM memory: {}", err)))
    }

    pub fn workflow_interface(
        ctx: StoreContextMut<'_, WorkflowData>,
        id_ptr: u32,
        id_len: u32,
    ) -> Result<i64, Trap> {
        let memory = ctx.data().exports().memory;
        let id = copy_string_from_wasm(&ctx, &memory, id_ptr, id_len)?;

        let workflows = &ctx.data().services.workflows;
        let interface = workflows
            .interface(&id)
            .map(|interface| interface.to_bytes());
        interface.into_wasm(&mut WasmAllocator::new(ctx))
    }

    #[allow(clippy::unnecessary_wraps)] // required by wasmtime
    pub fn create_channel_handles() -> Option<ExternRef> {
        let resource = HostResource::from(SharedChannelHandles {
            inner: Arc::new(Mutex::new(ChannelHandles::default())),
        });
        Some(resource.into_ref())
    }

    pub fn set_channel_handle(
        ctx: StoreContextMut<'_, WorkflowData>,
        handles: Option<ExternRef>,
        channel_kind: i32,
        name_ptr: u32,
        name_len: u32,
        spawn_config: i32,
    ) -> Result<(), Trap> {
        let channel_kind = ChannelKind::try_from_wasm(channel_kind).map_err(Trap::new)?;
        let channel_config = ChannelSpawnConfig::try_from_wasm(spawn_config).map_err(Trap::new)?;
        let memory = ctx.data().exports().memory;
        let name = copy_string_from_wasm(&ctx, &memory, name_ptr, name_len)?;

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
        Ok(())
    }

    pub fn spawn(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        id_ptr: u32,
        id_len: u32,
        args_ptr: u32,
        args_len: u32,
        handles: Option<ExternRef>,
        error_ptr: u32,
    ) -> Result<Option<ExternRef>, Trap> {
        let memory = ctx.data().exports().memory;
        let id = copy_string_from_wasm(&ctx, &memory, id_ptr, id_len)?;
        let args = copy_bytes_from_wasm(&ctx, &memory, args_ptr, args_len)?;
        let handles = HostResource::from_ref(handles.as_ref())?.as_channel_handles()?;
        let handles = handles.inner.lock().unwrap();
        ctx.data().validate_handles(&id, &handles)?;

        let mut workflow_id = None;
        let result = ctx
            .data_mut()
            .spawn_workflow(&id, args, &handles)
            .map(|id| {
                workflow_id = Some(id);
            });
        Self::write_spawn_result(&mut ctx, result, error_ptr)?;
        Ok(workflow_id.map(|id| HostResource::Workflow(id).into_ref()))
    }

    pub fn poll_workflow_completion(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        workflow: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> Result<i64, Trap> {
        let workflow_id = HostResource::from_ref(workflow.as_ref())?.as_workflow()?;
        let mut poll_cx = WasmContext::new(poll_cx);
        let poll_result = ctx
            .data_mut()
            .poll_workflow_completion(workflow_id, &mut poll_cx);
        crate::trace!(
            "Polled completion for workflow {} with context {:?}: {:?}",
            workflow_id,
            poll_cx,
            poll_result
        );
        poll_cx.save_waker(&mut ctx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }
}
