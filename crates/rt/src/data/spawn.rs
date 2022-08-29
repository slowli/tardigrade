//! Spawning workflows.

use serde::{Deserialize, Serialize};
use wasmtime::{AsContextMut, ExternRef, StoreContextMut, Trap};

use std::{
    collections::{HashMap, HashSet},
    mem,
    sync::Mutex,
    task::Poll,
};

use crate::{
    data::{
        channel::{ChannelStates, InboundChannelState, OutboundChannelState},
        helpers::{
            HostResource, Message, WakeIfPending, WakerPlacement, WasmContext, WasmContextPtr,
        },
        WorkflowData,
    },
    receipt::{ResourceEventKind, ResourceId},
    services::{ChannelHandles, ManageChannels},
    utils::{copy_bytes_from_wasm, copy_string_from_wasm, drop_value, WasmAllocator},
};
use tardigrade_shared::{
    abi::{IntoWasm, TryFromWasm},
    interface::{ChannelKind, Interface},
    ChannelId, JoinError, SpawnError, WakerId, WorkflowId,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ChannelSpawnConfig {
    New,
    Closed,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct SpawnConfig {
    inner: Mutex<SpawnConfigInner>,
}

impl Clone for SpawnConfig {
    fn clone(&self) -> Self {
        Self {
            inner: Mutex::new(self.inner.lock().unwrap().clone()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpawnConfigInner {
    id: String,
    args: Message,
    inbound_channels: HashMap<String, ChannelSpawnConfig>,
    outbound_channels: HashMap<String, ChannelSpawnConfig>,
}

impl SpawnConfigInner {
    fn new(interface: &Interface<()>, id: String, args: Vec<u8>) -> Self {
        let inbound_channels = interface
            .inbound_channels()
            .map(|(name, _)| (name.to_owned(), ChannelSpawnConfig::New))
            .collect();
        let outbound_channels = interface
            .outbound_channels()
            .map(|(name, _)| (name.to_owned(), ChannelSpawnConfig::New))
            .collect();
        Self {
            id,
            args: args.into(),
            inbound_channels,
            outbound_channels,
        }
    }

    fn create_handles(&self, manager: &dyn ManageChannels) -> ChannelHandles<'_> {
        let inbound_channels = Self::map_channels(&self.inbound_channels, manager);
        let outbound_channels = Self::map_channels(&self.outbound_channels, manager);
        ChannelHandles {
            inbound: inbound_channels,
            outbound: outbound_channels,
        }
    }

    fn map_channels<'a>(
        channels: &'a HashMap<String, ChannelSpawnConfig>,
        manager: &dyn ManageChannels,
    ) -> HashMap<&'a str, ChannelId> {
        channels
            .iter()
            .map(|(name, config)| {
                let channel_id = match config {
                    ChannelSpawnConfig::Closed => <dyn ManageChannels>::CLOSED_CHANNEL,
                    ChannelSpawnConfig::New => manager.create_channel(),
                };
                (name.as_str(), channel_id)
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct ChildWorkflowState {
    pub(super) channels: ChannelStates,
    pub(super) completion_result: Poll<Result<(), JoinError>>,
    pub(super) wakes_on_completion: HashSet<WakerId>,
}

impl ChildWorkflowState {
    fn new(handles: &ChannelHandles<'_>) -> Self {
        Self {
            // FIXME: what is the appropriate capacity?
            channels: ChannelStates::new(handles, |_| Some(1)),
            completion_result: Poll::Pending,
            wakes_on_completion: HashSet::new(),
        }
    }

    pub(super) fn insert_waker(&mut self, waker_id: WakerId) {
        self.wakes_on_completion.insert(waker_id);
    }

    /// Returns the current state of an inbound channel with the specified name.
    pub fn inbound_channel(&self, channel_name: &str) -> Option<&InboundChannelState> {
        self.channels.inbound.get(channel_name)
    }

    /// Returns the current state of an outbound channel with the specified name.
    pub fn outbound_channel(&self, channel_name: &str) -> Option<&OutboundChannelState> {
        self.channels.outbound.get(channel_name)
    }
}

impl WorkflowData {
    pub fn child_workflows(&self) -> impl Iterator<Item = (WorkflowId, &ChildWorkflowState)> + '_ {
        self.child_workflows.iter().map(|(id, state)| (*id, state))
    }

    fn spawn_workflow(&mut self, config: &SpawnConfigInner) -> Result<WorkflowId, SpawnError> {
        let mut handles = config.create_handles(&*self.services.channels);
        let workflows = &self.services.workflows;
        let result = workflows.create_workflow(&config.id, config.args.clone().into(), &handles);
        if let &Ok(workflow_id) = &result {
            mem::swap(&mut handles.inbound, &mut handles.outbound);
            self.child_workflows
                .insert(workflow_id, ChildWorkflowState::new(&handles));
            self.current_execution().push_resource_event(
                ResourceId::Workflow(workflow_id),
                ResourceEventKind::Created,
            );
        }
        crate::log_result!(result, "Spawned workflow using {:?}", config)
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
        let interface = workflows.interface(&id).map(Interface::to_bytes);
        interface.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn create_spawner(
        ctx: StoreContextMut<'_, WorkflowData>,
        id_ptr: u32,
        id_len: u32,
        args_ptr: u32,
        args_len: u32,
    ) -> Result<Option<ExternRef>, Trap> {
        let memory = ctx.data().exports().memory;
        let id = copy_string_from_wasm(&ctx, &memory, id_ptr, id_len)?;
        let args = copy_bytes_from_wasm(&ctx, &memory, args_ptr, args_len)?;

        let workflows = &ctx.data().services.workflows;
        let interface = if let Some(interface) = workflows.interface(&id) {
            interface
        } else {
            let message = format!("workflow with ID `{}` is not found", id);
            return Err(Trap::new(message));
        };
        let resource = HostResource::from(SpawnConfig {
            inner: Mutex::new(SpawnConfigInner::new(interface, id, args)),
        });
        Ok(Some(resource.into_ref()))
    }

    pub fn set_spawner_handle(
        ctx: StoreContextMut<'_, WorkflowData>,
        spawner: Option<ExternRef>,
        channel_kind: i32,
        name_ptr: u32,
        name_len: u32,
    ) -> Result<(), Trap> {
        let channel_kind = ChannelKind::try_from_wasm(channel_kind).map_err(Trap::new)?;
        let memory = ctx.data().exports().memory;
        let name = copy_string_from_wasm(&ctx, &memory, name_ptr, name_len)?;
        let config = HostResource::from_ref(spawner.as_ref())?.as_spawn_config()?;
        let mut config = config.inner.lock().unwrap();

        let channels = match channel_kind {
            ChannelKind::Inbound => &mut config.inbound_channels,
            ChannelKind::Outbound => &mut config.outbound_channels,
        };
        let channel = channels
            .get_mut(&name)
            .ok_or_else(|| Trap::new(format!("{} channel `{}` not found", channel_kind, name)))?;
        *channel = ChannelSpawnConfig::Closed;

        Ok(())
    }

    pub fn spawn(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        spawner: Option<ExternRef>,
        error_ptr: u32,
    ) -> Result<Option<ExternRef>, Trap> {
        let config = HostResource::from_ref(spawner.as_ref())?.as_spawn_config()?;
        let config = config.inner.lock().unwrap();
        let mut workflow_id = None;
        let result = ctx.data_mut().spawn_workflow(&config).map(|id| {
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
