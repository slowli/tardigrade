//! Spawning workflows.

use serde::{Deserialize, Serialize};
use wasmtime::{AsContextMut, ExternRef, StoreContextMut, Trap};

use std::{collections::HashMap, mem};

use crate::{
    data::{
        channel::ChannelStates,
        helpers::{HostResource, Message},
        WorkflowData,
    },
    services::{ChannelHandles, ManageChannels},
    utils::{copy_bytes_from_wasm, copy_string_from_wasm, WasmAllocator},
};
use tardigrade_shared::{abi::IntoWasm, interface::Interface, ChannelId, SpawnError, WorkflowId};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ChannelSpawnConfig {
    New,
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SpawnConfig {
    id: String,
    args: Message,
    inbound_channels: HashMap<String, ChannelSpawnConfig>,
    outbound_channels: HashMap<String, ChannelSpawnConfig>,
}

impl SpawnConfig {
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
pub(super) struct ChildWorkflowState {
    pub channels: ChannelStates,
}

impl ChildWorkflowState {
    fn new(handles: &ChannelHandles<'_>) -> Self {
        // FIXME: what is the appropriate capacity?
        Self {
            channels: ChannelStates::new(handles, |_| Some(1)),
        }
    }
}

impl WorkflowData {
    fn spawn_workflow(&mut self, config: &SpawnConfig) -> Result<WorkflowId, SpawnError> {
        let mut handles = config.create_handles(&*self.services.channels);
        let workflows = &self.services.workflows;
        let result = workflows.create_workflow(&config.id, config.args.as_ref(), &handles);
        if let Ok(workflow_id) = &result {
            mem::swap(&mut handles.inbound, &mut handles.outbound);
            self.child_workflows
                .insert(*workflow_id, ChildWorkflowState::new(&handles));
        }
        crate::log_result!(result, "Spawned workflow using {:?}", config)
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

    pub fn get_spawner(
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
        let resource = HostResource::from(SpawnConfig::new(&interface, id, args));
        Ok(Some(resource.into_ref()))
    }

    pub fn spawn(
        mut ctx: StoreContextMut<'_, WorkflowData>,
        spawner: Option<ExternRef>,
        error_ptr: u32,
    ) -> Result<Option<ExternRef>, Trap> {
        let config = HostResource::from_ref(spawner.as_ref())?.as_spawn_config()?;
        let mut workflow_id = None;
        let result = ctx.data_mut().spawn_workflow(config).map(|id| {
            workflow_id = Some(id);
        });
        Self::write_spawn_result(&mut ctx, result, error_ptr)?;
        Ok(workflow_id.map(|id| HostResource::Workflow(id).into_ref()))
    }
}
