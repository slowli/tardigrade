//! Functions used in WASM imports.

use anyhow::{anyhow, ensure, Context};
use chrono::{TimeZone, Utc};
use tracing::field;
use tracing_tunnel::TracingEvent;
use wasmtime::{AsContextMut, ExternRef, StoreContextMut};

use std::{collections::HashSet, task::Poll};

use super::{
    copy_bytes_from_wasm, copy_string_from_wasm,
    instance::{HostResource, InstanceData, SharedChannelHandles},
    WasmAllocator,
};
use crate::{
    data::{ReportedErrorKind, WorkflowData},
    engine::{AsWorkflowData, CreateWaker},
    utils,
};
use tardigrade::{
    abi::{IntoWasm, ResourceKind},
    interface::{Handle, HandlePath, HandlePathBuf},
    task::{JoinError, TaskError},
    ChannelId, TaskId, TimerDefinition, TimerId, WakerId, WorkflowId,
};

pub(super) type WasmContextPtr = u32;

struct WasmContext<'a> {
    context: StoreContextMut<'a, InstanceData>,
    ptr: WasmContextPtr,
}

impl<'a> WasmContext<'a> {
    fn new(context: StoreContextMut<'a, InstanceData>, ptr: WasmContextPtr) -> Self {
        Self { context, ptr }
    }
}

impl AsWorkflowData for WasmContext<'_> {
    fn data(&self) -> &WorkflowData {
        &self.context.data().inner
    }

    fn data_mut(&mut self) -> &mut WorkflowData {
        &mut self.context.data_mut().inner
    }
}

impl CreateWaker for WasmContext<'_> {
    fn create_waker(&mut self) -> anyhow::Result<WakerId> {
        let exports = self.context.data().exports();
        exports.create_waker(self.context.as_context_mut(), self.ptr)
    }
}

/// Functions operating on `WorkflowData` exported to WASM.
pub(super) struct WorkflowFunctions;

/// General-purpose functions.
impl WorkflowFunctions {
    #[tracing::instrument(level = "debug", skip_all, err, fields(resource))]
    pub fn resource_id(resource: Option<ExternRef>) -> anyhow::Result<u64> {
        let resource = HostResource::from_ref(resource.as_ref())?;
        tracing::Span::current().record("resource", field::debug(resource));

        match resource {
            HostResource::Receiver(id) | HostResource::Sender(id) | HostResource::Workflow(id) => {
                Ok(*id)
            }
            HostResource::ChannelHandles(_) => {
                Err(anyhow!("getting ID for resource not supported"))
            }
        }
    }

    #[allow(clippy::needless_pass_by_value)] // required by wasmtime
    pub fn drop_resource(
        mut ctx: StoreContextMut<'_, InstanceData>,
        dropped: Option<ExternRef>,
    ) -> anyhow::Result<()> {
        let dropped = HostResource::from_ref(dropped.as_ref())?;
        let data = &mut ctx.data_mut().inner;
        let waker_ids = match dropped {
            HostResource::Receiver(channel_id) => data.receiver(*channel_id).drop(),
            HostResource::Sender(channel_id) => data.sender(*channel_id).drop(),
            HostResource::Workflow(workflow_id) => data.child(*workflow_id).drop(),
            HostResource::ChannelHandles(_) => HashSet::new(),
        };

        let exports = ctx.data().exports();
        for waker_id in waker_ids {
            exports.drop_waker(ctx.as_context_mut(), waker_id).ok();
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    #[allow(clippy::unnecessary_wraps)] // required by wasmtime
    pub fn create_handles() -> Option<ExternRef> {
        let resource = HostResource::ChannelHandles(SharedChannelHandles::default());
        Some(resource.into_ref())
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(path, channel))]
    pub fn insert_sender_into_handles(
        ctx: StoreContextMut<'_, InstanceData>,
        handles: Option<ExternRef>,
        path_ptr: u32,
        path_len: u32,
        sender: Option<ExternRef>,
    ) -> anyhow::Result<()> {
        let channel_id = sender
            .as_ref()
            .map(|half| HostResource::from_ref(Some(half)).and_then(HostResource::as_sender));
        let channel_id = channel_id.transpose()?.unwrap_or(0);
        let channel_half = Handle::Sender(channel_id);

        Self::insert_into_handles(&ctx, handles.as_ref(), path_ptr, path_len, channel_half)
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(path, channel))]
    pub fn insert_receiver_into_handles(
        ctx: StoreContextMut<'_, InstanceData>,
        handles: Option<ExternRef>,
        path_ptr: u32,
        path_len: u32,
        receiver: Option<ExternRef>,
    ) -> anyhow::Result<()> {
        let channel_id = receiver
            .as_ref()
            .map(|half| HostResource::from_ref(Some(half)).and_then(HostResource::as_receiver));
        let channel_id = channel_id.transpose()?.unwrap_or(0);
        let channel_half = Handle::Receiver(channel_id);

        Self::insert_into_handles(&ctx, handles.as_ref(), path_ptr, path_len, channel_half)
    }

    fn insert_into_handles(
        ctx: &StoreContextMut<'_, InstanceData>,
        handles: Option<&ExternRef>,
        path_ptr: u32,
        path_len: u32,
        channel_half: Handle<ChannelId>,
    ) -> anyhow::Result<()> {
        let memory = ctx.data().exports().memory;
        let path = copy_string_from_wasm(ctx, &memory, path_ptr, path_len)?;
        let path = HandlePathBuf::from(path.as_str());

        tracing::Span::current()
            .record("path", field::debug(&path))
            .record("channel", field::debug(&channel_half));

        let handles = HostResource::from_ref(handles)?.as_channel_handles()?;
        let mut handles = handles.inner.lock().unwrap();
        handles.insert(path, channel_half);
        tracing::debug!(?handles, "inserted channel handle");
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(path))]
    pub fn remove_from_handles(
        ctx: StoreContextMut<'_, InstanceData>,
        handles: Option<ExternRef>,
        path_ptr: u32,
        path_len: u32,
        kind_ptr: u32,
    ) -> anyhow::Result<Option<ExternRef>> {
        let memory = ctx.data().exports().memory;
        let path = copy_string_from_wasm(&ctx, &memory, path_ptr, path_len)?;
        let path = HandlePath::new(&path);

        tracing::Span::current().record("path", field::debug(path));

        let handles = HostResource::from_ref(handles.as_ref())?.as_channel_handles()?;
        let mut handles = handles.inner.lock().unwrap();
        let mut kind = ResourceKind::None;
        let removed = handles.remove(&path).and_then(|handle| match handle {
            Handle::Receiver(id) => {
                kind = ResourceKind::Receiver;
                (id != 0).then(|| HostResource::Receiver(id).into_ref())
            }
            Handle::Sender(id) => {
                kind = ResourceKind::Sender;
                (id != 0).then(|| HostResource::Sender(id).into_ref())
            }
        });

        let kind_bytes = (kind as i32).to_le_bytes();
        memory.write(ctx, kind_ptr as usize, &kind_bytes)?;
        Ok(removed)
    }

    pub fn report_panic(
        ctx: StoreContextMut<'_, InstanceData>,
        message_ptr: u32,
        message_len: u32,
        filename_ptr: u32,
        filename_len: u32,
        line: u32,
        column: u32,
    ) -> anyhow::Result<()> {
        Self::report_error_or_panic(
            ctx,
            ReportedErrorKind::Panic,
            message_ptr,
            message_len,
            filename_ptr,
            filename_len,
            line,
            column,
        )
    }

    #[allow(clippy::too_many_arguments)] // acceptable for internal fn
    fn report_error_or_panic(
        mut ctx: StoreContextMut<'_, InstanceData>,
        error_kind: ReportedErrorKind,
        message_ptr: u32,
        message_len: u32,
        filename_ptr: u32,
        filename_len: u32,
        line: u32,
        column: u32,
    ) -> anyhow::Result<()> {
        let memory = ctx.data().exports().memory;
        let message = if message_ptr == 0 {
            None
        } else {
            Some(copy_string_from_wasm(
                &ctx,
                &memory,
                message_ptr,
                message_len,
            )?)
        };
        let filename = if filename_ptr == 0 {
            None
        } else {
            Some(copy_string_from_wasm(
                &ctx,
                &memory,
                filename_ptr,
                filename_len,
            )?)
        };

        ctx.data_mut()
            .inner
            .report_error_or_panic(error_kind, message, filename, line, column);
        Ok(())
    }
}

/// Channel-related functions.
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
// ^ Required for WASM function wrappers
impl WorkflowFunctions {
    pub fn new_channel(
        mut ctx: StoreContextMut<'_, InstanceData>,
        stub_id: ChannelId,
    ) -> anyhow::Result<()> {
        ctx.data_mut().inner.create_channel_stub(stub_id)
    }

    pub fn poll_next_for_receiver(
        mut ctx: StoreContextMut<'_, InstanceData>,
        channel_ref: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> anyhow::Result<i64> {
        let channel_id = HostResource::from_ref(channel_ref.as_ref())?.as_receiver()?;
        let mut receiver = ctx.data_mut().inner.receiver(channel_id);

        let poll_result = receiver.poll_next();
        let mut poll_cx = WasmContext::new(ctx.as_context_mut(), poll_cx);
        let poll_result = poll_result.into_inner(&mut poll_cx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn poll_ready_for_sender(
        mut ctx: StoreContextMut<'_, InstanceData>,
        channel_ref: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> anyhow::Result<i32> {
        let channel_id = HostResource::from_ref(channel_ref.as_ref())?.as_sender()?;
        let mut sender = ctx.data_mut().inner.sender(channel_id);

        let poll_result = sender.poll_ready();
        let mut poll_cx = WasmContext::new(ctx.as_context_mut(), poll_cx);
        let poll_result = poll_result.into_inner(&mut poll_cx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn start_send(
        mut ctx: StoreContextMut<'_, InstanceData>,
        channel_ref: Option<ExternRef>,
        message_ptr: u32,
        message_len: u32,
    ) -> anyhow::Result<i32> {
        let channel_id = HostResource::from_ref(channel_ref.as_ref())?.as_sender()?;

        let memory = ctx.data().exports().memory;
        let message = copy_bytes_from_wasm(&ctx, &memory, message_ptr, message_len)?;
        let mut sender = ctx.data_mut().inner.sender(channel_id);
        let result = sender.start_send(message);
        result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn poll_flush_for_sender(
        mut ctx: StoreContextMut<'_, InstanceData>,
        channel_ref: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> anyhow::Result<i32> {
        let channel_id = HostResource::from_ref(channel_ref.as_ref())?.as_sender()?;
        let mut sender = ctx.data_mut().inner.sender(channel_id);

        let poll_result = sender.poll_flush();
        let mut poll_cx = WasmContext::new(ctx.as_context_mut(), poll_cx);
        let poll_result = poll_result.into_inner(&mut poll_cx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }
}

/// Task-related functions exported to WASM.
impl WorkflowFunctions {
    pub fn poll_task_completion(
        mut ctx: StoreContextMut<'_, InstanceData>,
        task_id: TaskId,
        poll_cx: WasmContextPtr,
    ) -> anyhow::Result<i64> {
        ensure!(
            ctx.data().inner.persisted.task(task_id).is_some(),
            "task {task_id} does not exist in the workflow"
        );

        let poll_result = ctx.data_mut().inner.task(task_id).poll_completion();
        let mut poll_cx = WasmContext::new(ctx.as_context_mut(), poll_cx);
        let poll_result = poll_result.into_inner(&mut poll_cx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn spawn_task(
        mut ctx: StoreContextMut<'_, InstanceData>,
        task_name_ptr: u32,
        task_name_len: u32,
        task_id: TaskId,
    ) -> anyhow::Result<()> {
        let memory = ctx.data().exports().memory;
        let task_name = copy_string_from_wasm(&ctx, &memory, task_name_ptr, task_name_len)?;
        ctx.data_mut().inner.spawn_task(task_id, task_name)
    }

    pub fn wake_task(
        mut ctx: StoreContextMut<'_, InstanceData>,
        task_id: TaskId,
    ) -> anyhow::Result<()> {
        ensure!(
            ctx.data().inner.persisted.task(task_id).is_some(),
            "task {task_id} does not exist in the workflow"
        );

        ctx.data_mut().inner.task(task_id).schedule_wakeup();
        Ok(())
    }

    pub fn schedule_task_abortion(
        mut ctx: StoreContextMut<'_, InstanceData>,
        task_id: TaskId,
    ) -> anyhow::Result<()> {
        ensure!(
            ctx.data().inner.persisted.task(task_id).is_some(),
            "task {task_id} does not exist in the workflow"
        );

        ctx.data_mut().inner.task(task_id).schedule_abortion();
        Ok(())
    }

    pub fn report_task_error(
        ctx: StoreContextMut<'_, InstanceData>,
        message_ptr: u32,
        message_len: u32,
        filename_ptr: u32,
        filename_len: u32,
        line: u32,
        column: u32,
    ) -> anyhow::Result<()> {
        Self::report_error_or_panic(
            ctx,
            ReportedErrorKind::TaskError,
            message_ptr,
            message_len,
            filename_ptr,
            filename_len,
            line,
            column,
        )
    }
}

/// Timer-related functions exported to WASM.
impl WorkflowFunctions {
    #[allow(clippy::needless_pass_by_value)] // for uniformity with other functions
    pub fn current_timestamp(ctx: StoreContextMut<'_, InstanceData>) -> i64 {
        ctx.data().inner.current_timestamp().timestamp_millis()
    }

    pub fn create_timer(
        mut ctx: StoreContextMut<'_, InstanceData>,
        timestamp_millis: i64,
    ) -> anyhow::Result<TimerId> {
        let definition = Self::timer_definition(timestamp_millis)?;
        Ok(ctx.data_mut().inner.create_timer(definition))
    }

    fn timer_definition(timestamp_millis: i64) -> anyhow::Result<TimerDefinition> {
        let expires_at = Utc
            .timestamp_millis_opt(timestamp_millis)
            .single()
            .ok_or_else(|| anyhow!("timestamp overflow"))?;
        Ok(TimerDefinition { expires_at })
    }

    pub fn drop_timer(
        mut ctx: StoreContextMut<'_, InstanceData>,
        timer_id: TimerId,
    ) -> anyhow::Result<()> {
        // We don't use resources for timers; thus, need to check whether the timer ID is valid.
        ensure!(
            ctx.data().inner.persisted.timers.get(timer_id).is_some(),
            "timer {timer_id} does not exist in the workflow"
        );

        let waker_ids = ctx.data_mut().inner.timer(timer_id).drop();
        let exports = ctx.data().exports();
        for waker_id in waker_ids {
            exports.drop_waker(ctx.as_context_mut(), waker_id).ok();
        }
        Ok(())
    }

    pub fn poll_timer(
        mut ctx: StoreContextMut<'_, InstanceData>,
        timer_id: TimerId,
        poll_cx: WasmContextPtr,
    ) -> anyhow::Result<i64> {
        // We don't use resources for timers; thus, need to check whether the timer ID is valid.
        ensure!(
            ctx.data().inner.persisted.timers.get(timer_id).is_some(),
            "timer {timer_id} does not exist in the workflow"
        );

        let poll_result = ctx.data_mut().inner.timer(timer_id).poll();
        let mut poll_cx = WasmContext::new(ctx.as_context_mut(), poll_cx);
        let poll_result = poll_result.into_inner(&mut poll_cx)?;
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }
}

#[derive(Debug)]
pub(super) struct TracingFunctions;

impl TracingFunctions {
    #[allow(clippy::needless_pass_by_value)] // required by wasmtime
    pub fn send_trace(
        mut ctx: StoreContextMut<'_, InstanceData>,
        trace_ptr: u32,
        trace_len: u32,
    ) -> anyhow::Result<()> {
        let memory = ctx.data().exports().memory;
        let trace = copy_string_from_wasm(&ctx, &memory, trace_ptr, trace_len)?;
        let trace: TracingEvent =
            serde_json::from_str(&trace).context("`TracingEvent` deserialization failed")?;
        ctx.data_mut().inner.send_trace(trace);
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct SpawnFunctions;

#[allow(clippy::needless_pass_by_value)]
impl SpawnFunctions {
    pub fn workflow_interface(
        ctx: StoreContextMut<'_, InstanceData>,
        id_ptr: u32,
        id_len: u32,
    ) -> anyhow::Result<i64> {
        let memory = ctx.data().exports().memory;
        let id = copy_string_from_wasm(&ctx, &memory, id_ptr, id_len)?;

        let interface = ctx.data().inner.workflow_interface(&id);
        let interface = interface.map(|interface| interface.to_bytes());
        interface.into_wasm(&mut WasmAllocator::new(ctx))
    }

    pub fn spawn(
        mut ctx: StoreContextMut<'_, InstanceData>,
        stub_id: WorkflowId,
        definition_id_ptr: u32,
        definition_id_len: u32,
        args_ptr: u32,
        args_len: u32,
        handles: Option<ExternRef>,
    ) -> anyhow::Result<()> {
        let memory = ctx.data().exports().memory;
        let definition_id =
            copy_string_from_wasm(&ctx, &memory, definition_id_ptr, definition_id_len)?;
        let args = copy_bytes_from_wasm(&ctx, &memory, args_ptr, args_len)?;
        let handles = HostResource::from_ref(handles.as_ref())?.as_channel_handles()?;
        let handles = handles.inner.lock().unwrap();

        ctx.data_mut()
            .inner
            .create_workflow_stub(stub_id, &definition_id, args, handles.clone())
    }

    pub fn poll_workflow_completion(
        mut ctx: StoreContextMut<'_, InstanceData>,
        child: Option<ExternRef>,
        poll_cx: WasmContextPtr,
    ) -> anyhow::Result<i64> {
        let child_id = HostResource::from_ref(child.as_ref())?.as_workflow()?;
        let poll_result = ctx.data_mut().inner.child(child_id).poll_completion();

        let mut poll_cx = WasmContext::new(ctx.as_context_mut(), poll_cx);
        let poll_result = poll_result.into_inner(&mut poll_cx)?;
        let poll_result = poll_result.map(utils::extract_task_poll_result);
        poll_result.into_wasm(&mut WasmAllocator::new(ctx))
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(child_id))]
    pub fn completion_error(
        ctx: StoreContextMut<'_, InstanceData>,
        workflow: Option<ExternRef>,
    ) -> anyhow::Result<i64> {
        let child_id = HostResource::from_ref(workflow.as_ref())?.as_workflow()?;
        tracing::Span::current().record("child_id", child_id);

        let persisted = &ctx.data().inner.persisted;
        let poll_result = persisted.child_workflow(child_id).unwrap().result();
        let maybe_err = match poll_result {
            Poll::Ready(Err(JoinError::Err(err))) => Some(err),
            _ => None,
        };
        let maybe_err = maybe_err.map(TaskError::clone_boxed);

        Ok(maybe_err
            .map(|err| err.into_wasm(&mut WasmAllocator::new(ctx)))
            .transpose()?
            .unwrap_or(0))
    }
}
