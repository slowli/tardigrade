//! Mappings between runtime types and Protobuf messages.

use chrono::{DateTime, TimeZone, Utc};
use prost_types::Timestamp;

use std::{collections::HashMap, task::Poll};

use crate::proto::{
    self, join_error, persisted_workflow, Channel, HandleType, Message, MessageRef, Module,
    Workflow, WorkflowDefinition,
};
use tardigrade::{
    handle::Handle,
    interface::{ArgsSpec, HandleSpec, Interface},
    task::{ErrorContext, ErrorLocation, JoinError, TaskError},
    ChannelId, Raw, WorkflowId,
};
use tardigrade_rt::{
    engine::PersistedWorkflowData,
    handle::ReceivedMessage,
    manager::{TickResult, WouldBlock},
    receipt::ExecutionError,
    storage::{
        ActiveWorkflowState, ChannelRecord, CompletedWorkflowState, DefinitionRecord,
        ErroredWorkflowState, ModuleRecord, WorkflowRecord, WorkflowState,
    },
    Channels, ChildWorkflow, ReceiverState, SenderState, TaskState,
};

pub(crate) fn from_timestamp(mut ts: Timestamp) -> Option<DateTime<Utc>> {
    ts.normalize();
    let nanos = u32::try_from(ts.nanos).unwrap();
    // ^ `unwrap()` is safe due to `Timestamp::normalize()`.
    let timestamp = Utc.timestamp_opt(ts.seconds, nanos);
    timestamp.single()
}

fn to_timestamp(ts: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: ts.timestamp(),
        nanos: ts.timestamp_subsec_nanos() as i32,
    }
}

impl Module {
    pub(crate) fn from_record(record: ModuleRecord) -> Self {
        let definitions = record
            .definitions
            .into_iter()
            .map(|(name, def)| (name, WorkflowDefinition::from_record(def)));

        Self {
            id: record.id,
            definitions: definitions.collect(),
        }
    }
}

impl WorkflowDefinition {
    pub(crate) fn from_record(record: DefinitionRecord) -> Self {
        Self {
            interface: Some(proto::Interface::from_data(&record.interface)),
        }
    }
}

impl proto::Interface {
    fn from_data(interface: &Interface) -> Self {
        let handles = interface
            .handles()
            .map(|(path, spec)| (path.to_string(), proto::HandleSpec::from_data(spec)));

        Self {
            args: Some(proto::ArgsSpec::from_data(interface.args())),
            handles: handles.collect(),
        }
    }
}

impl proto::ArgsSpec {
    fn from_data(spec: &ArgsSpec) -> Self {
        Self {
            description: spec.description.clone(),
        }
    }
}

impl proto::HandleSpec {
    fn from_data(spec: &HandleSpec) -> Self {
        match spec {
            HandleSpec::Receiver(spec) => Self {
                r#type: HandleType::Receiver as i32,
                description: spec.description.clone(),
                capacity: None,
            },
            HandleSpec::Sender(spec) => Self {
                r#type: HandleType::Sender as i32,
                description: spec.description.clone(),
                capacity: spec
                    .capacity
                    .and_then(|capacity| u32::try_from(capacity).ok()),
            },
        }
    }
}

impl Channel {
    pub(crate) fn from_record(id: ChannelId, record: ChannelRecord) -> Self {
        Self {
            id,
            receiver_workflow_id: record.receiver_workflow_id,
            sender_workflow_ids: record.sender_workflow_ids.into_iter().collect(),
            has_external_sender: record.has_external_sender,
            is_closed: record.is_closed,
            received_messages: record.received_messages as u64,
        }
    }
}

impl Message {
    pub(crate) fn from_data(channel_id: ChannelId, message: ReceivedMessage<Vec<u8>, Raw>) -> Self {
        Self {
            reference: Some(MessageRef {
                channel_id,
                index: message.index() as u64,
            }),
            payload: message.into_bytes(),
        }
    }
}

impl Workflow {
    pub(crate) fn from_record(record: WorkflowRecord) -> Self {
        Self {
            id: record.id,
            parent_id: record.parent_id,
            module_id: record.module_id,
            name_in_module: record.name_in_module,
            execution_count: record.execution_count as u32,
            state: Some(proto::workflow::State::from_record(record.state)),
        }
    }
}

impl proto::workflow::State {
    fn from_record(state: WorkflowState) -> Self {
        match state {
            WorkflowState::Active(state) => {
                Self::Active(proto::ActiveWorkflowState::from_record(*state))
            }
            WorkflowState::Errored(state) => {
                Self::Errored(proto::ErroredWorkflowState::from_record(state))
            }
            WorkflowState::Completed(state) => {
                Self::Completed(proto::CompletedWorkflowState::from_record(state))
            }
        }
    }
}

impl proto::ActiveWorkflowState {
    fn from_record(state: ActiveWorkflowState) -> Self {
        Self {
            persisted: Some(proto::PersistedWorkflow::from_data(
                state.persisted.common(),
            )),
        }
    }
}

impl proto::ErroredWorkflowState {
    fn from_record(state: ErroredWorkflowState) -> Self {
        let erroneous_messages = state
            .erroneous_messages
            .into_iter()
            .map(|message_ref| MessageRef {
                channel_id: message_ref.channel_id,
                index: message_ref.index as u64,
            })
            .collect();

        Self {
            persisted: Some(proto::PersistedWorkflow::from_data(
                state.persisted.common(),
            )),
            error: Some(proto::ExecutionError::from_data(&state.error)),
            erroneous_messages,
        }
    }
}

impl proto::ExecutionError {
    fn from_data(error: &ExecutionError) -> Self {
        let panic_info = error.panic_info();
        Self {
            trap_message: error.trap().to_string(),
            error_message: panic_info.and_then(|panic| panic.message.clone()),
            error_location: panic_info
                .and_then(|panic| panic.location.as_ref())
                .map(proto::ErrorLocation::from_data),
        }
    }
}

impl proto::CompletedWorkflowState {
    fn from_record(state: CompletedWorkflowState) -> Self {
        Self {
            error: match state.result {
                Ok(()) => None,
                Err(err) => Some(proto::JoinError::from_data(&err)),
            },
        }
    }
}

impl proto::PersistedWorkflow {
    fn from_data(persisted: &PersistedWorkflowData) -> Self {
        Self {
            tasks: persisted
                .tasks()
                .map(|(id, state)| (id, persisted_workflow::Task::from_data(state)))
                .collect(),
            child_workflows: persisted
                .child_workflows()
                .map(|(id, state)| (id, persisted_workflow::Child::from_data(&state)))
                .collect(),
            channels: map_channels(persisted.channels()),
        }
    }
}

fn map_channels(channels: Channels<'_>) -> HashMap<String, persisted_workflow::Channel> {
    let it = channels.handles().map(|(path, state)| {
        let channel_id = channels.channel_id(path).unwrap();
        let channel = persisted_workflow::Channel::from_data(channel_id, state);
        (path.to_string(), channel)
    });
    it.collect()
}

impl persisted_workflow::Task {
    fn from_data(state: &TaskState) -> Self {
        Self {
            name: state.name().to_owned(),
            is_completed: state.result().is_ready(),
            completion_error: match state.result() {
                Poll::Ready(Err(err)) => Some(proto::JoinError::from_data(err)),
                _ => None,
            },
            spawned_by: state.spawned_by(),
        }
    }
}

impl persisted_workflow::Child {
    fn from_data(state: &ChildWorkflow) -> Self {
        Self {
            channels: map_channels(state.channels()),
            is_completed: state.result().is_ready(),
            completion_error: match state.result() {
                Poll::Ready(Err(err)) => Some(proto::JoinError::from_data(err)),
                _ => None,
            },
        }
    }
}

impl persisted_workflow::Channel {
    fn from_data(id: ChannelId, state: Handle<&ReceiverState, &SenderState>) -> Self {
        use persisted_workflow::channel;

        Self {
            id,
            state: Some(match state {
                Handle::Receiver(state) => {
                    channel::State::Receiver(persisted_workflow::Receiver::from_data(state))
                }
                Handle::Sender(state) => {
                    channel::State::Sender(persisted_workflow::Sender::from_data(state))
                }
            }),
        }
    }
}

impl persisted_workflow::Receiver {
    fn from_data(state: &ReceiverState) -> Self {
        Self {
            is_closed: state.is_closed(),
            received_message_count: state.received_message_count() as u64,
        }
    }
}

impl persisted_workflow::Sender {
    fn from_data(state: &SenderState) -> Self {
        Self {
            is_closed: state.is_closed(),
            flushed_message_count: state.flushed_message_count() as u64,
        }
    }
}

impl proto::JoinError {
    fn from_data(err: &JoinError) -> Self {
        Self {
            r#type: Some(match err {
                JoinError::Aborted => join_error::Type::Aborted(()),
                JoinError::Err(err) => {
                    join_error::Type::TaskError(proto::TaskError::from_data(err))
                }
            }),
        }
    }
}

impl proto::TaskError {
    fn from_data(err: &TaskError) -> Self {
        Self {
            cause: err.cause().to_string(),
            location: Some(proto::ErrorLocation::from_data(err.location())),
            contexts: err
                .contexts()
                .iter()
                .map(proto::ErrorContext::from_data)
                .collect(),
        }
    }
}

impl proto::ErrorLocation {
    fn from_data(location: &ErrorLocation) -> Self {
        Self {
            filename: location.filename.to_string(),
            line: location.line,
            column: location.column,
        }
    }
}

impl proto::ErrorContext {
    fn from_data(context: &ErrorContext) -> Self {
        Self {
            message: context.message().to_owned(),
            location: Some(proto::ErrorLocation::from_data(context.location())),
        }
    }
}

type ExtendedWouldBlock = (Option<WorkflowId>, WouldBlock);

impl proto::TickResult {
    pub(crate) fn from_data(result: Result<TickResult, ExtendedWouldBlock>) -> Self {
        let (workflow_id, outcome) = match result {
            Ok(result) => {
                let outcome = match result.as_ref() {
                    Ok(_) => proto::tick_result::Outcome::Ok(()),
                    Err(err) => {
                        let err = proto::ExecutionError::from_data(err);
                        proto::tick_result::Outcome::Error(err)
                    }
                };
                (Some(result.workflow_id()), outcome)
            }
            Err((id, would_block)) => {
                let would_block = proto::WouldBlock::from_data(&would_block);
                let outcome = proto::tick_result::Outcome::WouldBlock(would_block);
                (id, outcome)
            }
        };

        Self {
            workflow_id,
            outcome: Some(outcome),
        }
    }
}

impl proto::WouldBlock {
    fn from_data(data: &WouldBlock) -> Self {
        Self {
            nearest_timer: data.nearest_timer_expiration().map(to_timestamp),
        }
    }
}
