//! Workflow-related types.
//!
//! See [the crate docs](crate) for an intro on workflows.
//!
//! # Examples
//!
//! Simple workflow definition:
//!
//! ```
//! # use futures::StreamExt;
//! # use serde::{Deserialize, Serialize};
//! use tardigrade::{
//!     channel::{Sender, Receiver},
//!     workflow::{GetInterface, Handle, Init, SpawnWorkflow, TaskHandle, Wasm},
//!     Data, Json,
//! };
//!
//! /// Workflow type. Usually, this should be a unit / empty struct.
//! #[derive(Debug, GetInterface)]
//! # #[tardigrade(interface = r#"{"v":0}"#)]
//! pub struct MyWorkflow(());
//!
//! /// Handle for the workflow. Fields are public for integration testing.
//! #[tardigrade::handle(for = "MyWorkflow")]
//! #[derive(Debug)]
//! pub struct MyHandle<Env> {
//!     /// Data input.
//!     pub input: Handle<Data<Input, Json>, Env>,
//!     /// Inbound channel with commands.
//!     pub commands: Handle<Receiver<Command, Json>, Env>,
//!     /// Outbound channel with events.
//!     pub events: Handle<Sender<Event, Json>, Env>,
//! }
//!
//! /// Input provided to the workflow. Since it's a single input,
//! /// it also acts as the initializer.
//! #[tardigrade::init(for = "MyWorkflow", codec = "Json")]
//! #[derive(Debug, Serialize, Deserialize)]
//! pub struct Input {
//!     pub start_counter: u32,
//! }
//!
//! /// Commands received via `commands` channel.
//! #[derive(Debug, Serialize, Deserialize)]
//! pub enum Command {
//!     Ping(String),
//!     // other variants...
//! }
//!
//! /// Events emitted via `events` channel.
//! #[derive(Debug, Serialize, Deserialize)]
//! pub enum Event {
//!     Pong(String),
//!     // other variants...
//! }
//!
//! impl MyHandle<Wasm> {
//!     async fn process_command(&mut self, command: &Command) {
//!         match command {
//!             Command::Ping(ping) => {
//!                 let counter = &mut self.input.as_mut().start_counter;
//!                 let pong = format!("{}, counter={}", ping, *counter);
//!                 *counter += 1;
//!                 self.events.send(Event::Pong(pong)).await;
//!             }
//!             // other commands...
//!         }
//!     }
//! }
//!
//! // Actual workflow logic.
//! impl SpawnWorkflow for MyWorkflow {
//!     fn spawn(mut handle: MyHandle<Wasm>) -> TaskHandle {
//!         TaskHandle::new(async move {
//!             while let Some(command) = handle.commands.next().await {
//!                 handle.process_command(&command).await;
//!             }
//!         })
//!     }
//! }
//!
//! tardigrade::workflow_entry!(MyWorkflow);
//! ```

use std::{collections::HashMap, fmt, future::Future, ops};

mod handle;
mod init;

pub use self::{
    handle::{Handle, TakeHandle},
    init::{Init, Initialize, Inputs, InputsBuilder},
};

/// Derives the [`GetInterface`] trait for a workflow type.
///
/// [`GetInterface`]: trait@GetInterface
#[cfg(feature = "derive")]
pub use tardigrade_derive::GetInterface;

use crate::{
    channel::{RawReceiver, RawSender},
    interface::{AccessError, DataInput, InboundChannel, Interface, OutboundChannel},
    RawData,
};

#[cfg(target_arch = "wasm32")]
mod imp {
    use crate::task::imp::RawTaskHandle;

    pub(super) type TaskHandle = RawTaskHandle;
}

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use std::{fmt, future::Future, pin::Pin};

    #[repr(transparent)]
    pub(super) struct TaskHandle(pub Pin<Box<dyn Future<Output = ()>>>);

    impl fmt::Debug for TaskHandle {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.debug_tuple("_").finish()
        }
    }

    impl TaskHandle {
        pub fn new(future: impl Future<Output = ()> + 'static) -> Self {
            Self(Box::pin(future))
        }
    }
}

/// Allows obtaining an [`Interface`] for a workflow.
///
/// This trait should be derived for workflow types using the corresponding macro.
pub trait GetInterface {
    /// Obtains the workflow interface.
    fn interface() -> Interface<Self>;
}

/// WASM environment.
///
/// This type is used as a type param for the [`TakeHandle`] trait. The returned handles
/// are ones provided via Tardigrade runtime imports for the WASM module, or emulated
/// in case of [tests](crate::test).
#[derive(Debug, Default)]
pub struct Wasm(());

/// Workflow that can be spawned.
///
/// As the supertraits imply, the workflow needs to be able to:
///
/// - Describe its interface
/// - Take necessary channel / data input handles from the [`Wasm`] environment
/// - Initialize from data inputs.
///
/// The supertraits are usually derived for workflow types using the corresponding macros,
/// while `SpawnWorkflow` itself is easy to implement manually.
pub trait SpawnWorkflow: GetInterface + TakeHandle<Wasm, Id = ()> + Initialize<Id = ()> {
    /// Spawns a workflow instance.
    fn spawn(handle: Self::Handle) -> TaskHandle;
}

/// Handle to a task, essentially equivalent to a boxed [`Future`].
#[derive(Debug)]
#[repr(transparent)]
pub struct TaskHandle(imp::TaskHandle);

impl TaskHandle {
    /// Creates a handle.
    pub fn new(future: impl Future<Output = ()> + 'static) -> Self {
        Self(imp::TaskHandle::new(future))
    }

    #[doc(hidden)] // only used in the `workflow_entry` macro
    pub fn from_workflow<W: SpawnWorkflow>() -> Result<Self, AccessError> {
        let mut wasm = Wasm::default();
        let handle = <W as TakeHandle<Wasm>>::take_handle(&mut wasm, &())?;
        Ok(W::spawn(handle))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn into_inner(self) -> std::pin::Pin<Box<dyn Future<Output = ()>>> {
        self.0 .0
    }
}

/// Dynamically-typed handle to a workflow containing handles to its inputs
/// and channels.
pub struct UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    data_inputs: HashMap<String, <RawData as TakeHandle<Env>>::Handle>,
    inbound_channels: HashMap<String, <RawReceiver as TakeHandle<Env>>::Handle>,
    outbound_channels: HashMap<String, <RawSender as TakeHandle<Env>>::Handle>,
}

#[allow(clippy::type_repetition_in_bounds)] // false positive
impl<Env> fmt::Debug for UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    <RawData as TakeHandle<Env>>::Handle: fmt::Debug,
    RawReceiver: TakeHandle<Env, Id = str>,
    <RawReceiver as TakeHandle<Env>>::Handle: fmt::Debug,
    RawSender: TakeHandle<Env, Id = str>,
    <RawSender as TakeHandle<Env>>::Handle: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UntypedHandle")
            .field("data_inputs", &self.data_inputs)
            .field("inbound_channels", &self.inbound_channels)
            .field("outbound_channels", &self.outbound_channels)
            .finish()
    }
}

impl<Env> TakeHandle<Env> for UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
    Interface<()>: TakeHandle<Env, Id = (), Handle = Interface<()>>,
{
    type Id = ();
    type Handle = Self;

    fn take_handle(env: &mut Env, _id: &()) -> Result<Self, AccessError> {
        let interface = Interface::<()>::take_handle(env, &())?;

        let data_inputs = interface
            .data_inputs()
            .map(|(name, _)| Ok((name.to_owned(), RawData::take_handle(&mut *env, name)?)))
            .collect::<Result<_, _>>()?;
        let inbound_channels = interface
            .inbound_channels()
            .map(|(name, _)| Ok((name.to_owned(), RawReceiver::take_handle(&mut *env, name)?)))
            .collect::<Result<_, _>>()?;
        let outbound_channels = interface
            .outbound_channels()
            .map(|(name, _)| Ok((name.to_owned(), RawSender::take_handle(&mut *env, name)?)))
            .collect::<Result<_, _>>()?;

        Ok(Self {
            data_inputs,
            inbound_channels,
            outbound_channels,
        })
    }
}

impl<Env> ops::Index<DataInput<'_>> for UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    type Output = <RawData as TakeHandle<Env>>::Handle;

    fn index(&self, index: DataInput<'_>) -> &Self::Output {
        self.data_inputs
            .get(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<Env> ops::IndexMut<DataInput<'_>> for UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    fn index_mut(&mut self, index: DataInput<'_>) -> &mut Self::Output {
        self.data_inputs
            .get_mut(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<Env> ops::Index<InboundChannel<'_>> for UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    type Output = <RawReceiver as TakeHandle<Env>>::Handle;

    fn index(&self, index: InboundChannel<'_>) -> &Self::Output {
        self.inbound_channels
            .get(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<Env> ops::IndexMut<InboundChannel<'_>> for UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    fn index_mut(&mut self, index: InboundChannel<'_>) -> &mut Self::Output {
        self.inbound_channels
            .get_mut(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<Env> ops::Index<OutboundChannel<'_>> for UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    type Output = <RawSender as TakeHandle<Env>>::Handle;

    fn index(&self, index: OutboundChannel<'_>) -> &Self::Output {
        self.outbound_channels
            .get(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<Env> ops::IndexMut<OutboundChannel<'_>> for UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    fn index_mut(&mut self, index: OutboundChannel<'_>) -> &mut Self::Output {
        self.outbound_channels
            .get_mut(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}
