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

use std::{collections::HashMap, fmt, future::Future, mem, ops};

mod handle;
mod init;

pub use self::{
    handle::{EnvExtensions, ExtendEnv, Handle, TakeHandle},
    init::{Init, Initialize, Inputs, InputsBuilder},
};

/// Derives the [`GetInterface`] trait for a workflow type.
///
/// [`GetInterface`]: trait@GetInterface
#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use tardigrade_derive::GetInterface;
use tardigrade_shared::interface::ValidateInterface;

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
pub trait GetInterface: ValidateInterface<Id = ()> {
    /// Name of the workflow. This name is used in workflow module definitions.
    const WORKFLOW_NAME: &'static str;
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

impl Wasm {
    #[doc(hidden)]
    pub const fn custom_section_len(name: &str, serialized_interface: &[u8]) -> usize {
        Self::leb128_len(name.len())
            + name.len()
            + Self::leb128_len(serialized_interface.len())
            + serialized_interface.len()
    }

    /// Returns the number of bytes in the unsigned LEB128 encoding of `value`.
    const fn leb128_len(value: usize) -> usize {
        let bits_count = mem::size_of::<usize>() * 8 - value.leading_zeros() as usize;
        if bits_count == 0 {
            1
        } else {
            (bits_count + 6) / 7 // == ceil(bits_count / 7)
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    const fn write_leb128<const N: usize>(
        mut buffer: [u8; N],
        mut value: usize,
        mut pos: usize,
    ) -> ([u8; N], usize) {
        loop {
            let low_bits = (value & 127) as u8;
            value >>= 7;
            buffer[pos] = low_bits;
            if value > 0 {
                buffer[pos] += 128; // Set the continuation bit
                pos += 1;
            } else {
                pos += 1;
                break;
            }
        }
        (buffer, pos)
    }

    #[doc(hidden)]
    pub const fn custom_section<const N: usize>(
        name: &str,
        serialized_interface: &[u8],
    ) -> [u8; N] {
        debug_assert!(N == Self::custom_section_len(name, serialized_interface));

        let (mut buffer, pos) = Self::write_leb128([0; N], name.len(), 0);
        let mut i = 0;
        while i < name.len() {
            buffer[pos + i] = name.as_bytes()[i];
            i += 1;
        }

        let (mut buffer, pos) =
            Self::write_leb128(buffer, serialized_interface.len(), pos + name.len());
        let mut i = 0;
        while i < serialized_interface.len() {
            buffer[pos + i] = serialized_interface[i];
            i += 1;
        }
        buffer
    }
}

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

impl<Env> UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    /// Removes an element with the specified index from this handle. Returns `None` if
    /// the element is not present in the handle.
    pub fn remove<I>(&mut self, index: I) -> Option<I::Output>
    where
        I: UntypedHandleIndex<Env>,
    {
        index.remove_from(self)
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
            .collect::<Result<_, AccessError>>()?;
        let inbound_channels = interface
            .inbound_channels()
            .map(|(name, _)| Ok((name.to_owned(), RawReceiver::take_handle(&mut *env, name)?)))
            .collect::<Result<_, AccessError>>()?;
        let outbound_channels = interface
            .outbound_channels()
            .map(|(name, _)| Ok((name.to_owned(), RawSender::take_handle(&mut *env, name)?)))
            .collect::<Result<_, AccessError>>()?;

        Ok(Self {
            data_inputs,
            inbound_channels,
            outbound_channels,
        })
    }
}

/// Types that can be used for indexing [`UntypedHandle`].
pub trait UntypedHandleIndex<Env>: Copy + fmt::Display
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    /// Output type for the indexing operation.
    type Output;

    #[doc(hidden)]
    fn get_from(self, handle: &UntypedHandle<Env>) -> Option<&Self::Output>;

    #[doc(hidden)]
    fn get_mut_from(self, handle: &mut UntypedHandle<Env>) -> Option<&mut Self::Output>;

    #[doc(hidden)]
    fn remove_from(self, handle: &mut UntypedHandle<Env>) -> Option<Self::Output>;
}

macro_rules! impl_index {
    ($target:ty => $raw:ty, $field:ident) => {
        impl<Env> UntypedHandleIndex<Env> for $target
        where
            RawData: TakeHandle<Env, Id = str>,
            RawReceiver: TakeHandle<Env, Id = str>,
            RawSender: TakeHandle<Env, Id = str>,
        {
            type Output = <$raw as TakeHandle<Env>>::Handle;

            fn get_from(self, handle: &UntypedHandle<Env>) -> Option<&Self::Output> {
                handle.$field.get(self.0)
            }

            fn get_mut_from(self, handle: &mut UntypedHandle<Env>) -> Option<&mut Self::Output> {
                handle.$field.get_mut(self.0)
            }

            fn remove_from(self, handle: &mut UntypedHandle<Env>) -> Option<Self::Output> {
                handle.$field.remove(self.0)
            }
        }
    };
}

impl_index!(DataInput<'_> => RawData, data_inputs);
impl_index!(InboundChannel<'_> => RawReceiver, inbound_channels);
impl_index!(OutboundChannel<'_> => RawSender, outbound_channels);

impl<Env, I> ops::Index<I> for UntypedHandle<Env>
where
    I: UntypedHandleIndex<Env>,
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        index
            .get_from(self)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<Env, I> ops::IndexMut<I> for UntypedHandle<Env>
where
    I: UntypedHandleIndex<Env>,
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        index
            .get_mut_from(self)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leb128_len_is_computed_correctly() {
        assert_eq!(Wasm::leb128_len(0), 1);
        assert_eq!(Wasm::leb128_len(5), 1);
        assert_eq!(Wasm::leb128_len(127), 1);
        assert_eq!(Wasm::leb128_len(128), 2);
        assert_eq!(Wasm::leb128_len(256), 2);
        assert_eq!(Wasm::leb128_len(16_383), 2);
        assert_eq!(Wasm::leb128_len(16_384), 3);
        assert_eq!(Wasm::leb128_len(624_485), 3);
    }

    #[test]
    fn writing_leb128() {
        let (buffer, pos) = Wasm::write_leb128([0; 3], 624_485, 0);
        assert_eq!(buffer, [0xe5, 0x8e, 0x26]);
        assert_eq!(pos, 3);
    }

    struct SimpleInterface;

    impl SimpleInterface {
        const WORKFLOW_NAME: &'static str = "SimpleInterface";
        const SERIALIZED_INTERFACE: &'static [u8] = br#"{"v":0,"in":{"test":{}}}"#;
    }

    #[test]
    fn writing_custom_section() {
        const LEN: usize = Wasm::custom_section_len(
            SimpleInterface::WORKFLOW_NAME,
            SimpleInterface::SERIALIZED_INTERFACE,
        );
        const SECTION: [u8; LEN] = Wasm::custom_section(
            SimpleInterface::WORKFLOW_NAME,
            SimpleInterface::SERIALIZED_INTERFACE,
        );

        assert_eq!(
            usize::from(SECTION[0]),
            SimpleInterface::WORKFLOW_NAME.len()
        );
        assert_eq!(SECTION[1..16], *b"SimpleInterface");
        assert_eq!(
            usize::from(SECTION[16]),
            SimpleInterface::SERIALIZED_INTERFACE.len()
        );
        assert_eq!(SECTION[17..], *SimpleInterface::SERIALIZED_INTERFACE);
    }
}
