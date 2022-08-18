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
//!     workflow::{GetInterface, Handle, SpawnWorkflow, TaskHandle, Wasm, WorkflowFn},
//!     Json,
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
//!     /// Inbound channel with commands.
//!     pub commands: Handle<Receiver<Command, Json>, Env>,
//!     /// Outbound channel with events.
//!     pub events: Handle<Sender<Event, Json>, Env>,
//! }
//!
//! /// Args provided to the workflow on creation.
//! #[derive(Debug, Serialize, Deserialize)]
//! pub struct Args {
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
//!     async fn process_command(&mut self, command: &Command, counter: &mut u32) {
//!         match command {
//!             Command::Ping(ping) => {
//!                 let pong = format!("{}, counter={}", ping, *counter);
//!                 *counter += 1;
//!                 self.events.send(Event::Pong(pong)).await;
//!             }
//!             // other commands...
//!         }
//!     }
//! }
//!
//! // Workflow interface declaration.
//! impl WorkflowFn for MyWorkflow {
//!     type Args = Args;
//!     type Codec = Json;
//! }
//!
//! // Actual workflow logic.
//! impl SpawnWorkflow for MyWorkflow {
//!     fn spawn(mut args: Args, mut handle: MyHandle<Wasm>) -> TaskHandle {
//!         TaskHandle::new(async move {
//!             while let Some(command) = handle.commands.next().await {
//!                 handle.process_command(&command, &mut args.start_counter).await;
//!             }
//!         })
//!     }
//! }
//!
//! tardigrade::workflow_entry!(MyWorkflow);
//! ```

use std::{future::Future, mem};

/// Derives the [`GetInterface`] trait for a workflow type.
///
/// [`GetInterface`]: trait@GetInterface
#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use tardigrade_derive::GetInterface;

use crate::{
    interface::{AccessError, AccessErrorKind, Interface, InterfaceLocation, ValidateInterface},
    Decode, Encode, Raw,
};

mod handle;
mod untyped;

pub use self::{
    handle::{EnvExtensions, ExtendEnv, Handle, TakeHandle},
    untyped::{UntypedHandle, UntypedHandleIndex},
};

#[cfg(target_arch = "wasm32")]
mod imp {
    use std::{panic::PanicInfo, ptr};

    use crate::task::imp::RawTaskHandle;

    pub(super) type TaskHandle = RawTaskHandle;

    #[link(wasm_import_module = "tardigrade_rt")]
    extern "C" {
        #[link_name = "panic"]
        fn report_panic(
            message_ptr: *const u8,
            message_len: usize,
            filename_ptr: *const u8,
            filename_len: usize,
            line: u32,
            column: u32,
        );
    }

    pub(super) fn handle_panic(panic_info: &PanicInfo<'_>) {
        let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            Some(*s)
        } else {
            panic_info
                .payload()
                .downcast_ref::<String>()
                .map(String::as_str)
        };
        if let Some(location) = panic_info.location() {
            unsafe {
                report_panic(
                    message.map_or_else(ptr::null, str::as_ptr),
                    message.map_or(0, str::len),
                    location.file().as_ptr(),
                    location.file().len(),
                    location.line(),
                    location.column(),
                );
            }
        } else {
            unsafe {
                report_panic(
                    message.map_or_else(ptr::null, str::as_ptr),
                    message.map_or(0, str::len),
                    ptr::null(),
                    0,
                    0, // line
                    0, // column
                );
            }
        }
    }
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
    /// Sets the panic hook to pass panic messages to the host.
    #[doc(hidden)] // used by the `workflow_entry!` macro
    pub fn set_panic_hook() {
        #[cfg(target_arch = "wasm32")]
        std::panic::set_hook(Box::new(imp::handle_panic));
    }

    #[doc(hidden)] // used only by proc macros
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

    #[doc(hidden)] // used only by proc macros
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

/// Functional interface of a workflow.
pub trait WorkflowFn {
    /// Argument(s) supplied to the workflow on its creation.
    type Args;
    /// Codec used for [`Self::Args`] to encode / decode the arguments in order to pass them from
    /// the host to WASM.
    type Codec: Default + Encode<Self::Args> + Decode<Self::Args>;
}

impl WorkflowFn for () {
    type Args = Vec<u8>;
    type Codec = Raw;
}

/// Workflow that can be spawned.
pub trait SpawnWorkflow: GetInterface + TakeHandle<Wasm, Id = ()> + WorkflowFn {
    /// Spawns a workflow instance.
    fn spawn(args: Self::Args, handle: Self::Handle) -> TaskHandle;
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
    pub fn from_workflow<W: SpawnWorkflow>(raw_data: Vec<u8>) -> Result<Self, AccessError> {
        let data = W::Codec::default()
            .try_decode_bytes(raw_data)
            .map_err(|err| {
                AccessErrorKind::Custom(Box::new(err)).with_location(InterfaceLocation::Args)
            })?;
        let mut wasm = Wasm::default();
        let handle = <W as TakeHandle<Wasm>>::take_handle(&mut wasm, &())?;
        Ok(W::spawn(data, handle))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn into_inner(self) -> std::pin::Pin<Box<dyn Future<Output = ()>>> {
        self.0 .0
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
