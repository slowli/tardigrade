//! Workflow-related types.
//!
//! See [the crate docs](crate) for an intro on workflows.
//!
//! # Examples
//!
//! Simple workflow definition:
//!
//! ```
//! # use async_trait::async_trait;
//! # use futures::{SinkExt, StreamExt};
//! # use serde::{Deserialize, Serialize};
//! use tardigrade::{
//!     channel::{Sender, Receiver}, task::TaskResult, workflow::*, Json,
//! };
//!
//! /// Handle for the workflow. Fields are public for integration testing.
//! #[derive(WithHandle, GetInterface, WorkflowEntry)]
//! #[tardigrade(derive(Debug), auto_interface)]
//! pub struct MyWorkflow<Fmt: HandleFormat = Wasm> {
//!     /// Receiver for commands.
//!     pub commands: InEnv<Receiver<Command, Json>, Fmt>,
//!     /// Sender for events.
//!     pub events: InEnv<Sender<Event, Json>, Fmt>,
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
//! impl MyWorkflow {
//!     async fn process_command(
//!         &mut self,
//!         command: &Command,
//!         counter: &mut u32,
//!     ) {
//!         match command {
//!             Command::Ping(ping) => {
//!                 let pong = format!("{}, counter={}", ping, *counter);
//!                 *counter += 1;
//!                 self.events.send(Event::Pong(pong)).await.ok();
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
//! #[async_trait(?Send)]
//! impl SpawnWorkflow for MyWorkflow {
//!     async fn spawn(args: Args, mut handle: Self) -> TaskResult {
//!         let mut counter = args.start_counter;
//!         while let Some(command) = handle.commands.next().await {
//!             handle
//!                 .process_command(&command, &mut counter)
//!                 .await;
//!         }
//!         Ok(())
//!     }
//! }
//! ```

use async_trait::async_trait;

use std::{borrow::Cow, future::Future, mem};

/// Derives the [`GetInterface`] trait for a workflow type.
///
/// [`GetInterface`]: trait@GetInterface
pub use tardigrade_derive::GetInterface;

/// Derives the [`WithHandle`] trait for a workflow type.
///
/// [`WithHandle`]: trait@WithHandle
pub use tardigrade_derive::WithHandle;

/// Derives the [`WorkflowEntry`] trait for a workflow type.
///
/// [`WorkflowEntry`]: trait@WorkflowEntry
pub use tardigrade_derive::WorkflowEntry;

mod handle;
mod untyped;

pub use self::{
    handle::{
        DelegateHandle, HandleFormat, InEnv, InsertHandles, IntoRaw, Inverse, TakeHandles,
        TryFromRaw, WithHandle,
    },
    untyped::UntypedHandles,
};

#[cfg(target_arch = "wasm32")]
#[doc(hidden)]
pub use crate::wasm_utils::HostHandles;

use crate::{
    channel::{RawReceiver, RawSender, Receiver, Sender},
    handle::{AccessError, AccessErrorKind, HandlePath},
    interface::{ArgsSpec, Interface, InterfaceBuilder, ReceiverSpec, SenderSpec},
    task::TaskResult,
    Codec, Raw,
};

#[cfg(target_arch = "wasm32")]
mod imp {
    use std::{panic::PanicInfo, ptr};

    use crate::task::imp::RawTaskHandle;

    pub(super) type TaskHandle = RawTaskHandle;

    #[link(wasm_import_module = "tardigrade_rt")]
    extern "C" {
        #[link_name = "report_panic"]
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

    use crate::task::TaskResult;

    #[repr(transparent)]
    pub(super) struct TaskHandle(pub Pin<Box<dyn Future<Output = TaskResult>>>);

    impl fmt::Debug for TaskHandle {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.debug_tuple("_").finish()
        }
    }

    impl TaskHandle {
        pub fn for_main_task(future: impl Future<Output = TaskResult> + 'static) -> Self {
            Self(Box::pin(future))
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
type HostHandles = UntypedHandles<Wasm>;

/// Allows obtaining an [`Interface`] for a workflow.
///
/// This trait should be derived for workflow types using the corresponding macro.
pub trait GetInterface: WithHandle + Sized + 'static {
    /// Obtains the workflow interface.
    ///
    /// The default implementation uses the [`WithHandle`] implementation to create
    /// an owned interface. The `GetInterface` derive macro provides a more efficient cached
    /// implementation.
    fn interface() -> Cow<'static, Interface> {
        Cow::Owned(interface_by_handle::<Self>())
    }
}

impl GetInterface for () {
    fn interface() -> Cow<'static, Interface> {
        Cow::Owned(Interface::default())
    }
}

#[doc(hidden)]
pub fn interface_by_handle<W: WithHandle>() -> Interface {
    struct Wrapper(InterfaceBuilder);

    impl TakeHandles<()> for Wrapper {
        fn take_receiver(&mut self, path: HandlePath<'_>) -> Result<(), AccessError> {
            self.0.insert_receiver(path, ReceiverSpec::default());
            Ok(())
        }

        fn take_sender(&mut self, path: HandlePath<'_>) -> Result<(), AccessError> {
            self.0.insert_sender(path, SenderSpec::default());
            Ok(())
        }

        fn drain(&mut self) -> UntypedHandles<()> {
            UntypedHandles::<()>::default() // never used
        }
    }

    let mut builder = Wrapper(InterfaceBuilder::new(ArgsSpec::default()));
    W::take_from_untyped(&mut builder, HandlePath::EMPTY).unwrap();
    builder.0.build()
}

/// Workflow that is accessible by its name from a module.
///
/// This trait should be automatically derived using the
/// [corresponding derive macro](macro@WorkflowEntry) since the macro additionally generates
/// the WASM entry point for the workflow.
pub trait WorkflowEntry: SpawnWorkflow {
    /// Name of the workflow.
    const WORKFLOW_NAME: &'static str;

    #[doc(hidden)]
    fn you_should_use_derive_macro_to_implement_this_trait();
}

/// WASM environment.
///
/// This type is a [`HandleFormat`] with [`Receiver`] and [`Sender`] handles being ones that
/// are available to the client workflow logic. Handles are provided by the Tardigrade runtime
/// when the workflow is initialized and can be created in runtime using the [`channel()`] function.
/// The whole process is emulated in case of [tests](crate::test).
///
/// [`channel()`]: crate::channel::channel()
#[derive(Debug, Default)]
pub struct Wasm;

impl HandleFormat for Wasm {
    type RawReceiver = RawReceiver;
    type Receiver<T, C: Codec<T>> = Receiver<T, C>;
    type RawSender = RawSender;
    type Sender<T, C: Codec<T>> = Sender<T, C>;
}

impl Wasm {
    /// Sets the panic hook to pass panic messages to the host.
    fn set_panic_hook() {
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
    type Args: Send;
    /// Codec used for [`Self::Args`] to encode / decode the arguments in order to pass them from
    /// the host to WASM.
    type Codec: Codec<Self::Args>;
}

impl WorkflowFn for () {
    type Args = Vec<u8>;
    type Codec = Raw;
}

/// Workflow that can be spawned.
///
/// This trait should be defined using the [`async_trait`] proc macro.
///
/// [`async_trait`]: https://docs.rs/async-trait/
///
/// # Examples
///
/// See [module docs](crate::workflow#examples) for workflow definition examples.
#[async_trait(?Send)]
pub trait SpawnWorkflow: GetInterface + WithHandle + WorkflowFn {
    /// Spawns the main task of the workflow.
    ///
    /// The workflow completes immediately when its main task completes,
    /// and its completion state is determined by the returned [`TaskResult`].
    /// (Cf. processes in operating systems.) Thus, if a workflow needs to wait for some
    /// [`spawn`]ed task, it needs to preserve its [`JoinHandle`], propagating [`TaskError`]s
    /// if necessary.
    ///
    /// See [`task`](crate::task#error-handling) module docs for more information
    /// about error handling.
    ///
    /// [`spawn`]: crate::task::spawn()
    /// [`JoinHandle`]: crate::task::JoinHandle
    /// [`TaskError`]: crate::task::TaskError
    async fn spawn(args: Self::Args, handle: InEnv<Self, Wasm>) -> TaskResult;
}

/// Handle to a task, essentially equivalent to a boxed [`Future`].
#[derive(Debug)]
#[repr(transparent)]
#[doc(hidden)] // only used by the `workflow_entry!` macro
pub struct TaskHandle(imp::TaskHandle);

impl TaskHandle {
    /// Creates a handle.
    pub(crate) fn new(future: impl Future<Output = TaskResult> + 'static) -> Self {
        Self(imp::TaskHandle::for_main_task(future))
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip_all, err, fields(args.len = raw_args.len()))
    )]
    pub(crate) fn from_workflow<W: SpawnWorkflow>(
        raw_args: Vec<u8>,
        mut raw_handles: HostHandles,
    ) -> Result<Self, AccessError> {
        let args = <W::Codec>::try_decode_bytes(raw_args)
            .map_err(|err| AccessErrorKind::Custom(Box::new(err)))?;
        let handle = W::take_from_untyped(&mut raw_handles, HandlePath::EMPTY)?;
        Ok(Self::new(W::spawn(args, handle)))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn into_inner(self) -> std::pin::Pin<Box<dyn Future<Output = TaskResult>>> {
        self.0 .0
    }
}

#[doc(hidden)] // only used by `workflow_entry!` macro
pub fn spawn_workflow<W: SpawnWorkflow>(raw_args: Vec<u8>, raw_handles: HostHandles) -> TaskHandle {
    Wasm::set_panic_hook();

    #[cfg(all(feature = "tracing", target_arch = "wasm32"))]
    tracing::subscriber::set_global_default(crate::tracing::new_subscriber()).ok();

    TaskHandle::from_workflow::<W>(raw_args, raw_handles).unwrap()
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
