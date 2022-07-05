use std::{collections::HashMap, future::Future, ops};

use crate::{
    channel::{RawReceiver, RawSender},
    RawData,
};
use tardigrade_shared::workflow::{
    DataInput, GetInterface, Inbound, Initialize, Interface, Outbound, TakeHandle,
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

/// WASM environment.
#[derive(Debug, Default)]
pub struct Wasm(());

pub trait SpawnWorkflow: GetInterface + TakeHandle<Wasm, Id = ()> + Initialize<Id = ()> {
    fn spawn(handle: Self::Handle) -> TaskHandle;
}

/// Handle to a task.
#[derive(Debug)]
#[repr(transparent)]
pub struct TaskHandle(imp::TaskHandle);

impl TaskHandle {
    /// Creates a handle.
    pub fn new(future: impl Future<Output = ()> + 'static) -> Self {
        Self(imp::TaskHandle::new(future))
    }

    /// Creates a handle from the specified workflow definition.
    pub fn from_workflow<W: SpawnWorkflow>() -> Self {
        let mut wasm = Wasm::default();
        let handle = <W as TakeHandle<Wasm>>::take_handle(&mut wasm, &());
        W::spawn(handle)
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

impl<Env> UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    // TODO: replace with `TakeHandle` impl
    pub fn new(env: &mut Env, interface: &Interface<()>) -> Self {
        let data_inputs = interface
            .data_inputs()
            .map(|(name, _)| (name.to_owned(), RawData::take_handle(&mut *env, name)))
            .collect();
        let inbound_channels = interface
            .inbound_channels()
            .map(|(name, _)| (name.to_owned(), RawReceiver::take_handle(&mut *env, name)))
            .collect();
        let outbound_channels = interface
            .outbound_channels()
            .map(|(name, _)| (name.to_owned(), RawSender::take_handle(&mut *env, name)))
            .collect();
        Self {
            data_inputs,
            inbound_channels,
            outbound_channels,
        }
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

impl<Env> ops::Index<Inbound<'_>> for UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    type Output = <RawReceiver as TakeHandle<Env>>::Handle;

    fn index(&self, index: Inbound<'_>) -> &Self::Output {
        self.inbound_channels
            .get(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<Env> ops::IndexMut<Inbound<'_>> for UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    fn index_mut(&mut self, index: Inbound<'_>) -> &mut Self::Output {
        self.inbound_channels
            .get_mut(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<Env> ops::Index<Outbound<'_>> for UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    type Output = <RawSender as TakeHandle<Env>>::Handle;

    fn index(&self, index: Outbound<'_>) -> &Self::Output {
        self.outbound_channels
            .get(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}

impl<Env> ops::IndexMut<Outbound<'_>> for UntypedHandle<Env>
where
    RawData: TakeHandle<Env, Id = str>,
    RawReceiver: TakeHandle<Env, Id = str>,
    RawSender: TakeHandle<Env, Id = str>,
{
    fn index_mut(&mut self, index: Outbound<'_>) -> &mut Self::Output {
        self.outbound_channels
            .get_mut(index.0)
            .unwrap_or_else(|| panic!("{} is not defined", index))
    }
}
