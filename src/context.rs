use std::future::Future;

use tardigrade_shared::workflow::{GetInterface, Initialize, InputsBuilder, TakeHandle};

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

pub trait SpawnWorkflow:
    GetInterface + TakeHandle<Wasm, ()> + Initialize<InputsBuilder, ()>
{
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
        let handle = <W as TakeHandle<Wasm, ()>>::take_handle(&mut wasm, ());
        W::spawn(handle)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn into_inner(self) -> std::pin::Pin<Box<dyn Future<Output = ()>>> {
        self.0 .0
    }
}
