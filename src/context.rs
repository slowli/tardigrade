use std::future::Future;

use tardigrade_shared::workflow::{InputsBuilder, ProvideInterface, PutHandle, TakeHandle};

#[cfg(all(target_arch = "wasm32", not(target_os = "emscripten")))]
mod imp {
    use crate::task::imp::TaskPointer;

    pub(super) type TaskHandle = TaskPointer;
}

#[cfg(not(all(target_arch = "wasm32", not(target_os = "emscripten"))))]
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
    TakeHandle<Wasm, (), Handle = Self::Wasm> + PutHandle<InputsBuilder, ()> + ProvideInterface
{
    type Wasm: Into<TaskHandle>;
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

    #[cfg(not(all(target_arch = "wasm32", not(target_os = "emscripten"))))]
    pub(crate) fn into_inner(self) -> std::pin::Pin<Box<dyn Future<Output = ()>>> {
        self.0 .0
    }
}
