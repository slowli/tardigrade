//! WASM-specific utils.

use externref::{externref, Resource};
use slab::Slab;

use std::task::{Context, Poll, Waker};

use crate::{
    abi::ResourceKind,
    channel::{RawReceiver, RawSender},
    handle::{AccessError, AccessErrorKind, Handle, HandlePath, ReceiverAt, SenderAt},
    workflow::{TakeHandles, UntypedHandles, Wasm},
};

/// Container similar to `Slab`, but with guarantee that returned item keys are never reused.
#[derive(Debug)]
pub(crate) struct Registry<T> {
    items: Slab<T>,
    item_counter: u32,
}

impl<T> Registry<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            items: Slab::with_capacity(capacity),
            item_counter: 0,
        }
    }

    pub fn insert(&mut self, value: T) -> u64 {
        let slab_key = self.items.insert(value);
        let item_counter = self.item_counter;
        self.item_counter += 1;
        (u64::from(item_counter) << 32) + u64::try_from(slab_key).unwrap()
    }

    pub fn get_mut(&mut self, key: u64) -> &mut T {
        let slab_key = (key & 0x_ffff_ffff) as usize;
        self.items
            .get_mut(slab_key)
            .expect("registry item not found")
    }

    pub fn remove(&mut self, key: u64) -> T {
        let slab_key = (key & 0x_ffff_ffff) as usize;
        self.items.remove(slab_key)
    }
}

#[derive(Debug)]
pub(crate) struct StubState<T> {
    value: Option<T>,
    waker: Option<Waker>,
}

impl<T> Default for StubState<T> {
    fn default() -> Self {
        Self {
            value: None,
            waker: None,
        }
    }
}

impl<T> StubState<T> {
    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<T> {
        if let Some(value) = self.value.take() {
            Poll::Ready(value)
        } else {
            self.insert_waker(cx);
            Poll::Pending
        }
    }

    fn insert_waker(&mut self, cx: &mut Context<'_>) {
        let waker_needs_replacement = self
            .waker
            .as_ref()
            .map_or(true, |waker| !waker.will_wake(cx.waker()));
        if waker_needs_replacement {
            self.waker = Some(cx.waker().clone());
        }
    }

    pub fn set_value(&mut self, value: T) {
        debug_assert!(self.value.is_none());
        self.value = Some(value);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// Collection of handles owned by the host.
#[derive(Debug)]
pub struct HostHandles {
    resource: Resource<Self>,
}

impl HostHandles {
    #[doc(hidden)]
    pub fn from_resource(resource: Resource<Self>) -> Self {
        Self { resource }
    }

    pub(crate) fn into_resource(self) -> Resource<Self> {
        self.resource
    }
}

impl From<UntypedHandles<Wasm>> for HostHandles {
    fn from(handles: UntypedHandles<Wasm>) -> Self {
        let resource = unsafe { create_handles() };
        for (path, handle) in handles {
            let path = path.to_string();
            match handle {
                Handle::Sender(sender) => unsafe {
                    insert_sender(
                        &resource,
                        path.as_ptr(),
                        path.len(),
                        sender.as_resource().map(Resource::upcast_ref),
                    );
                },
                Handle::Receiver(receiver) => unsafe {
                    insert_receiver(
                        &resource,
                        path.as_ptr(),
                        path.len(),
                        receiver.into_resource().map(Resource::upcast),
                    );
                },
            }
        }
        Self { resource }
    }
}

impl TakeHandles<Wasm> for HostHandles {
    fn take_receiver(&mut self, path: HandlePath<'_>) -> Result<RawReceiver, AccessError> {
        let path_str = path.to_cow_string();
        let mut kind = ResourceKind::None;
        let handle =
            unsafe { remove_handle(&self.resource, path_str.as_ptr(), path_str.len(), &mut kind) };

        match kind {
            ResourceKind::None => Err(AccessErrorKind::Missing.with_location(ReceiverAt(path))),
            ResourceKind::Receiver => {
                let handle = handle.map(|generic| unsafe { generic.downcast_unchecked() });
                Ok(RawReceiver::from_resource(handle))
            }
            _ => Err(AccessErrorKind::KindMismatch.with_location(ReceiverAt(path))),
        }
    }

    fn take_sender(&mut self, path: HandlePath<'_>) -> Result<RawSender, AccessError> {
        let path_str = path.to_cow_string();
        let mut kind = ResourceKind::None;
        let handle =
            unsafe { remove_handle(&self.resource, path_str.as_ptr(), path_str.len(), &mut kind) };

        match kind {
            ResourceKind::None => Err(AccessErrorKind::Missing.with_location(SenderAt(path))),
            ResourceKind::Sender => {
                let handle = handle.map(|generic| unsafe { generic.downcast_unchecked() });
                Ok(RawSender::from_resource(handle))
            }
            _ => Err(AccessErrorKind::KindMismatch.with_location(SenderAt(path))),
        }
    }

    fn drain(&mut self) -> UntypedHandles<Wasm> {
        // TODO: implement this. Not critical since in the only place where `HostHandles` are used
        //   so far (`TaskHandle::from_workflow()`), this method shouldn't be called.
        UntypedHandles::<Wasm>::new()
    }
}

#[externref]
#[link(wasm_import_module = "tardigrade_rt")]
extern "C" {
    #[link_name = "handles::create"]
    fn create_handles() -> Resource<HostHandles>;

    #[link_name = "handles::insert_sender"]
    fn insert_sender(
        handles: &Resource<HostHandles>,
        name_ptr: *const u8,
        name_len: usize,
        sender: Option<&Resource<()>>,
    );

    #[link_name = "handles::insert_receiver"]
    fn insert_receiver(
        handles: &Resource<HostHandles>,
        name_ptr: *const u8,
        name_len: usize,
        sender: Option<Resource<()>>,
    );

    #[link_name = "handles::remove"]
    fn remove_handle(
        handles: &Resource<HostHandles>,
        name_ptr: *const u8,
        name_len: usize,
        kind: *mut ResourceKind,
    ) -> Option<Resource<()>>;
}
