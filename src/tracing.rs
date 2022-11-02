//! Tracing logic.

#[cfg(target_arch = "wasm32")]
mod imp {
    use tracing_tunnel::{TracingEvent, TracingEventSender};

    pub(crate) fn new_subscriber() -> TracingEventSender {
        TracingEventSender::new(|event| on_tracing_event(&event))
    }

    fn on_tracing_event(event: &TracingEvent) {
        #[link(wasm_import_module = "tracing")]
        extern "C" {
            #[link_name = "send_trace"]
            fn send_trace(trace_ptr: *const u8, trace_len: usize);
        }

        let event_bytes = serde_json::to_vec(event).expect("cannot serialize `TracingEvent`");
        unsafe {
            send_trace(event_bytes.as_ptr(), event_bytes.len());
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) use self::imp::new_subscriber;
