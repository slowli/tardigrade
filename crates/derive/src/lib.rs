//! Proc macros for Tardigrade workflows.
//!
//! See the [`tardigrade`] crate docs for docs on the macros from this crate and examples
//! of their usage.
//!
//! [`tardigrade`]: https://docs.rs/tardigrade/

#![recursion_limit = "128"]
// Documentation settings.
#![doc(html_root_url = "https://docs.rs/tardigrade-derive/0.1.0")]
// Linter settings.
#![warn(missing_debug_implementations, bare_trait_objects)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::must_use_candidate, clippy::module_name_repetitions)]

extern crate proc_macro;

use proc_macro::TokenStream;

mod interface;
mod utils;
mod with_handle;
mod workflow_entry;

#[proc_macro_derive(GetInterface, attributes(tardigrade))]
pub fn get_interface(input: TokenStream) -> TokenStream {
    interface::impl_get_interface(input)
}

#[proc_macro_derive(WithHandle, attributes(tardigrade))]
pub fn with_handle(input: TokenStream) -> TokenStream {
    with_handle::impl_with_handle(input)
}

#[proc_macro_derive(WorkflowEntry, attributes(tardigrade))]
pub fn workflow_entry(input: TokenStream) -> TokenStream {
    workflow_entry::impl_workflow_entry(input)
}
