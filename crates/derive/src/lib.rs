#![recursion_limit = "128"]

extern crate proc_macro;

use proc_macro::TokenStream;

mod init;
mod interface;
mod shared;
mod take_handle;

#[proc_macro_attribute]
pub fn handle(attr: TokenStream, input: TokenStream) -> TokenStream {
    take_handle::impl_take_handle(attr, input)
}

#[proc_macro_attribute]
pub fn init(attr: TokenStream, input: TokenStream) -> TokenStream {
    init::impl_initialize(attr, input)
}

#[proc_macro_derive(GetInterface, attributes(tardigrade))]
pub fn get_interface(input: TokenStream) -> TokenStream {
    interface::impl_get_interface(input)
}
