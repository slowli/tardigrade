#![recursion_limit = "128"]

extern crate proc_macro;

use proc_macro::TokenStream;

mod init;
mod interface;
mod shared;
mod take_handle;

#[proc_macro_derive(TakeHandle, attributes(tardigrade))]
pub fn take_handle(input: TokenStream) -> TokenStream {
    take_handle::impl_take_handle(input)
}

#[proc_macro_derive(Initialize, attributes(tardigrade))]
pub fn initialize(input: TokenStream) -> TokenStream {
    init::impl_initialize(input)
}

#[proc_macro_derive(ValidateInterface, attributes(tardigrade))]
pub fn validate_interface(input: TokenStream) -> TokenStream {
    interface::impl_validate_interface(input)
}

#[proc_macro_derive(GetInterface, attributes(tardigrade))]
pub fn get_interface(input: TokenStream) -> TokenStream {
    interface::impl_get_interface(input)
}
