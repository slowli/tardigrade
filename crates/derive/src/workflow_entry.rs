//! `WorkflowEntry` derive macro.

use darling::FromMeta;
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{DeriveInput, Ident, Path};

use crate::utils::{find_meta_attrs, DeriveAttrs};

#[derive(Debug)]
struct WorkflowEntry {
    name: Ident,
    crate_path: Path,
}

impl WorkflowEntry {
    fn new(input: &DeriveInput) -> Result<Self, syn::Error> {
        let attrs = find_meta_attrs("tardigrade", &input.attrs).map_or_else(
            || Ok(DeriveAttrs::default()),
            |meta| DeriveAttrs::from_nested_meta(&meta),
        )?;

        Ok(Self {
            name: input.ident.clone(),
            crate_path: attrs
                .crate_path
                .unwrap_or_else(|| syn::parse_quote!(tardigrade)),
        })
    }

    fn impl_workflow_entry(&self) -> impl ToTokens {
        let name = &self.name;
        let cr = &self.crate_path;

        quote! {
            impl #cr::workflow::WorkflowEntry for #name {
                const WORKFLOW_NAME: &'static str = core::stringify!(#name);

                fn you_should_use_derive_macro_to_implement_this_trait() { }
            }
        }
    }

    fn define_export(&self) -> impl ToTokens {
        let name = &self.name;
        let export_name = format!("tardigrade_rt::spawn::{name}");
        let cr = &self.crate_path;
        let externref = quote!(#cr::_reexports::externref);
        let externref_str = externref.to_string();

        quote! {
            #[cfg(target_arch = "wasm32")]
            #[doc(hidden)]
            #[#externref::externref(crate = #externref_str)]
            #[export_name = #export_name]
            pub unsafe extern "C" fn __tardigrade_spawn(
                data_ptr: *mut u8,
                data_len: usize,
                handles: #externref::Resource<#cr::workflow::HostHandles>,
            ) -> #cr::workflow::TaskHandle {
                let data = std::vec::Vec::from_raw_parts(data_ptr, data_len, data_len);
                let handles = #cr::workflow::HostHandles::from_resource(handles);
                #cr::workflow::spawn_workflow::<#name>(data, handles)
            }
        }
    }
}

impl ToTokens for WorkflowEntry {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let workflow_entry_impl = self.impl_workflow_entry();
        let export = self.define_export();

        tokens.extend(quote! {
            const _: () = {
                #workflow_entry_impl
                #export
            };
        });
    }
}

pub(crate) fn impl_workflow_entry(input: TokenStream) -> TokenStream {
    let input: DeriveInput = syn::parse(input).unwrap();
    let entry = match WorkflowEntry::new(&input) {
        Ok(entry) => entry,
        Err(err) => return err.into_compile_error().into(),
    };
    let tokens = quote!(#entry);
    tokens.into()
}
