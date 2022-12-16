use darling::{FromDeriveInput, FromMeta};
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{spanned::Spanned, DeriveInput, Generics};

use std::{env, fs, path::Path};

use crate::utils::{find_meta_attrs, DeriveAttrs};
use tardigrade_shared::interface::Interface;

#[derive(Debug)]
struct GetInterface {
    input: DeriveInput,
    compressed_spec: Option<Vec<u8>>,
}

impl GetInterface {
    fn check_generics(generics: &Generics) -> darling::Result<()> {
        if generics.lifetimes().count() > 0 {
            let message = "lifetime generics are not supported";
            return Err(darling::Error::custom(message).with_span(generics));
        }
        if generics.const_params().count() > 0 {
            let message = "const params are not supported";
            return Err(darling::Error::custom(message).with_span(generics));
        }
        for type_param in generics.type_params() {
            if type_param.default.is_none() {
                let message = "type param must have default value";
                return Err(darling::Error::custom(message).with_span(type_param));
            }
        }
        Ok(())
    }

    fn get_spec_contents(spec_path: &str) -> darling::Result<Vec<u8>> {
        let manifest_path = env::var_os("CARGO_MANIFEST_DIR").ok_or_else(|| {
            let message = "`CARGO_MANIFEST_DIR` env var not set; please use cargo";
            darling::Error::custom(message)
        })?;
        let full_spec_path = Path::new(&manifest_path).join(spec_path);

        fs::read(full_spec_path).map_err(|err| {
            let message = format!("cannot read workflow spec at `{spec_path}`: {err}");
            darling::Error::custom(message)
        })
    }

    fn data_section(&self) -> impl ToTokens {
        let name = &self.input.ident;
        let wasm = quote!(tardigrade::workflow::Wasm);
        let args = quote!(core::stringify!(#name), INTERFACE_SPEC);
        quote! {
            #[cfg_attr(target_arch = "wasm32", link_section = "__tardigrade_spec")]
            #[doc(hidden)]
            pub static __TARDIGRADE_SPEC: [u8; #wasm::custom_section_len(#args)] =
                #wasm::custom_section(#args);
        }
    }

    fn impl_get_interface(&self) -> impl ToTokens {
        let name = &self.input.ident;
        let tr = quote!(tardigrade::workflow::GetInterface);
        let interface = quote!(tardigrade::interface::Interface);

        let init_closure = if self.compressed_spec.is_some() {
            quote! {
                || {
                    let interface = #interface::from_bytes(&INTERFACE_SPEC);
                    tardigrade::workflow::interface_by_handle::<#name>()
                        .check_compatibility(&interface)
                        .expect("workflow interface does not match declaration");
                    interface
                }
            }
        } else {
            quote!(tardigrade::workflow::interface_by_handle::<#name>)
        };

        quote! {
            impl #tr for #name {
                fn interface() -> std::borrow::Cow<'static, #interface> {
                    static INTERFACE: tardigrade::_reexports::Lazy<#interface>
                        = tardigrade::_reexports::Lazy::new(#init_closure);
                    std::borrow::Cow::Borrowed(&INTERFACE)
                }
            }
        }
    }
}

impl FromDeriveInput for GetInterface {
    fn from_derive_input(input: &DeriveInput) -> darling::Result<Self> {
        Self::check_generics(&input.generics)?;

        let attrs = find_meta_attrs("tardigrade", &input.attrs).map_or_else(
            || Ok(DeriveAttrs::default()),
            |meta| DeriveAttrs::from_nested_meta(&meta),
        )?;

        let compressed_spec = if attrs.auto_interface.is_some() {
            None
        } else {
            let spec = attrs.interface.as_deref().unwrap_or("src/tardigrade.json");
            let contents =
                Self::get_spec_contents(spec).map_err(|err| err.with_span(&input.ident))?;

            let interface: Interface = serde_json::from_slice(&contents).map_err(|err| {
                let message = format!("error deserializing workflow spec: {err}");
                darling::Error::custom(message).with_span(&input.ident)
            })?;
            let spec = serde_json::to_vec(&interface).map_err(|err| {
                let message = format!("error serializing workflow spec: {err}");
                darling::Error::custom(message).with_span(&input.ident)
            })?;
            Some(spec)
        };

        Ok(Self {
            input: input.clone(),
            compressed_spec,
        })
    }
}

impl ToTokens for GetInterface {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let data_section = self.compressed_spec.as_ref().map(|spec| {
            let spec = syn::LitByteStr::new(spec, self.input.span());
            let data_section = self.data_section();
            quote! {
                const INTERFACE_SPEC: &[u8] = #spec;
                #data_section
            }
        });
        let get_interface_impl = self.impl_get_interface();

        tokens.extend(quote! {
            const _: () = {
                #data_section
                #get_interface_impl
            };
        });
    }
}

pub(crate) fn impl_get_interface(input: TokenStream) -> TokenStream {
    let input: DeriveInput = syn::parse(input).unwrap();
    let interface = match GetInterface::from_derive_input(&input) {
        Ok(interface) => interface,
        Err(err) => return err.write_errors().into(),
    };
    let tokens = quote!(#interface);
    tokens.into()
}
