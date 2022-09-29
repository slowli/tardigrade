use darling::{FromDeriveInput, FromMeta};
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{spanned::Spanned, DeriveInput};

use std::{borrow::Cow, env, fs, path::Path};

use crate::utils::{find_meta_attrs, DeriveAttrs};
use tardigrade_shared::interface::Interface;

#[derive(Debug)]
struct GetInterface {
    input: DeriveInput,
    compressed_spec: Vec<u8>,
}

impl GetInterface {
    fn get_spec_contents(spec: &str) -> darling::Result<Cow<'_, [u8]>> {
        if let Some(spec_path) = spec.strip_prefix("file:") {
            let manifest_path = env::var_os("CARGO_MANIFEST_DIR").ok_or_else(|| {
                let message = "`CARGO_MANIFEST_DIR` env var not set; please use cargo";
                darling::Error::custom(message)
            })?;
            let full_spec_path = Path::new(&manifest_path).join(spec_path);

            let contents = fs::read(&full_spec_path).map_err(|err| {
                let message = format!("cannot read workflow spec at `{}`: {}", spec_path, err);
                darling::Error::custom(message)
            })?;
            Ok(Cow::Owned(contents))
        } else {
            Ok(Cow::Borrowed(spec.as_bytes()))
        }
    }

    fn data_section(&self) -> impl ToTokens {
        let name = &self.input.ident;
        let wasm = quote!(tardigrade::workflow::Wasm);
        let args = quote! {
            <#name as tardigrade::workflow::GetInterface>::WORKFLOW_NAME,
            INTERFACE_SPEC,
        };
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
        let (impl_generics, ty_generics, where_clause) = self.input.generics.split_for_impl();

        quote! {
            impl #impl_generics #tr for #name #ty_generics #where_clause {
                const WORKFLOW_NAME: &'static str = core::stringify!(#name);

                fn interface() -> tardigrade::interface::Interface<Self> {
                    tardigrade::interface::Interface::from_bytes(&INTERFACE_SPEC)
                        .downcast::<Self>()
                        .expect("workflow interface does not match declaration")
                }
            }
        }
    }
}

impl FromDeriveInput for GetInterface {
    fn from_derive_input(input: &DeriveInput) -> darling::Result<Self> {
        let attrs = find_meta_attrs("tardigrade", &input.attrs).map_or_else(
            || Ok(DeriveAttrs::default()),
            |meta| DeriveAttrs::from_nested_meta(&meta),
        )?;
        let spec = attrs
            .interface
            .as_deref()
            .unwrap_or("file:src/tardigrade.json");
        let contents = Self::get_spec_contents(spec).map_err(|err| err.with_span(&input.ident))?;

        let interface: Interface<()> = serde_json::from_slice(&contents).map_err(|err| {
            let message = format!("error deserializing workflow spec: {}", err);
            darling::Error::custom(message).with_span(&input.ident)
        })?;
        let compressed_spec = serde_json::to_vec(&interface).map_err(|err| {
            let message = format!("error serializing workflow spec: {}", err);
            darling::Error::custom(message).with_span(&input.ident)
        })?;

        Ok(Self {
            input: input.clone(),
            compressed_spec,
        })
    }
}

impl ToTokens for GetInterface {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let data_section = self.data_section();
        let get_interface_impl = self.impl_get_interface();
        let spec = syn::LitByteStr::new(&self.compressed_spec, self.input.span());
        tokens.extend(quote! {
            const _: () = {
                const INTERFACE_SPEC: &[u8] = #spec;
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
