use darling::FromDeriveInput;
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{spanned::Spanned, DeriveInput};

use std::{env, fs, path::Path};

use crate::shared::{TargetField, TargetStruct};
use tardigrade_shared::workflow::Interface;

impl TargetField {
    fn validate_interface(&self) -> impl ToTokens {
        let ty = &self.ty;
        let id_ty = self.id_ty();
        let id = self.id();
        let tr = quote!(tardigrade::workflow::ValidateInterface<#id_ty>);
        quote! {
            <#ty as #tr>::validate_interface(&mut *errors, interface, #id);
        }
    }
}

#[derive(Debug)]
struct ValidateInterface {
    base: TargetStruct,
}

impl ValidateInterface {
    fn validation_method(&self) -> impl ToTokens {
        let fields = self.base.fields.iter();
        let validations = fields.map(TargetField::validate_interface);

        quote! {
            fn validate_interface(
                errors: &mut tardigrade::workflow::InterfaceErrors,
                interface: &tardigrade::workflow::Interface<()>,
                _id: (),
            ) {
                #(#validations)*
            }
        }
    }
}

impl FromDeriveInput for ValidateInterface {
    fn from_derive_input(input: &DeriveInput) -> darling::Result<Self> {
        Ok(Self {
            base: TargetStruct::from_derive_input(input)?,
        })
    }
}

impl ToTokens for ValidateInterface {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let name = &self.base.ident;
        let (impl_generics, ty_generics, where_clause) = self.base.generics.split_for_impl();
        let tr = quote!(tardigrade::workflow::ValidateInterface<()>);
        let validation_method = self.validation_method();

        tokens.extend(quote! {
            impl #impl_generics #tr for #name #ty_generics #where_clause {
                #validation_method
            }
        })
    }
}

pub(crate) fn impl_validate_interface(input: TokenStream) -> TokenStream {
    let input: DeriveInput = syn::parse(input).unwrap();
    let interface = match ValidateInterface::from_derive_input(&input) {
        Ok(interface) => interface,
        Err(err) => return err.write_errors().into(),
    };
    let tokens = quote!(#interface);
    tokens.into()
}

#[derive(Debug)]
struct GetInterface {
    input: DeriveInput,
    compressed_spec: Vec<u8>,
}

impl GetInterface {
    fn data_section(&self) -> impl ToTokens {
        let len = self.compressed_spec.len();
        let spec = syn::LitByteStr::new(&self.compressed_spec, self.input.span());

        quote! {
            #[cfg_attr(target_arch = "wasm32", link_section = "__tardigrade_spec")]
            #[doc(hidden)]
            pub static __TARDIGRADE_SPEC: [u8; #len] = *#spec;
        }
    }

    fn impl_get_interface(&self) -> impl ToTokens {
        let name = &self.input.ident;
        let tr = quote!(tardigrade::workflow::GetInterface);
        let (impl_generics, ty_generics, where_clause) = self.input.generics.split_for_impl();

        quote! {
            impl #impl_generics #tr for #name #ty_generics #where_clause {
                fn interface() -> tardigrade::workflow::Interface<Self> {
                    tardigrade::workflow::Interface::from_bytes(&__TARDIGRADE_SPEC)
                        .downcast::<Self>()
                        .expect("workflow interface does not match declaration")
                }
            }
        }
    }
}

impl FromDeriveInput for GetInterface {
    fn from_derive_input(input: &DeriveInput) -> darling::Result<Self> {
        let manifest_path = env::var_os("CARGO_MANIFEST_DIR").ok_or_else(|| {
            let message = "`CARGO_MANIFEST_DIR` env var not set; please use cargo";
            darling::Error::custom(message).with_span(&input.ident)
        })?;
        // FIXME: make relative path configurable
        let spec_path = "src/tardigrade.json";
        let full_spec_path = Path::new(&manifest_path).join(spec_path);

        let contents = fs::read(&full_spec_path).map_err(|err| {
            let message = format!("cannot read workflow spec at `{}`: {}", spec_path, err);
            darling::Error::custom(message).with_span(&input.ident)
        })?;
        let interface: Interface<()> = serde_json::from_slice(&contents).map_err(|err| {
            let message = format!("error deserializing spec at `{}`: {}", spec_path, err);
            darling::Error::custom(message).with_span(&input.ident)
        })?;
        let compressed_spec = serde_json::to_vec(&interface).map_err(|err| {
            let message = format!("error serializing spec: {}", err);
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
        tokens.extend(quote! {
            #data_section
            #get_interface_impl
        })
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
