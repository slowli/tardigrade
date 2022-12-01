//! Deriving `TakeHandle`.

use darling::{ast::Style, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{quote, ToTokens};
use syn::{parse_quote, DeriveInput, Generics, Path, Type};

use crate::utils::{find_meta_attrs, DeriveAttrs, TargetField, TargetStruct};

impl TargetField {
    fn check_for_handle(&self, env_ident: &Ident) -> darling::Result<()> {
        const MSG: &str = "fields in handle struct must be wrapped in `InEnv<_, _>`";

        let wrapper = self
            .wrapper
            .as_ref()
            .ok_or_else(|| darling::Error::custom(MSG).with_span(&self.span))?;

        if wrapper.ident != "InEnv" || wrapper.inner_types.len() != 2 {
            return Err(darling::Error::custom(MSG).with_span(&self.span));
        }

        let is_valid = if let Type::Path(syn::TypePath { path, .. }) = &wrapper.inner_types[1] {
            path.get_ident().map_or(false, |id| id == env_ident)
        } else {
            false
        };
        if is_valid {
            Ok(())
        } else {
            Err(darling::Error::custom(MSG).with_span(&self.span))
        }
    }

    fn init_by_take_handle(&self, field_index: usize) -> impl ToTokens {
        let unwrapped_ty = &self.wrapper.as_ref().unwrap().inner_types[0];
        let path = self.path();
        let tr = quote!(tardigrade::workflow::TakeHandle<Env>);
        let field = self.ident(field_index);
        quote!(#field: <#unwrapped_ty as #tr>::take_handle(&mut *env, #path)?)
    }
}

#[derive(Debug)]
struct Handle {
    base: TargetStruct,
    env: Ident,
    derive_clone: bool,
    derive_debug: bool,
}

impl Handle {
    fn new(input: &DeriveInput) -> darling::Result<Self> {
        let env = Self::env_generic(&input.generics)?;
        let mut base = TargetStruct::new(input)?;
        for field in &base.fields {
            field.check_for_handle(&env)?;
        }
        base.generics = input.generics.clone();

        Ok(Self {
            base,
            env,
            derive_clone: false, // FIXME
            derive_debug: false,
        })
    }

    fn env_generic(generics: &Generics) -> darling::Result<Ident> {
        const MSG: &str = "Handle struct must have an env generic";

        if generics.params.is_empty() {
            return Err(darling::Error::custom(MSG).with_span(generics));
        }
        let generic = generics.params.last().unwrap();
        if let syn::GenericParam::Type(ty_param) = generic {
            Ok(ty_param.ident.clone())
        } else {
            Err(darling::Error::custom(MSG).with_span(generics))
        }
    }

    fn impl_take_handle(&self) -> impl ToTokens {
        let handle = &self.base.ident;
        let env = &self.env;
        let env_tr = quote!(tardigrade::workflow::WorkflowEnv);
        let wasm = quote!(tardigrade::workflow::Wasm);
        let (.., where_clause) = self.base.generics.split_for_impl();
        let tr = quote!(tardigrade::workflow::WithHandle);
        let with_handle_impl = quote! {
            impl #tr for #handle <#wasm> #where_clause {
                type Handle<#env: #env_tr> = #handle <#env>;
            }
        };

        let handle_fields = self.base.fields.iter().enumerate();
        let handle_fields = handle_fields.map(|(idx, field)| field.init_by_take_handle(idx));
        let handle_fields = match self.base.style {
            Style::Struct => quote!({ #(#handle_fields,)* }),
            Style::Tuple | Style::Unit => quote!(( #(#handle_fields,)* )),
        };
        let tr = quote!(tardigrade::workflow::TakeHandle);
        quote! {
            #with_handle_impl

            impl<#env: #env_tr> #tr <#env> for #handle <#wasm> #where_clause {
                fn take_handle(
                    env: &mut #env,
                    path: tardigrade::interface::HandlePath<'_>,
                ) -> core::result::Result<Self::Handle<#env>, tardigrade::interface::AccessError> {
                    core::result::Result::Ok(#handle #handle_fields)
                }
            }
        }
    }
}

impl ToTokens for Handle {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let clone_impl = if self.derive_clone {
            Some(self.base.impl_clone())
        } else {
            None
        };
        let debug_impl = if self.derive_debug {
            Some(self.base.impl_debug())
        } else {
            None
        };
        let take_handle_impl = self.impl_take_handle();

        tokens.extend(quote! {
            #clone_impl
            #debug_impl
            #take_handle_impl
        });
    }
}

fn derive_take_handle(input: &DeriveInput) -> darling::Result<impl ToTokens> {
    // Determine whether we deal with a handle or a delegated struct.
    let attrs = find_meta_attrs("tardigrade", &input.attrs).map_or_else(
        || Ok(DeriveAttrs::default()),
        |meta| DeriveAttrs::from_nested_meta(&meta),
    )?;

    if let Some(handle) = &attrs.handle {
        Ok(impl_take_handle_delegation(input, handle))
    } else {
        let handle = Handle::new(input)?;
        Ok(quote!(#handle))
    }
}

// TODO: support generic handles
fn impl_take_handle_delegation(input: &DeriveInput, handle: &Path) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let target = &input.ident;
    let tr = quote!(tardigrade::workflow::WithHandle);
    let env_tr = quote!(tardigrade::workflow::WorkflowEnv);
    let with_handle_impl = quote! {
        impl #impl_generics #tr for #target #ty_generics #where_clause {
            type Handle<Env: #env_tr> = #handle <Env>;
        }
    };

    let tr = quote!(tardigrade::workflow::TakeHandle);
    let wasm = quote!(tardigrade::workflow::Wasm);
    let mut extended_generics = input.generics.clone();
    extended_generics
        .params
        .push(parse_quote!(Env: tardigrade::workflow::WorkflowEnv));
    let (impl_generics, _, where_clause) = extended_generics.split_for_impl();

    let take_handle_impl = quote! {
        impl #impl_generics #tr<Env> for #target #ty_generics #where_clause {
            fn take_handle(
                env: &mut Env,
                path: tardigrade::interface::HandlePath<'_>,
            ) -> core::result::Result<Self::Handle<Env>, tardigrade::interface::AccessError> {
                <#handle <#wasm> as #tr<Env>>::take_handle(env, path)
            }
        }
    };

    quote!(#with_handle_impl #take_handle_impl)
}

pub(crate) fn impl_take_handle(input: TokenStream) -> TokenStream {
    let input: DeriveInput = match syn::parse(input) {
        Ok(input) => input,
        Err(err) => return err.into_compile_error().into(),
    };
    let tokens = match derive_take_handle(&input) {
        Ok(tokens) => tokens,
        Err(err) => return err.write_errors().into(),
    };
    quote!(#tokens).into()
}
