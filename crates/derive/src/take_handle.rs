//! Deriving `WithHandle` and related traits.

use darling::ast::Style;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{quote, ToTokens};
use syn::{parse_quote, DeriveInput, Generics, Path, Type};

use crate::shared::{MacroAttrs, TargetField, TargetStruct};

impl TargetField {
    fn check_for_handle(&self, env_ident: &Ident) -> darling::Result<()> {
        const MSG: &str = "fields in handle struct must be wrapped in `Handle<_, _>`";

        let wrapper = self
            .wrapper
            .as_ref()
            .ok_or_else(|| darling::Error::custom(MSG).with_span(&self.span))?;

        if wrapper.ident != "Handle" || wrapper.inner_types.len() != 2 {
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

    /*
    fn init_by_cloning(&self, field_index: usize) -> impl ToTokens {
        let ty = &self.ty;
        let field = self.ident(field_index);
        quote!(#field: <#ty as core::clone::Clone>::clone(&self.#field))
    }
     */

    fn init_by_take_handle(&self, field_index: usize) -> impl ToTokens {
        let unwrapped_ty = &self.wrapper.as_ref().unwrap().inner_types[0];
        let id = self.id();
        let tr = quote!(tardigrade::workflow::TakeHandle<Env>);
        let field = self.ident(field_index);
        quote!(#field: <#unwrapped_ty as #tr>::take_handle(&mut *env, #id))
    }

    /*
    fn debug_in_handle(&self, field_index: usize) -> impl ToTokens {
        if let Some(ident) = &self.ident {
            let name = ident.to_string();
            quote!(.field(#name, &self.#ident))
        } else {
            let field_index = syn::Index::from(field_index);
            quote!(.field(&self.#field_index))
        }
    }
    */
}

#[derive(Debug)]
struct TakeHandle {
    base: TargetStruct,
    env: Ident,
    generics: Generics,
    target: Path,
}

impl TakeHandle {
    fn new(input: &mut DeriveInput, attrs: MacroAttrs) -> darling::Result<Self> {
        let env = Self::env_generic(&input.generics)?;
        let base = TargetStruct::new(input)?;
        for field in &base.fields {
            field.check_for_handle(&env)?;
        }

        let fields = match &mut input.data {
            syn::Data::Struct(syn::DataStruct { fields, .. }) => fields,
            _ => {
                return Err(darling::Error::unsupported_shape(
                    "can be only implemented for structs",
                ))
            }
        };
        for field in fields {
            field.attrs.clear();
        }

        Self::extend_handle_generics(&mut input.generics, &base.fields);
        Ok(Self {
            base,
            env,
            generics: input.generics.clone(),
            target: attrs.target,
        })
    }

    fn env_generic(generics: &Generics) -> darling::Result<Ident> {
        const MSG: &str = "Handle struct must have single type generic";

        if generics.params.len() != 1 {
            return Err(darling::Error::custom(MSG).with_span(generics));
        }
        let generic = generics.params.first().unwrap();
        if let syn::GenericParam::Type(ty_param) = generic {
            Ok(ty_param.ident.clone())
        } else {
            Err(darling::Error::custom(MSG).with_span(generics))
        }
    }

    fn extend_handle_generics(generics: &mut Generics, fields: &[TargetField]) {
        let mut where_clause = generics
            .where_clause
            .take()
            .unwrap_or_else(|| parse_quote!(where));

        for field in fields {
            let wrapper = field.wrapper.as_ref().unwrap();
            let ty = &wrapper.inner_types[0];
            let env = &wrapper.inner_types[1];
            let id_ty = field.id_ty();
            let tr = quote!(tardigrade::workflow::TakeHandle<#env, Id = #id_ty>);
            where_clause.predicates.push(parse_quote!(#ty: #tr));
        }
        generics.where_clause = Some(where_clause);
    }

    /*
    fn impl_std_trait_for_handle(
        &self,
        tr: impl ToTokens,
        methods: impl ToTokens,
    ) -> impl ToTokens {
        let handle = &self.handle_ident;
        let (impl_generics, ty_generics, where_clause) = self.handle_generics.split_for_impl();
        let mut where_clause = where_clause.unwrap().clone();
        for field in &self.base.fields {
            let field_handle = field.handle_field_ty();
            where_clause
                .predicates
                .push(parse_quote!(#field_handle: #tr));
        }

        quote! {
            impl #impl_generics #tr for #handle #ty_generics #where_clause {
                #methods
            }
        }
    }

    fn impl_clone_for_handle(&self) -> impl ToTokens {
        let handle_fields = self.base.fields.iter().enumerate();
        let handle_fields = handle_fields.map(|(idx, field)| field.init_by_cloning(idx));
        let handle_fields = match self.base.style {
            Style::Struct => quote!({ #(#handle_fields,)* }),
            Style::Tuple | Style::Unit => quote!(( #(#handle_fields,)* )),
        };

        let methods = quote! {
            fn clone(&self) -> Self {
                Self #handle_fields
            }
        };
        self.impl_std_trait_for_handle(quote!(core::clone::Clone), methods)
    }

    fn impl_debug_for_handle(&self) -> impl ToTokens {
        let name = self.handle_ident.to_string();
        let handle_fields = self.base.fields.iter().enumerate();
        let debug_fields = handle_fields.map(|(idx, field)| field.debug_in_handle(idx));
        let debug_start = match self.base.style {
            Style::Struct => quote!(.debug_struct(#name)),
            Style::Tuple | Style::Unit => quote!(.debug_tuple(#name)),
        };
        let methods = quote! {
            fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                formatter #debug_start #(#debug_fields)* .finish()
            }
        };
        self.impl_std_trait_for_handle(quote!(core::fmt::Debug), methods)
    }
    */

    fn impl_take_handle(&self) -> impl ToTokens {
        let handle = &self.base.ident;
        let target = &self.target;
        let env = &self.env;
        let tr = quote!(tardigrade::workflow::TakeHandle);
        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();

        let handle_fields = self.base.fields.iter().enumerate();
        let handle_fields = handle_fields.map(|(idx, field)| field.init_by_take_handle(idx));
        let handle_fields = match self.base.style {
            Style::Struct => quote!({ #(#handle_fields,)* }),
            Style::Tuple | Style::Unit => quote!(( #(#handle_fields,)* )),
        };

        quote! {
            impl #impl_generics #tr<#env> for #target #where_clause {
                type Id = ();
                type Handle = #handle #ty_generics;

                fn take_handle(env: &mut #env, _id: &()) -> Self::Handle {
                    #handle #handle_fields
                }
            }
        }
    }
}

impl ToTokens for TakeHandle {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        //let clone_impl_for_handle = self.impl_clone_for_handle();
        //let debug_impl_for_handle = self.impl_debug_for_handle();
        let take_handle_impl = self.impl_take_handle();

        tokens.extend(quote! {
            #take_handle_impl
        });
    }
}

pub(crate) fn impl_take_handle(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attrs = match MacroAttrs::parse(attr) {
        Ok(attrs) => attrs,
        Err(err) => return err.write_errors().into(),
    };
    let mut input: syn::DeriveInput = match syn::parse(input) {
        Ok(input) => input,
        Err(err) => return err.into_compile_error().into(),
    };

    let init = match TakeHandle::new(&mut input, attrs) {
        Ok(init) => init,
        Err(err) => return err.write_errors().into(),
    };
    let tokens = quote!(#input #init);
    tokens.into()
}
