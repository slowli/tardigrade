//! Deriving `WithHandle` and related traits.

use darling::{ast::Style, FromDeriveInput};
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{quote, ToTokens};
use syn::{parse_quote, DeriveInput, Generics};

use crate::shared::{TargetField, TargetStruct};

impl TargetField {
    fn handle_field_ty(&self) -> impl ToTokens {
        let ty = &self.ty;
        let id_ty = self.id_ty();
        let tr = quote!(tardigrade::workflow::TakeHandle<Env, #id_ty>);
        quote!(<#ty as #tr>::Handle)
    }

    fn handle_field_spec(&self) -> impl ToTokens {
        let ty = self.handle_field_ty();
        if let Some(ident) = &self.ident {
            quote!(pub #ident: #ty)
        } else {
            quote!(pub #ty)
        }
    }

    fn init_by_cloning(&self, field_index: usize) -> impl ToTokens {
        let ty = self.handle_field_ty();
        let field = self.ident(field_index);
        quote!(#field: <#ty as core::clone::Clone>::clone(&self.#field))
    }

    fn init_by_take_handle(&self, field_index: usize) -> impl ToTokens {
        let ty = &self.ty;
        let id = self.id();
        let id_ty = self.id_ty();
        let tr = quote!(tardigrade::workflow::TakeHandle<Env, #id_ty>);
        let field = self.ident(field_index);
        quote!(#field: <#ty as #tr>::take_handle(&mut *env, #id))
    }

    fn debug_in_handle(&self, field_index: usize) -> impl ToTokens {
        if let Some(ident) = &self.ident {
            let name = ident.to_string();
            quote!(.field(#name, &self.#ident))
        } else {
            let field_index = syn::Index::from(field_index);
            quote!(.field(&self.#field_index))
        }
    }
}

#[derive(Debug)]
struct TakeHandle {
    base: TargetStruct,
    handle_ident: Ident,
    handle_generics: Generics,
}

impl TakeHandle {
    fn create_handle_ident(ty_ident: &Ident) -> Ident {
        let name = format!("{}Handle", ty_ident);
        Ident::new(&name, ty_ident.span())
    }

    fn create_handle_generics(ty_generics: &Generics, fields: &[TargetField]) -> Generics {
        let mut generics = ty_generics.clone();
        generics.params.push(parse_quote!(Env));
        let mut where_clause = generics
            .where_clause
            .take()
            .unwrap_or_else(|| parse_quote!(where));

        for field in fields {
            let ty = &field.ty;
            let id_ty = field.id_ty();
            let tr = quote!(tardigrade::workflow::TakeHandle<Env, #id_ty>);
            where_clause.predicates.push(parse_quote!(#ty: #tr));
        }
        generics.where_clause = Some(where_clause);
        generics
    }

    fn define_handle_struct(&self) -> impl ToTokens {
        let vis = &self.base.vis;
        let handle = &self.handle_ident;
        let (_, ty_generics, where_clause) = self.handle_generics.split_for_impl();

        let fields = self.base.fields.iter().map(TargetField::handle_field_spec);
        let fields = match self.base.style {
            Style::Struct => quote!({ #(#fields,)* }),
            Style::Tuple | Style::Unit => quote!(( #(#fields,)* )),
        };

        quote! {
            #vis struct #handle #ty_generics #where_clause #fields
        }
    }

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

    fn impl_take_handle(&self) -> impl ToTokens {
        let name = &self.base.ident;
        let handle = &self.handle_ident;
        let tr = quote!(tardigrade::workflow::TakeHandle);
        let (_, ty_generics, _) = self.base.generics.split_for_impl();
        let (impl_generics, handle_ty_generics, where_clause) =
            self.handle_generics.split_for_impl();

        let handle_fields = self.base.fields.iter().enumerate();
        let handle_fields = handle_fields.map(|(idx, field)| field.init_by_take_handle(idx));
        let handle_fields = match self.base.style {
            Style::Struct => quote!({ #(#handle_fields,)* }),
            Style::Tuple | Style::Unit => quote!(( #(#handle_fields,)* )),
        };

        quote! {
            impl #impl_generics #tr<Env, ()> for #name #ty_generics #where_clause {
                type Handle = #handle #handle_ty_generics;

                fn take_handle(env: &mut Env, _id: ()) -> Self::Handle {
                    #handle #handle_fields
                }
            }
        }
    }
}

impl FromDeriveInput for TakeHandle {
    fn from_derive_input(input: &DeriveInput) -> darling::Result<Self> {
        let base = TargetStruct::from_derive_input(input)?;
        // TODO: support ident override
        let handle_ident = Self::create_handle_ident(&input.ident);

        Ok(Self {
            handle_generics: Self::create_handle_generics(&input.generics, &base.fields),
            base,
            handle_ident,
        })
    }
}

impl ToTokens for TakeHandle {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let handle_struct = self.define_handle_struct();
        let clone_impl_for_handle = self.impl_clone_for_handle();
        let debug_impl_for_handle = self.impl_debug_for_handle();
        let take_handle_impl = self.impl_take_handle();

        tokens.extend(quote! {
            #handle_struct
            #clone_impl_for_handle
            #debug_impl_for_handle
            #take_handle_impl
        });
    }
}

pub(crate) fn impl_take_handle(input: TokenStream) -> TokenStream {
    let input: DeriveInput = syn::parse(input).unwrap();
    let take_handle = match TakeHandle::from_derive_input(&input) {
        Ok(take_handle) => take_handle,
        Err(err) => return err.write_errors().into(),
    };
    let tokens = quote!(#take_handle);
    tokens.into()
}
