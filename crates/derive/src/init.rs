use darling::{ast::Style, FromDeriveInput};
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_quote, DeriveInput, Generics, Ident};

use crate::shared::{TargetField, TargetStruct};
use syn::spanned::Spanned;

#[derive(Debug)]
struct Initialize {
    base: TargetStruct,
    init_ident: Ident,
    init_generics: Generics,
}

impl TargetField {
    fn init_field_ty(&self) -> impl ToTokens {
        let ty = &self.ty;
        let id_ty = self.id_ty();
        let tr = quote!(tardigrade::workflow::Initialize<Env, #id_ty>);
        quote!(<#ty as #tr>::Init)
    }

    fn init_field_spec(&self) -> Option<impl ToTokens> {
        if self.init {
            let ty = self.init_field_ty();
            Some(if let Some(ident) = &self.ident {
                quote!(pub #ident: #ty)
            } else {
                quote!(pub #ty)
            })
        } else {
            None
        }
    }

    fn init_from_field(&self, field_index: usize) -> impl ToTokens {
        debug_assert!(self.init);

        let name = self.ident(field_index);
        let id = self.id();
        let ty = &self.ty;
        let id_ty = self.id_ty();
        let tr = quote!(tardigrade::workflow::Initialize<Env, #id_ty>);

        quote! {
            <#ty as #tr>::initialize(&mut *env, #id, init.#name);
        }
    }
}

impl Initialize {
    fn create_init_ident(ident: &Ident) -> Ident {
        let name = format!("{}Init", ident);
        Ident::new(&name, ident.span())
    }

    fn create_init_generics(ty_generics: &Generics, fields: &[TargetField]) -> Generics {
        let mut generics = ty_generics.clone();
        generics.params.push(parse_quote!(Env));
        let mut where_clause = generics
            .where_clause
            .take()
            .unwrap_or_else(|| parse_quote!(where));

        let init_fields = fields.iter().filter(|&field| field.init);
        for field in init_fields {
            let ty = &field.ty;
            let id_ty = field.id_ty();
            let tr = quote!(tardigrade::workflow::Initialize<Env, #id_ty>);
            where_clause.predicates.push(parse_quote!(#ty: #tr));
        }
        generics.where_clause = Some(where_clause);
        generics
    }

    fn define_init_struct(&self) -> impl ToTokens {
        let vis = &self.base.vis;
        let init = &self.init_ident;
        let (_, ty_generics, where_clause) = self.init_generics.split_for_impl();

        let fields = self
            .base
            .fields
            .iter()
            .filter_map(TargetField::init_field_spec);
        let fields = match self.base.style {
            Style::Struct => quote!({ #(#fields,)* }),
            Style::Tuple | Style::Unit => quote!(( #(#fields,)* )),
        };

        quote! {
            #vis struct #init #ty_generics #where_clause #fields
        }
    }

    fn impl_initialize(&self) -> impl ToTokens {
        let name = &self.base.ident;
        let init = &self.init_ident;
        let tr = quote!(tardigrade::workflow::Initialize);
        let (_, ty_generics, _) = self.base.generics.split_for_impl();
        let (impl_generics, init_ty_generics, where_clause) = self.init_generics.split_for_impl();

        let init_fields = self
            .base
            .fields
            .iter()
            .filter(|&field| field.init)
            .enumerate();
        let init_fields = init_fields.map(|(idx, field)| field.init_from_field(idx));

        quote! {
            impl #impl_generics #tr<Env, ()> for #name #ty_generics #where_clause {
                type Init = #init #init_ty_generics;

                fn initialize(env: &mut Env, _id: (), init: Self::Init) {
                    #(#init_fields)*
                }
            }
        }
    }
}

impl FromDeriveInput for Initialize {
    fn from_derive_input(input: &DeriveInput) -> darling::Result<Self> {
        let base = TargetStruct::from_derive_input(input)?;
        let init_ident = Self::create_init_ident(&input.ident);

        if base.fields.iter().all(|field| !field.init) {
            let message = "no initialized fields found; mark them with `#[tardigrade(init)]`";
            return Err(darling::Error::custom(message).with_span(&input.span()));
        }

        Ok(Self {
            init_generics: Self::create_init_generics(&input.generics, &base.fields),
            base,
            init_ident,
        })
    }
}

impl ToTokens for Initialize {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let init_struct = self.define_init_struct();
        let initialize_impl = self.impl_initialize();
        tokens.extend(quote! {
            #init_struct
            #initialize_impl
        });
    }
}

pub(crate) fn impl_initialize(input: TokenStream) -> TokenStream {
    let input: DeriveInput = syn::parse(input).unwrap();
    let init = match Initialize::from_derive_input(&input) {
        Ok(init) => init,
        Err(err) => return err.write_errors().into(),
    };
    let tokens = quote!(#init);
    tokens.into()
}
