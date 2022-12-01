//! Types / logic shared across multiple derive macros.

use darling::{ast::Style, FromMeta};
use proc_macro2::{Ident, Span};
use quote::{quote, ToTokens};
use syn::{
    parse_quote, spanned::Spanned, Attribute, Data, DataStruct, DeriveInput, Generics, NestedMeta,
    Path, Type, TypePath,
};

use std::collections::{HashMap, HashSet};

pub(crate) fn find_meta_attrs(name: &str, args: &[Attribute]) -> Option<NestedMeta> {
    args.iter()
        .filter_map(|attr| attr.parse_meta().ok())
        .find(|meta| meta.path().is_ident(name))
        .map(NestedMeta::from)
}

#[derive(Debug, Default, FromMeta)]
pub(crate) struct DeriveAttrs {
    #[darling(default)]
    pub interface: Option<String>,
    #[darling(default)]
    pub handle: Option<Path>,
    #[darling(default)]
    pub auto_interface: Option<()>,
    #[darling(rename = "crate", default)]
    pub crate_path: Option<Path>,
    #[darling(default)]
    #[allow(clippy::zero_sized_map_values)] // required for `derive(FromMeta)`
    pub derive: HashMap<String, ()>,
}

/// Field of a struct for which one of traits needs to be derived.
#[derive(Debug)]
pub(crate) struct TargetField {
    pub span: Span,
    pub ident: Option<Ident>,
    /// Name of the field considering a potential `#[_(rename = "...")]` override.
    pub name: Option<String>,
    pub ty: Path,
    pub flatten: bool,
}

impl TargetField {
    fn new(field: &syn::Field) -> darling::Result<Self> {
        let ident = field.ident.clone();
        let attrs = find_meta_attrs("tardigrade", &field.attrs).map_or_else(
            || Ok(TargetFieldAttrs::default()),
            |meta| TargetFieldAttrs::from_nested_meta(&meta),
        )?;

        let name = attrs
            .rename
            .or_else(|| ident.as_ref().map(ToString::to_string));
        let Type::Path(TypePath { path, .. }) = &field.ty else {
            return Err(darling::Error::custom("unsupported field shape"));
        };

        Ok(Self {
            span: field.span(),
            ident,
            name,
            ty: path.clone(),
            flatten: attrs.flatten,
        })
    }

    pub(crate) fn ident(&self, field_index: usize) -> impl ToTokens {
        if let Some(ident) = &self.ident {
            quote!(#ident)
        } else {
            let field_index = syn::Index::from(field_index);
            quote!(#field_index)
        }
    }

    pub(crate) fn path(&self) -> impl ToTokens {
        if self.flatten {
            quote!(path)
        } else {
            let name = self.name.as_ref().unwrap(); // Safe because of previous validation
            quote!(path.join(#name))
        }
    }

    fn init_by_cloning(&self, field_index: usize) -> impl ToTokens {
        let ty = &self.ty;
        let field = self.ident(field_index);
        quote!(#field: <#ty as core::clone::Clone>::clone(&self.#field))
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

/// Attributes for `TargetField`.
#[derive(Debug, Default, FromMeta)]
pub(crate) struct TargetFieldAttrs {
    #[darling(default)]
    pub rename: Option<String>,
    #[darling(default)]
    pub flatten: bool,
}

#[derive(Debug)]
pub(crate) struct TargetStruct {
    pub ident: Ident,
    pub style: Style,
    pub fields: Vec<TargetField>,
    pub generics: Generics,
}

impl TargetStruct {
    pub fn new(input: &DeriveInput) -> darling::Result<Self> {
        let fields = match &input.data {
            Data::Struct(DataStruct { fields, .. }) => fields,
            _ => {
                return Err(darling::Error::unsupported_shape(
                    "can be only implemented for structs",
                ))
            }
        };
        let style = Style::from(fields);
        let fields = fields
            .iter()
            .map(TargetField::new)
            .collect::<Result<_, _>>()?;

        let this = Self {
            ident: input.ident.clone(),
            style,
            fields,
            generics: input.generics.clone(),
        };

        let mut field_names = HashSet::with_capacity(this.fields.len());
        for field in &this.fields {
            if let Some(name) = &field.name {
                if !field_names.insert(name) {
                    let msg = "Duplicate field name";
                    return Err(darling::Error::custom(msg).with_span(&field.span));
                }
            } else if !field.flatten {
                let msg = "Unnamed fields necessitate #[tardigrade(rename = ...)]";
                let err = darling::Error::custom(msg).with_span(&field.span);
                return Err(err);
            }
        }
        Ok(this)
    }

    fn impl_std_trait(&self, tr: impl ToTokens, methods: impl ToTokens) -> impl ToTokens {
        let handle = &self.ident;
        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();
        let mut where_clause = where_clause.cloned().unwrap_or_else(|| parse_quote!(where));
        for field in &self.fields {
            let ty = &field.ty;
            where_clause.predicates.push(parse_quote!(#ty: #tr));
        }

        quote! {
            impl #impl_generics #tr for #handle #ty_generics #where_clause {
                #methods
            }
        }
    }

    pub(crate) fn impl_clone(&self) -> impl ToTokens {
        let fields = self.fields.iter().enumerate();
        let fields = fields.map(|(idx, field)| field.init_by_cloning(idx));
        let fields = match self.style {
            Style::Struct => quote!({ #(#fields,)* }),
            Style::Tuple | Style::Unit => quote!(( #(#fields,)* )),
        };

        let methods = quote! {
            fn clone(&self) -> Self {
                Self #fields
            }
        };
        self.impl_std_trait(quote!(core::clone::Clone), methods)
    }

    pub(crate) fn impl_debug(&self) -> impl ToTokens {
        let name = self.ident.to_string();
        let fields = self.fields.iter().enumerate();
        let debug_fields = fields.map(|(idx, field)| field.debug_in_handle(idx));
        let debug_start = match self.style {
            Style::Struct => quote!(.debug_struct(#name)),
            Style::Tuple | Style::Unit => quote!(.debug_tuple(#name)),
        };
        let methods = quote! {
            fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                formatter #debug_start #(#debug_fields)* .finish()
            }
        };
        self.impl_std_trait(quote!(core::fmt::Debug), methods)
    }
}
