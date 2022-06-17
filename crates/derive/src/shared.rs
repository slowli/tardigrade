//! Types / logic shared across multiple derive macros.

use darling::{
    ast::{Fields, Style},
    FromDeriveInput, FromField, FromMeta,
};
use proc_macro2::{Ident, Span};
use quote::{quote, ToTokens};
use syn::{
    spanned::Spanned, Attribute, Data, DataStruct, DeriveInput, Generics, NestedMeta, Type,
    Visibility,
};

use std::collections::HashSet;

pub(crate) fn find_meta_attrs(name: &str, args: &[Attribute]) -> Option<NestedMeta> {
    args.as_ref()
        .iter()
        .filter_map(|a| a.parse_meta().ok())
        .find(|m| m.path().is_ident(name))
        .map(NestedMeta::from)
}

/// Field of a struct for which one of traits needs to be derived.
#[derive(Debug)]
pub(crate) struct TargetField {
    pub span: Span,
    pub ident: Option<Ident>,
    /// Name of the field considering a potential `#[_(rename = "...")]` override.
    pub name: Option<String>,
    pub ty: Type,
    pub flatten: bool,
    /// Whether to include field into init struct.
    pub init: bool,
}

impl TargetField {
    fn detect_init(field_ty: &Type) -> bool {
        if let Type::Path(path) = field_ty {
            let last_segment = if let Some(segment) = path.path.segments.last() {
                segment
            } else {
                return false;
            };
            last_segment.ident == "Data"
        } else {
            false
        }
    }

    pub(crate) fn ident(&self, field_index: usize) -> impl ToTokens {
        if let Some(ident) = &self.ident {
            quote!(#ident)
        } else {
            let field_index = syn::Index::from(field_index);
            quote!(#field_index)
        }
    }

    pub(crate) fn id_ty(&self) -> impl ToTokens {
        if self.flatten {
            quote!(())
        } else {
            quote!(&'static str)
        }
    }

    pub(crate) fn id(&self) -> impl ToTokens {
        if self.flatten {
            quote!(())
        } else {
            let name = self.name.as_ref().unwrap(); // Safe because of previous validation
            quote!(#name)
        }
    }
}

impl FromField for TargetField {
    fn from_field(field: &syn::Field) -> darling::Result<Self> {
        let ident = field.ident.clone();

        let attrs = find_meta_attrs("tardigrade", &field.attrs)
            .map(|meta| TargetFieldAttrs::from_nested_meta(&meta))
            .unwrap_or_else(|| Ok(TargetFieldAttrs::default()))?;

        let name = attrs
            .rename
            .or_else(|| ident.as_ref().map(ToString::to_string));

        let init = attrs.init.unwrap_or_else(|| Self::detect_init(&field.ty));

        Ok(Self {
            span: field.span(),
            ident,
            name,
            ty: field.ty.clone(),
            flatten: attrs.flatten,
            init,
        })
    }
}

/// Attributes for `TargetField`.
#[derive(Debug, Default, FromMeta)]
pub(crate) struct TargetFieldAttrs {
    #[darling(default)]
    pub rename: Option<String>,
    #[darling(default)]
    pub flatten: bool,
    #[darling(default)]
    pub init: Option<bool>,
}

#[derive(Debug)]
pub(crate) struct TargetStruct {
    pub vis: Visibility,
    pub ident: Ident,
    pub generics: Generics,
    pub style: Style,
    pub fields: Vec<TargetField>,
}

impl FromDeriveInput for TargetStruct {
    fn from_derive_input(input: &DeriveInput) -> darling::Result<Self> {
        let fields = match &input.data {
            Data::Struct(DataStruct { fields, .. }) => fields,
            _ => {
                return Err(darling::Error::unsupported_shape(
                    "`WithHandle` can be only implemented for structs",
                ))
            }
        };
        let fields = Fields::try_from(fields)?;

        let this = Self {
            vis: input.vis.clone(),
            ident: input.ident.clone(),
            generics: input.generics.clone(),
            style: fields.style,
            fields: fields.fields,
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
}
