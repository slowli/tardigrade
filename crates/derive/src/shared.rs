//! Types / logic shared across multiple derive macros.

use darling::{ast::Style, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::{quote, ToTokens};
use syn::{
    parse::Parser, punctuated::Punctuated, spanned::Spanned, Attribute, Data, DataStruct,
    DeriveInput, NestedMeta, Path, Token, Type,
};

use std::collections::HashSet;

fn take_meta_attr(name: &str, attrs: &mut Vec<Attribute>) -> Option<NestedMeta> {
    let (i, meta) = attrs.iter().enumerate().find_map(|(i, attr)| {
        if let Ok(meta) = attr.parse_meta() {
            if meta.path().is_ident(name) {
                return Some((i, NestedMeta::from(meta)));
            }
        }
        None
    })?;
    attrs.remove(i);
    Some(meta)
}

#[derive(Debug)]
pub(crate) struct FieldWrapper {
    pub ident: Ident,
    pub inner_types: Vec<Type>,
}

impl FieldWrapper {
    const MAX_INNER_TYPES: usize = 2;

    fn new(ty: &Type) -> Option<Self> {
        if let Type::Path(syn::TypePath { path, .. }) = ty {
            let last_segment = path.segments.last().unwrap();
            let args = match &last_segment.arguments {
                syn::PathArguments::AngleBracketed(args) => &args.args,
                _ => return None,
            };

            if args.is_empty() || args.len() > Self::MAX_INNER_TYPES {
                return None;
            }
            let inner_types = args.iter().filter_map(|arg| match arg {
                syn::GenericArgument::Type(ty) => Some(ty.clone()),
                _ => None,
            });
            let inner_types: Vec<_> = inner_types.collect();
            if inner_types.len() != args.len() {
                None
            } else {
                Some(Self {
                    ident: last_segment.ident.clone(),
                    inner_types,
                })
            }
        } else {
            None
        }
    }
}

/// Field of a struct for which one of traits needs to be derived.
#[derive(Debug)]
pub(crate) struct TargetField {
    pub span: Span,
    pub ident: Option<Ident>,
    /// Name of the field considering a potential `#[_(rename = "...")]` override.
    pub name: Option<String>,
    pub _ty: Type,
    pub flatten: bool,
    pub wrapper: Option<FieldWrapper>,
}

impl TargetField {
    fn new(field: &mut syn::Field) -> darling::Result<Self> {
        let ident = field.ident.clone();

        let attrs = take_meta_attr("tardigrade", &mut field.attrs)
            .map(|meta| TargetFieldAttrs::from_nested_meta(&meta))
            .unwrap_or_else(|| Ok(TargetFieldAttrs::default()))?;

        let name = attrs
            .rename
            .or_else(|| ident.as_ref().map(ToString::to_string));

        Ok(Self {
            span: field.span(),
            ident,
            name,
            _ty: field.ty.clone(),
            flatten: attrs.flatten,
            wrapper: FieldWrapper::new(&field.ty),
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

    pub(crate) fn id_ty(&self) -> impl ToTokens {
        if self.flatten {
            quote!(())
        } else {
            quote!(str)
        }
    }

    pub(crate) fn id(&self) -> impl ToTokens {
        if self.flatten {
            quote!(&())
        } else {
            let name = self.name.as_ref().unwrap(); // Safe because of previous validation
            quote!(#name)
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
}

impl TargetStruct {
    pub fn new(input: &mut DeriveInput) -> darling::Result<Self> {
        let fields = match &mut input.data {
            Data::Struct(DataStruct { fields, .. }) => fields,
            _ => {
                return Err(darling::Error::unsupported_shape(
                    "can be only implemented for structs",
                ))
            }
        };
        let style = Style::from(&*fields);
        let fields = fields
            .iter_mut()
            .map(TargetField::new)
            .collect::<Result<_, _>>()?;

        let this = Self {
            ident: input.ident.clone(),
            style,
            fields,
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

#[derive(Debug, FromMeta)]
pub(crate) struct MacroAttrs {
    #[darling(rename = "for")]
    pub target: Path,
}

impl MacroAttrs {
    pub fn parse(tokens: TokenStream) -> darling::Result<Self> {
        let meta = Punctuated::<NestedMeta, Token![,]>::parse_terminated.parse(tokens)?;
        let meta: Vec<_> = meta.into_iter().collect();
        Self::from_list(&meta)
    }
}
