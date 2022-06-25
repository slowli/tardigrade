//! Types / logic shared across multiple derive macros.

use darling::{ast::Style, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::{quote, ToTokens};
use syn::{
    parse::Parser, parse_quote, punctuated::Punctuated, spanned::Spanned, Attribute, Data,
    DataStruct, DeriveInput, Generics, Meta, NestedMeta, Path, Token, Type,
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

pub(crate) fn take_derive(attrs: &mut Vec<syn::Attribute>, name: &str) -> bool {
    let idx = attrs.iter_mut().enumerate().find_map(|(i, attr)| {
        if attr.path.is_ident("derive") {
            if let Ok(Meta::List(mut list)) = attr.parse_meta() {
                let trait_pos = list.nested.iter().position(|tr| {
                    if let NestedMeta::Meta(Meta::Path(path)) = tr {
                        path.is_ident(name)
                    } else {
                        false
                    }
                });

                if let Some(trait_pos) = trait_pos {
                    remove_nested(&mut list.nested, trait_pos);
                    *attr = parse_quote!(#[ #list ]);
                    return Some((i, list.nested.is_empty()));
                }
            }
        }
        None
    });

    if let Some((idx, needs_removal)) = idx {
        if needs_removal {
            attrs.remove(idx);
        }
        true
    } else {
        false
    }
}

fn remove_nested(list: &mut Punctuated<NestedMeta, Token![,]>, idx: usize) {
    debug_assert!(idx < list.len());
    let tail_len = list.len() - idx - 1;
    let mut popped_items: Vec<_> = (0..tail_len)
        .map(|_| list.pop().unwrap().into_value())
        .collect();
    popped_items.reverse();

    list.pop(); // Remove the target item
    for item in popped_items {
        list.push(item);
    }
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
    pub ty: Type,
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
            ty: field.ty.clone(),
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
        let mut where_clause = where_clause.unwrap().clone();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taking_derive() {
        let mut input: DeriveInput = parse_quote! {
            #[derive(Debug, PartialEq)]
            #[derive(Clone)]
            struct Foo;
        };
        take_derive(&mut input.attrs, "Debug");

        let expected: DeriveInput = parse_quote! {
            #[derive(PartialEq)]
            #[derive(Clone)]
            struct Foo;
        };
        assert_eq!(input, expected);

        take_derive(&mut input.attrs, "Clone");

        let expected: DeriveInput = parse_quote! {
            #[derive(PartialEq)]
            struct Foo;
        };
        assert_eq!(input, expected);
    }
}
