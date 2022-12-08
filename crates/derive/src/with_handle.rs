//! Deriving `TakeHandle`.

use darling::{ast::Style, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{quote, ToTokens};
use syn::{
    AngleBracketedGenericArguments, DeriveInput, GenericArgument, Generics, Path, PathArguments,
    Type, TypePath,
};

use crate::utils::{find_meta_attrs, DeriveAttrs, TargetField, TargetStruct};

impl TargetField {
    fn unwrap_ty(&self, cr: &Path, fmt: &Ident) -> darling::Result<Path> {
        const MSG: &str = "fields in handle struct must have env param as last type arg`";

        let mut unwrapped_ty = self.ty.clone();
        let Some(last_segment) = unwrapped_ty.segments.last_mut() else {
            return Err(darling::Error::custom(MSG).with_span(&self.ty));
        };
        let args = match &mut last_segment.arguments {
            PathArguments::AngleBracketed(args) => &mut args.args,
            _ => return Err(darling::Error::custom(MSG).with_span(&self.ty)),
        };
        let last_arg = args
            .last_mut()
            .ok_or_else(|| darling::Error::custom(MSG).with_span(&self.ty))?;
        if let GenericArgument::Type(Type::Path(TypePath { path, .. })) = last_arg {
            if !path.is_ident(fmt) {
                return Err(darling::Error::custom(MSG).with_span(&self.ty));
            }
            *path = syn::parse_quote!(#cr::workflow::Wasm);
        } else {
            return Err(darling::Error::custom(MSG).with_span(&self.ty));
        }
        Ok(unwrapped_ty)
    }

    fn insert_handle(
        &self,
        cr: &Path,
        fmt: &Ident,
        field_index: usize,
    ) -> darling::Result<impl ToTokens> {
        let unwrapped_ty = self.unwrap_ty(cr, fmt)?;
        let path = self.path();
        let tr = quote!(#cr::workflow::WithHandle);
        let field = self.ident(field_index);
        Ok(quote! {
            <#unwrapped_ty as #tr>::insert_into_untyped(handle.#field, untyped, #path)
        })
    }

    fn init_by_take_handle(
        &self,
        cr: &Path,
        fmt: &Ident,
        field_index: usize,
    ) -> darling::Result<impl ToTokens> {
        let unwrapped_ty = self.unwrap_ty(cr, fmt)?;
        let path = self.path();
        let tr = quote!(#cr::workflow::WithHandle);
        let field = self.ident(field_index);
        Ok(quote!(#field: <#unwrapped_ty as #tr>::take_from_untyped(untyped, #path)?))
    }
}

#[derive(Debug)]
struct Handle {
    base: TargetStruct,
    fmt: Ident,
    crate_path: Path,
    derive_clone: bool,
    derive_debug: bool,
}

impl Handle {
    fn new(input: &DeriveInput, attrs: DeriveAttrs) -> darling::Result<Self> {
        let fmt = Self::fmt_generic(&input.generics)?;
        let mut base = TargetStruct::new(input)?;
        base.generics = input.generics.clone();

        for derive in attrs.derive.keys() {
            if derive != "Clone" && derive != "Debug" {
                let msg = format!(
                    "Unsupported additional derive: `{derive}`. Only `Clone` and `Debug` \
                    are supported for now"
                );
                return Err(darling::Error::custom(msg).with_span(input));
            }
        }

        Ok(Self {
            base,
            fmt,
            crate_path: attrs
                .crate_path
                .unwrap_or_else(|| syn::parse_quote!(tardigrade)),
            derive_clone: attrs.derive.contains_key("Clone"),
            derive_debug: attrs.derive.contains_key("Debug"),
        })
    }

    fn fmt_generic(generics: &Generics) -> darling::Result<Ident> {
        const MSG: &str = "Handle struct must have a format generic";

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

    fn impl_with_handle(&self) -> darling::Result<impl ToTokens> {
        let cr = &self.crate_path;
        let handle = &self.base.ident;
        let fmt = &self.fmt;
        let fmt_tr = quote!(#cr::workflow::HandleFormat);

        let (_, ty_generics, where_clause) = self.base.generics.split_for_impl();
        let mut reduced_generics = self.base.generics.clone();
        reduced_generics.params.pop(); // removes the format generic
        let (reduced_impl_generics, wasm_ty_generics, _) = reduced_generics.split_for_impl();
        let wasm_ty_generics = if reduced_generics.params.is_empty() {
            syn::parse_quote!(<#cr::workflow::Wasm>)
        } else {
            let mut args: AngleBracketedGenericArguments = syn::parse_quote!(#wasm_ty_generics);
            args.args.push(syn::parse_quote!(#cr::workflow::Wasm));
            args
        };

        let handle_fields = self.base.fields.iter().enumerate();
        let handle_fields = handle_fields
            .map(|(idx, field)| field.init_by_take_handle(cr, fmt, idx))
            .collect::<Result<Vec<_>, _>>()?;
        let take_fields = match self.base.style {
            Style::Struct => quote!({ #(#handle_fields,)* }),
            Style::Tuple | Style::Unit => quote!(( #(#handle_fields,)* )),
        };

        let handle_fields = self.base.fields.iter().enumerate();
        let insert_fields = handle_fields
            .map(|(idx, field)| field.insert_handle(cr, fmt, idx))
            .collect::<Result<Vec<_>, _>>()?;

        let tr = quote!(#cr::workflow::WithHandle);
        Ok(quote! {
            impl #reduced_impl_generics #tr for #handle #wasm_ty_generics #where_clause {
                type Handle<#fmt: #fmt_tr> = #handle #ty_generics;

                fn take_from_untyped<#fmt: #fmt_tr>(
                    untyped: &mut dyn #cr::workflow::TakeHandles<#fmt>,
                    path: #cr::handle::HandlePath<'_>,
                ) -> core::result::Result<Self::Handle<#fmt>, #cr::handle::AccessError> {
                    core::result::Result::Ok(#handle #take_fields)
                }

                fn insert_into_untyped<#fmt: #fmt_tr>(
                    handle: Self::Handle<#fmt>,
                    untyped: &mut dyn #cr::workflow::BuildHandles<#fmt>,
                    path: #cr::handle::HandlePath<'_>,
                ) {
                    #(#insert_fields;)*
                }
            }
        })
    }

    fn to_tokens(&self) -> darling::Result<proc_macro2::TokenStream> {
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
        let with_handle_impl = self.impl_with_handle()?;

        Ok(quote! {
            #clone_impl
            #debug_impl
            #with_handle_impl
        })
    }
}

fn derive_with_handle(input: &DeriveInput) -> darling::Result<impl ToTokens> {
    // Determine whether we deal with a handle or a delegated struct.
    let attrs = find_meta_attrs("tardigrade", &input.attrs).map_or_else(
        || Ok(DeriveAttrs::default()),
        |meta| DeriveAttrs::from_nested_meta(&meta),
    )?;
    Handle::new(input, attrs)?.to_tokens()
}

pub(crate) fn impl_with_handle(input: TokenStream) -> TokenStream {
    let input: DeriveInput = match syn::parse(input) {
        Ok(input) => input,
        Err(err) => return err.into_compile_error().into(),
    };
    let tokens = match derive_with_handle(&input) {
        Ok(tokens) => tokens,
        Err(err) => return err.write_errors().into(),
    };
    quote!(#tokens).into()
}
