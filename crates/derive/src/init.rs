use darling::FromMeta;
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{DeriveInput, Ident, Path};

use crate::utils::{parse_attr, TargetField, TargetStruct};

#[derive(Debug, FromMeta)]
struct InitAttrs {
    #[darling(rename = "for")]
    target: Path,
    #[darling(default)]
    rename: Option<String>,
    #[darling(default)]
    codec: Option<Path>,
}

#[derive(Debug)]
enum InitializeStruct {
    MultiField(TargetStruct),
    SingleField {
        ident: Ident,
        codec: Path,
        name: String,
    },
}

impl InitializeStruct {
    fn ident(&self) -> &Ident {
        match self {
            Self::MultiField(target) => &target.ident,
            Self::SingleField { ident, .. } => ident,
        }
    }

    fn initialize_method(&self) -> impl ToTokens {
        match self {
            Self::MultiField(target) => {
                let init_fields = target.fields.iter().enumerate();
                let init_fields = init_fields.map(|(idx, field)| field.init_from_field(idx));
                quote!(#(#init_fields)*)
            }
            Self::SingleField { name, codec, ident } => {
                let ty = quote!(tardigrade::Data<#ident, #codec>);
                let tr = quote!(tardigrade::workflow::Initialize);
                quote!(<#ty as #tr>::initialize(&mut *builder, init, #name))
            }
        }
    }
}

#[derive(Debug)]
struct Initialize {
    base: InitializeStruct,
    target: Path,
}

impl TargetField {
    fn check_for_init(&self) -> darling::Result<()> {
        const MSG: &str = "fields in init struct must be wrapped in `Init<_>`";

        let wrapper = self
            .wrapper
            .as_ref()
            .ok_or_else(|| darling::Error::custom(MSG).with_span(&self.span))?;

        if wrapper.ident != "Init" || wrapper.inner_types.len() != 1 {
            Err(darling::Error::custom(MSG).with_span(&self.span))
        } else {
            Ok(())
        }
    }

    fn init_from_field(&self, field_index: usize) -> impl ToTokens {
        let name = self.ident(field_index);
        let id = self.id();
        let unwrapped_ty = &self.wrapper.as_ref().unwrap().inner_types[0];
        let tr = quote!(tardigrade::workflow::Initialize);

        quote! {
            <#unwrapped_ty as #tr>::initialize(&mut *builder, init.#name, #id);
        }
    }
}

impl Initialize {
    fn new(input: &mut DeriveInput, attrs: InitAttrs) -> darling::Result<Self> {
        if !input.generics.params.is_empty() {
            let message = "generics are not supported";
            return Err(darling::Error::custom(message).with_span(&input.generics));
        }
        let base = if let Some(codec) = attrs.codec {
            InitializeStruct::SingleField {
                ident: input.ident.clone(),
                codec,
                name: attrs
                    .rename
                    .unwrap_or_else(|| Self::normalize_name(&input.ident)),
            }
        } else {
            let base = TargetStruct::new(input)?;
            for field in &base.fields {
                field.check_for_init()?;
            }
            InitializeStruct::MultiField(base)
        };

        Ok(Self {
            base,
            target: attrs.target,
        })
    }

    fn normalize_name(ident: &Ident) -> String {
        let ident = ident.to_string();
        let mut snake = String::with_capacity(ident.len());
        for (i, ch) in ident.char_indices() {
            if i > 0 && ch.is_ascii_uppercase() {
                snake.push('_');
            }
            snake.push(ch.to_ascii_lowercase());
        }
        snake
    }

    fn impl_initialize(&self) -> impl ToTokens {
        let target = &self.target;
        let init = self.base.ident();
        let tr = quote!(tardigrade::workflow::Initialize);
        let method_impl = self.base.initialize_method();

        quote! {
            impl #tr for #target {
                type Id = ();
                type Init = #init;

                fn initialize(
                    builder: &mut tardigrade::workflow::InputsBuilder,
                    init: Self::Init,
                    _id: &(),
                ) {
                    #method_impl
                }
            }
        }
    }
}

impl ToTokens for Initialize {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let initialize_impl = self.impl_initialize();
        tokens.extend(quote! {
            #initialize_impl
        });
    }
}

pub(crate) fn impl_initialize(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attrs = match parse_attr::<InitAttrs>(attr) {
        Ok(attrs) => attrs,
        Err(err) => return err.write_errors().into(),
    };
    let mut input: DeriveInput = match syn::parse(input) {
        Ok(input) => input,
        Err(err) => return err.into_compile_error().into(),
    };
    let init = match Initialize::new(&mut input, attrs) {
        Ok(init) => init,
        Err(err) => return err.write_errors().into(),
    };
    let tokens = quote!(#input #init);
    tokens.into()
}
