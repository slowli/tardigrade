use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{DeriveInput, Path};

use crate::utils::{MacroAttrs, TargetField, TargetStruct};

#[derive(Debug)]
struct Initialize {
    base: TargetStruct,
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
    fn new(input: &mut DeriveInput, attrs: MacroAttrs) -> darling::Result<Self> {
        if !input.generics.params.is_empty() {
            let message = "generics are not supported";
            return Err(darling::Error::custom(message).with_span(&input.generics));
        }
        let base = TargetStruct::new(input)?;
        for field in &base.fields {
            field.check_for_init()?;
        }

        Ok(Self {
            base,
            target: attrs.target,
        })
    }

    fn impl_initialize(&self) -> impl ToTokens {
        let target = &self.target;
        let init = &self.base.ident;
        let tr = quote!(tardigrade::workflow::Initialize);

        let init_fields = self.base.fields.iter().enumerate();
        let init_fields = init_fields.map(|(idx, field)| field.init_from_field(idx));

        quote! {
            impl #tr for #target {
                type Id = ();
                type Init = #init;

                fn initialize(
                    builder: &mut tardigrade::workflow::InputsBuilder,
                    init: Self::Init,
                    _id: &(),
                ) {
                    #(#init_fields)*
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
    let attrs = match MacroAttrs::parse(attr) {
        Ok(attrs) => attrs,
        Err(err) => return err.write_errors().into(),
    };
    let mut input: syn::DeriveInput = match syn::parse(input) {
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
