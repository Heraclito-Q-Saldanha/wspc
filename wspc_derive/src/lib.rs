use quote::ToTokens;

#[proc_macro_derive(IntoResponse)]
pub fn derive_into_response(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let input = syn::parse_macro_input!(input as syn::DeriveInput);
	expand_derive_into_response(input).unwrap_or_else(syn::Error::into_compile_error).into()
}

fn expand_derive_into_response(input: syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
	let ident = input.ident;
	let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

	Ok(quote::quote! {
		impl #impl_generics ::wspc::IntoResponse for #ident #ty_generics #where_clause {
			fn into_response(self) -> ::wspc::CallbackResult {
				match ::serde_json::to_value(self) {
					Ok(value) => Ok(value),
					Err(_) => Err(::wspc::ErrorResponse::parse_error(::wspc::Value::Null)),
				}
			}
		}
	})
}

#[proc_macro_derive(IntoErrorResponse, attributes(response))]
pub fn derive_into_error_response(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let input = syn::parse_macro_input!(input as syn::DeriveInput);
	expand_derive(input).unwrap_or_else(syn::Error::into_compile_error).into()
}

fn expand_derive(input: syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
	let ident = input.ident;
	let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
	let (type_code, type_message) = parse_error_response_attrs(&input.attrs)?;

	let body = match input.data {
		syn::Data::Enum(data) => {
			let mut arms = Vec::new();
			for variant in data.variants {
				let v_ident = variant.ident;
				let (code, message) = parse_error_response_attrs(&variant.attrs)?;
				let code_tokens = code.or(type_code.clone()).unwrap_or_else(|| quote::quote! { ::wspc::INTERNAL_ERROR_CODE });
				let message_tokens = message.or(type_message.clone()).unwrap_or_else(|| quote::quote! { "Internal error".to_string() });

				let pattern = match variant.fields {
					syn::Fields::Unit => quote::quote! { Self::#v_ident },
					syn::Fields::Unnamed(_) => quote::quote! { Self::#v_ident (..) },
					syn::Fields::Named(_) => quote::quote! { Self::#v_ident { .. } },
				};

				arms.push(quote::quote! {
					#pattern => ::wspc::ErrorResponse {
						code: #code_tokens,
						message: #message_tokens,
						data: ::wspc::Value::Null,
					}
				});
			}

			quote::quote! {
				match self {
					#(#arms),*
				}
			}
		}
		syn::Data::Struct(_) | syn::Data::Union(_) => {
			let code_tokens = type_code.unwrap_or_else(|| quote::quote! { ::wspc::INTERNAL_ERROR_CODE });
			let message_tokens = type_message.unwrap_or_else(|| quote::quote! { "Internal error".to_string() });

			quote::quote! {
				::wspc::ErrorResponse {
					code: #code_tokens,
					message: #message_tokens,
					data: ::wspc::Value::Null,
				}
			}
		}
	};

	Ok(quote::quote! {
		impl #impl_generics ::wspc::IntoErrorResponse for #ident #ty_generics #where_clause {
			fn into_error_response(self) -> ::wspc::ErrorResponse {
				#body
			}
		}
	})
}

fn parse_error_response_attrs(attrs: &[syn::Attribute]) -> syn::Result<(Option<proc_macro2::TokenStream>, Option<proc_macro2::TokenStream>)> {
	let mut code = None;
	let mut message = None;

	for attr in attrs {
		if !attr.path().is_ident("response") {
			continue;
		}

		attr.parse_nested_meta(|meta| {
			if meta.path.is_ident("code") {
				let expr: syn::Expr = meta.value()?.parse()?;
				code = Some(quote::quote! { #expr });
				return Ok(());
			}

			if meta.path.is_ident("message") {
				let expr: syn::Expr = meta.value()?.parse()?;
				let tokens = match expr {
					syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }) => quote::quote! { #s.to_string() },
					_ => quote::quote! { (#expr).to_string() },
				};
				message = Some(tokens);
				return Ok(());
			}

			let option = meta.path.to_token_stream().to_string();
			Err(meta.error(format!("unknown error_response option `{option}`; expected `code` or `message`")))
		})?;
	}

	Ok((code, message))
}
