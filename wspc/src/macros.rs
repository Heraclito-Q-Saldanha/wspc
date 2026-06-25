use crate::*;

macro_rules! impl_function_call {
	( ) => {
		impl<X: IntoResponse + Send + 'static, Z: Fn() -> X> FunctionCall<(), SyncCall> for Z {
			fn call(&self, _ctx: CallContext) -> CallbackFuture {
				let result = (self)();
				Box::pin(async move { result.into_response() })
			}
		}

		impl<X: IntoResponse, Y: std::future::Future<Output = X> + Send + 'static, Z: Fn() -> Y> FunctionCall<(), AsyncCall> for Z {
			fn call(&self, _ctx: CallContext) -> CallbackFuture {
				let future = (self)();
				Box::pin(async move { future.await.into_response() })
			}
		}
	};
	( $( $name:ident ),* ) => {
	       #[allow(nonstandard_style)]
	       impl<X: IntoResponse + Send + 'static, Z: Fn($( $name ),*) -> X, $( $name : Arg ),*> FunctionCall<( $( $name, )* ), SyncCall> for Z {
	           fn call(&self, mut ctx: CallContext) -> CallbackFuture {
	            	$(
						let $name = match $name::from_context(&mut ctx) {
							Ok(value) => value,
							Err(err) => {
								let err = err.into_error_response();
								return Box::pin(async move { Err(err) });
							}
						};
	            	)*
	            	let result = (self)( $( $name ),* ).into_response();
					Box::pin(async move { result })
	           }
	       }

	       #[allow(nonstandard_style)]
	       impl<X: IntoResponse, Y: std::future::Future<Output = X> + Send + 'static, Z: Fn($( $name ),*) -> Y, $( $name : Arg ),*> FunctionCall<( $( $name, )* ), AsyncCall> for Z {
	           fn call(&self, mut ctx: CallContext) -> CallbackFuture {
	            	$(
						let $name = match $name::from_context(&mut ctx) {
							Ok(value) => value,
							Err(err) => {
								let err = err.into_error_response();
								return Box::pin(async move { Err(err) });
							}
						};
	            	)*
	            	let future = (self)( $( $name ),* );

	            	Box::pin(async move { future.await.into_response() })
	           }
	       }
	   };
}

macro_rules! impl_default_response {
    ($t:ty, $( $generic:ident ),* ) => {
        impl<$( $generic: serde::Serialize ),*> IntoResponse for $t {
            #[inline(always)]
            fn into_response(self) -> CallbackResult {
                match serde_json::to_value(self) {
                    Ok(value) => Ok(value),
                    Err(_) => Err(ErrorResponse::parse_error(Value::Null)),
                }
            }
        }
    };
}
impl_function_call!();
impl_function_call!(A);
impl_function_call!(A, B);
impl_function_call!(A, B, C);
impl_function_call!(A, B, C, D);
impl_function_call!(A, B, C, D, E);
impl_function_call!(A, B, C, D, E, F);
impl_function_call!(A, B, C, D, E, F, G);
impl_function_call!(A, B, C, D, E, F, G, H);
impl_function_call!(A, B, C, D, E, F, G, H, I);
impl_function_call!(A, B, C, D, E, F, G, H, I, J);
impl_function_call!(A, B, C, D, E, F, G, H, I, J, K);
impl_function_call!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_function_call!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_function_call!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_function_call!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_function_call!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

impl_default_response!(Value,);
impl_default_response!(u8,);
impl_default_response!(i8,);
impl_default_response!(u16,);
impl_default_response!(i16,);
impl_default_response!(u32,);
impl_default_response!(i32,);
impl_default_response!(u64,);
impl_default_response!(i64,);
impl_default_response!(u128,);
impl_default_response!(i128,);
impl_default_response!(f32,);
impl_default_response!(f64,);
impl_default_response!(String,);
impl_default_response!(&'static str,);

impl_default_response!(Vec<T>, T);
impl_default_response!(Box<[T]>, T);

impl_default_response!((),);
impl_default_response!((A,), A);
impl_default_response!((A, B,), A, B);
impl_default_response!((A, B, C), A, B, C);
impl_default_response!((A, B, C, D), A, B, C, D);
impl_default_response!((A, B, C, D, E), A, B, C, D, E);
impl_default_response!((A, B, C, D, E, F), A, B, C, D, E, F);
impl_default_response!((A, B, C, D, E, F, G), A, B, C, D, E, F, G);
impl_default_response!((A, B, C, D, E, F, G, H), A, B, C, D, E, F, G, H);
impl_default_response!((A, B, C, D, E, F, G, H, I), A, B, C, D, E, F, G, H, I);
impl_default_response!((A, B, C, D, E, F, G, H, I, J), A, B, C, D, E, F, G, H, I, J);
impl_default_response!((A, B, C, D, E, F, G, H, I, J, K), A, B, C, D, E, F, G, H, I, J, K);
impl_default_response!((A, B, C, D, E, F, G, H, I, J, K, L), A, B, C, D, E, F, G, H, I, J, K, L);
impl_default_response!((A, B, C, D, E, F, G, H, I, J, K, L, M), A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_default_response!((A, B, C, D, E, F, G, H, I, J, K, L, M, N), A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_default_response!((A, B, C, D, E, F, G, H, I, J, K, L, M, N, O), A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_default_response!((A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P), A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);
