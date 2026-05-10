use crate::*;

use std::future;
use std::marker;
use std::pin;
use std::sync;

pub type CallbackResult = Result<Value, ErrorResponse>;
pub type CallbackFuture = pin::Pin<Box<dyn future::Future<Output = CallbackResult> + Send>>;

pub struct CallContext {
	pub app: App,
	pub socket: Socket,
	pub args: RpcParams,
}

pub struct Params<T>(pub T);

impl<T> Params<T> {
	#[inline(always)]
	pub fn into_inner(self) -> T {
		self.0
	}
}

impl<T> std::ops::Deref for Params<T> {
	type Target = T;

	#[inline(always)]
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<T> std::ops::DerefMut for Params<T> {
	#[inline(always)]
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

pub trait IntoResponse {
	fn into_response(self) -> CallbackResult;
}

pub trait IntoErrorResponse {
	fn into_error_response(self) -> ErrorResponse;
}

pub trait Arg: Sized {
	type Error: IntoErrorResponse;
	fn from_context(context: &mut CallContext) -> Result<Self, Self::Error>;
}

pub struct AsyncCall;
pub struct SyncCall;

pub trait FunctionCall<Args, Kind> {
	fn call(&self, ctx: CallContext) -> CallbackFuture;
}

trait ErasedFunctionCall {
	fn call(&self, ctx: CallContext) -> CallbackFuture;
}

#[derive(Clone)]
pub struct Callback {
	handler: sync::Arc<dyn ErasedFunctionCall + Send + Sync>,
}

impl Callback {
	pub fn new<Args: Send + Sync + 'static, Kind: Send + Sync + 'static, F: FunctionCall<Args, Kind> + Send + Sync + 'static>(handler: F) -> Self {
		let handler = sync::Arc::new((handler, std::marker::PhantomData::<(Args, Kind)>));
		Self { handler }
	}
	#[inline(always)]
	pub fn call(&self, ctx: CallContext) -> CallbackFuture {
		self.handler.call(ctx)
	}
}

impl<Args, Kind, F: FunctionCall<Args, Kind>> ErasedFunctionCall for (F, marker::PhantomData<(Args, Kind)>) {
	#[inline(always)]
	fn call(&self, ctx: CallContext) -> CallbackFuture {
		self.0.call(ctx)
	}
}

impl<T: serde::de::DeserializeOwned> Arg for T {
	type Error = error::Error;
	fn from_context(ctx: &mut CallContext) -> Result<Self, Self::Error> {
		match &mut ctx.args {
			RpcParams::Null => Err(error::Error::ArgumentNotFound),
			RpcParams::Object(args) => {
				let args = std::mem::take(args);
				let value = Value::Object(args);
				Ok(serde_json::from_value(value)?)
			}
			RpcParams::Array(args) => match args.pop_front() {
				Some(arg) => Ok(serde_json::from_value(arg)?),
				None => Err(error::Error::ArgumentNotFound),
			},
		}
	}
}

impl<T: serde::de::DeserializeOwned> Arg for Params<T> {
	type Error = error::Error;

	#[inline(always)]
	fn from_context(ctx: &mut CallContext) -> Result<Self, Self::Error> {
		let args = std::mem::take(&mut ctx.args);
		let value: Value = args.into();
		Ok(Self(serde_json::from_value(value)?))
	}
}

impl Arg for App {
	type Error = error::Error;
	#[inline(always)]
	fn from_context(context: &mut CallContext) -> Result<Self, Self::Error> {
		Ok(context.app.clone())
	}
}

impl<T: serde::Serialize, E: IntoErrorResponse> IntoResponse for Result<T, E> {
	fn into_response(self) -> CallbackResult {
		match self {
			Ok(t) => match serde_json::to_value(t) {
				Ok(t) => Ok(t),
				Err(_) => Err(ErrorResponse::internal_error(Value::Null)),
			},
			Err(e) => Err(e.into_error_response()),
		}
	}
}
