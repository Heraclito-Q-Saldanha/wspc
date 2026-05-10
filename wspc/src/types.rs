use crate::*;

use std::collections;

pub const PARSE_ERROR_CODE: i32 = -32700;
pub const INVALID_REQUEST_CODE: i32 = -32600;
pub const METHOD_NOT_FOUND_CODE: i32 = -32601;
pub const INVALID_PARAMS_CODE: i32 = -32602;
pub const INTERNAL_ERROR_CODE: i32 = -32603;

pub type Value = serde_json::Value;
pub type Map<K = String, V = Value> = serde_json::Map<K, V>;
pub type Number = serde_json::Number;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
#[serde(untagged)]
pub enum RpcParams {
	#[default]
	Null,
	Array(collections::VecDeque<Value>),
	Object(Map<String, Value>),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Version {
	V1,
	#[default]
	V2,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Id {
	#[default]
	Null,
	Number(u64),
	String(String),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RpcRequest {
	#[serde(default)]
	pub jsonrpc: Version,
	#[serde(default)]
	pub id: Id,
	pub method: String,
	#[serde(default)]
	pub params: RpcParams,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum RpcResponse {
	Success { jsonrpc: Option<Version>, id: Id, result: Value },
	Error { jsonrpc: Option<Version>, id: Id, error: ErrorResponse },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Message<T> {
	Single(T),
	Batch(Vec<T>),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ErrorResponse {
	pub code: i32,
	pub message: String,
	pub data: Value,
}

impl RpcRequest {
	#[inline(always)]
	pub fn new(id: Id, method: &str, params: RpcParams) -> Self {
		Self {
			jsonrpc: Version::V2,
			method: method.to_string(),
			id,
			params,
		}
	}
}

impl TryFrom<Value> for RpcParams {
	type Error = error::Error;
	#[inline(always)]
	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Null => Ok(RpcParams::Null),
			Value::Array(array) => Ok(RpcParams::Array(array.into())),
			Value::Object(object) => Ok(RpcParams::Object(object)),
			_ => Err(error::Error::InvalidArgumentType),
		}
	}
}

impl From<RpcParams> for Value {
	#[inline(always)]
	fn from(value: RpcParams) -> Self {
		match value {
			RpcParams::Null => Value::Null,
			RpcParams::Array(array) => Value::Array(array.into()),
			RpcParams::Object(object) => Value::Object(object),
		}
	}
}

impl RpcResponse {
	#[inline(always)]
	pub fn parse_error(id: Id, data: Value) -> Self {
		Self::Error {
			jsonrpc: Some(Version::V2),
			error: ErrorResponse::parse_error(data),
			id,
		}
	}
	#[inline(always)]
	pub fn invalid_request(id: Id, data: Value) -> Self {
		Self::Error {
			id,
			jsonrpc: Some(Version::V2),
			error: ErrorResponse::invalid_request(data),
		}
	}
	#[inline(always)]
	pub fn method_not_found(id: Id, data: Value) -> Self {
		Self::Error {
			id,
			jsonrpc: Some(Version::V2),
			error: ErrorResponse::method_not_found(data),
		}
	}
	#[inline(always)]
	pub fn invalid_params(id: Id, data: Value) -> Self {
		Self::Error {
			id,
			jsonrpc: Some(Version::V2),
			error: ErrorResponse::invalid_params(data),
		}
	}
	#[inline(always)]
	pub fn internal_error(id: Id, data: Value) -> Self {
		Self::Error {
			id,
			jsonrpc: Some(Version::V2),
			error: ErrorResponse::internal_error(data),
		}
	}
}

impl ErrorResponse {
	#[inline(always)]
	pub fn parse_error(data: Value) -> Self {
		Self {
			code: PARSE_ERROR_CODE,
			message: "Failed to parse JSON".to_string(),
			data,
		}
	}
	#[inline(always)]
	pub fn invalid_request(data: Value) -> Self {
		Self {
			code: INVALID_REQUEST_CODE,
			message: "Invalid request".to_string(),
			data,
		}
	}
	#[inline(always)]
	pub fn method_not_found(data: Value) -> Self {
		Self {
			code: METHOD_NOT_FOUND_CODE,
			message: "Method not found".to_string(),
			data,
		}
	}
	#[inline(always)]
	pub fn invalid_params(data: Value) -> Self {
		Self {
			code: INVALID_PARAMS_CODE,
			message: "Invalid params".to_string(),
			data,
		}
	}
	#[inline(always)]
	pub fn internal_error(data: Value) -> Self {
		Self {
			code: INTERNAL_ERROR_CODE,
			message: "Internal error".to_string(),
			data,
		}
	}
}

impl IntoErrorResponse for ErrorResponse {
	#[inline(always)]
	fn into_error_response(self) -> ErrorResponse {
		self
	}
}

impl serde::Serialize for Version {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		match *self {
			Version::V1 => serializer.serialize_str("1.0"),
			Version::V2 => serializer.serialize_str("2.0"),
		}
	}
}

impl<'de> serde::Deserialize<'de> for Version {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let s = String::deserialize(deserializer)?;
		match s.as_str() {
			"1.0" => Ok(Version::V1),
			"2.0" => Ok(Version::V2),
			_ => Err(serde::de::Error::custom("invalid version")),
		}
	}
}
