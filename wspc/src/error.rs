use crate::*;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
	ArgumentNotFound,
	InvalidArgumentType,
	SocketClosed,
	Serde(serde_json::Error),
}

impl From<serde_json::Error> for Error {
	#[inline(always)]
	fn from(value: serde_json::Error) -> Self {
		Self::Serde(value)
	}
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
	#[inline(always)]
	fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
		Self::SocketClosed
	}
}

impl IntoErrorResponse for Error {
	fn into_error_response(self) -> ErrorResponse {
		match self {
			Self::ArgumentNotFound => ErrorResponse::invalid_params(Value::Null),
			Self::InvalidArgumentType => ErrorResponse::invalid_params(Value::Null),
			Self::Serde(e) => ErrorResponse::parse_error(Value::String(e.to_string())),
			_ => ErrorResponse::internal_error(Value::Null),
		}
	}
}
