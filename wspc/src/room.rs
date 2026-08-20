use crate::*;

use std::sync;

use tokio::sync::broadcast;
use tokio_stream::wrappers;

struct InnerRoom {
	sender: broadcast::Sender<RpcRequest>,
}

#[derive(Clone)]
pub struct Room {
	inner: sync::Arc<InnerRoom>,
}

impl Room {
	pub(crate) fn new() -> Self {
		let sender = broadcast::channel(1024).0;
		let inner = sync::Arc::new(InnerRoom { sender });

		Self { inner }
	}
	pub(crate) fn broadcast_stream(&self) -> wrappers::BroadcastStream<RpcRequest> {
		wrappers::BroadcastStream::new(self.inner.sender.subscribe())
	}
	pub fn emit<T: serde::Serialize>(&self, method: &str, args: T) -> error::Result<()> {
		let params = RpcParams::try_from(serde_json::to_value(args)?)?;
		let message = RpcRequest::new(Id::Null, &method, params);
		let _ = self.inner.sender.send(message);

		Ok(())
	}
}
