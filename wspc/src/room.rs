use crate::*;

use std::sync;

use tokio::sync::broadcast;
use tokio_stream::wrappers;

struct InnerRoom {
	sender: broadcast::Sender<RpcRequest>,
	#[cfg(any(feature = "uuid_v4", feature = "uuid_v7"))]
	sockets: dashmap::DashMap<uuid::Uuid, Socket>,
}

#[derive(Clone)]
pub struct Room {
	inner: sync::Arc<InnerRoom>,
}

impl Room {
	pub(crate) fn new() -> Self {
		let sender = broadcast::channel(1024).0;
		#[cfg(any(feature = "uuid_v4", feature = "uuid_v7"))]
		let sockets = dashmap::DashMap::new();

		let inner = sync::Arc::new(InnerRoom {
			sender,
			#[cfg(any(feature = "uuid_v4", feature = "uuid_v7"))]
			sockets,
		});

		Self { inner }
	}
	pub(crate) fn broadcast_stream(&self) -> wrappers::BroadcastStream<RpcRequest> {
		wrappers::BroadcastStream::new(self.inner.sender.subscribe())
	}
	#[cfg(any(feature = "uuid_v4", feature = "uuid_v7"))]
	pub(crate) fn insert_socket(&self, socket: Socket) {
		self.inner.sockets.insert(socket.id(), socket);
	}
	#[cfg(any(feature = "uuid_v4", feature = "uuid_v7"))]
	pub(crate) fn remove_socket(&self, socket_id: uuid::Uuid) {
		self.inner.sockets.remove(&socket_id);
	}
	pub fn emit<T: serde::Serialize>(&self, method: &str, args: T) -> error::Result<()> {
		let params = RpcParams::try_from(serde_json::to_value(args)?)?;
		let message = RpcRequest::new(Id::Null, &method, params);
		let _ = self.inner.sender.send(message);

		Ok(())
	}
	#[cfg(any(feature = "uuid_v4", feature = "uuid_v7"))]
	pub fn sockets(&self) -> Vec<Socket> {
		self.inner.sockets.iter().map(|s| s.value().clone()).collect()
	}
}
