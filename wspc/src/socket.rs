use crate::*;

use std::sync;

use axum::extract;

use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub(crate) enum Command {
	JoinRoom { name: String, ack: oneshot::Sender<()> },
	LeaveRoom { name: String, ack: oneshot::Sender<()> },
	SendMessage(extract::ws::Message),
	Close { ack: oneshot::Sender<()> },
}

struct InnerSocket {
	#[cfg(any(feature = "uuid_v4", feature = "uuid_v7"))]
	id: uuid::Uuid,
	#[cfg(feature = "state")]
	state: TypeMap,
	sender: mpsc::UnboundedSender<Command>,
}

#[derive(Clone)]
pub struct Socket {
	inner: sync::Arc<InnerSocket>,
}

impl Socket {
	pub(crate) fn new(sender: mpsc::UnboundedSender<Command>) -> Self {
		#[cfg(feature = "uuid_v4")]
		let id = uuid::Uuid::new_v4();
		#[cfg(feature = "uuid_v7")]
		let id = uuid::Uuid::now_v7();
		#[cfg(feature = "state")]
		let state = TypeMap::new();

		let inner = {
			sync::Arc::new(InnerSocket {
				#[cfg(any(feature = "uuid_v4", feature = "uuid_v7"))]
				id,
				#[cfg(feature = "state")]
				state,
				sender,
			})
		};

		Socket { inner }
	}
	pub(crate) fn write(&self, msg: extract::ws::Message) -> error::Result<()> {
		Ok(self.inner.sender.send(Command::SendMessage(msg))?)
	}
	pub fn send<T: serde::Serialize>(&self, method: &str, msg: T) -> error::Result<()> {
		let params = RpcParams::try_from(serde_json::to_value(msg)?)?;
		let message = serde_json::to_string(&RpcRequest::new(Id::Null, &method, params))?;

		Ok(self.write(extract::ws::Message::Text(message.into()))?)
	}
	#[cfg(any(feature = "uuid_v4", feature = "uuid_v7"))]
	#[inline]
	pub fn id(&self) -> uuid::Uuid {
		self.inner.id
	}
	#[cfg(feature = "state")]
	#[inline]
	pub fn set_state<T: Send + Sync + Clone + 'static>(&self, value: T) -> Option<T> {
		self.inner.state.set(value)
	}
	#[cfg(feature = "state")]
	#[inline]
	pub fn get_state<T: Send + Sync + Clone + 'static>(&self) -> Option<T> {
		self.inner.state.get::<T>()
	}
	pub async fn join<T: ToString>(&self, room: T) -> error::Result<()> {
		let name = room.to_string();
		let (ack, awaiter) = oneshot::channel();

		self.inner.sender.send(Command::JoinRoom { name, ack })?;

		Ok(awaiter.await?)
	}
	pub async fn leave<T: ToString>(&self, room: T) -> error::Result<()> {
		let name = room.to_string();
		let (ack, awaiter) = oneshot::channel();

		self.inner.sender.send(Command::LeaveRoom { name, ack })?;

		Ok(awaiter.await?)
	}
	pub async fn close(&self) -> error::Result<()> {
		let (ack, awaiter) = oneshot::channel();
		self.inner.sender.send(Command::Close { ack })?;

		Ok(awaiter.await?)
	}
}

impl Arg for Socket {
	type Error = error::Error;
	#[inline(always)]
	fn from_context(context: &mut CallContext) -> Result<Self, Self::Error> {
		Ok(context.socket.clone())
	}
}
