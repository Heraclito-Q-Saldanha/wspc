use crate::*;

use std::sync;

use axum::extract;

use tokio::sync::broadcast;
use tokio::sync::mpsc;

pub(crate) enum Command {
	Join { room: String, receiver: broadcast::Receiver<RpcRequest> },
	Leave { room: String },
	Message(extract::ws::Message),
}

struct InnerSocket {
	#[cfg(any(feature = "uuid_v4", feature = "uuid_v7"))]
	id: uuid::Uuid,
	#[cfg(feature = "state")]
	state: TypeMap,
	app: App,
	sender: mpsc::UnboundedSender<Command>,
}

#[derive(Clone)]
pub struct Socket {
	inner: sync::Arc<InnerSocket>,
}

impl Socket {
	pub(crate) fn new(app: App, sender: mpsc::UnboundedSender<Command>) -> Self {
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
				app,
				sender,
			})
		};

		Socket { inner }
	}
	pub(crate) fn write(&self, msg: extract::ws::Message) -> error::Result<()> {
		Ok(self.inner.sender.send(Command::Message(msg))?)
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
	pub fn join<T: ToString>(&self, room: T) -> error::Result<()> {
		let room = room.to_string();
		let receiver = self.inner.app.room(&room).subscribe();

		self.inner.sender.send(Command::Join { room, receiver })?;

		Ok(())
	}
	pub fn leave<T: ToString>(&self, room: T) -> error::Result<()> {
		let room = room.to_string();

		self.inner.sender.send(Command::Leave { room })?;

		Ok(())
	}
}

impl Arg for Socket {
	type Error = error::Error;
	#[inline(always)]
	fn from_context(context: &mut CallContext) -> Result<Self, Self::Error> {
		Ok(context.socket.clone())
	}
}
