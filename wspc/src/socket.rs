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
	id: uuid::Uuid,
	app: App,
	state: state::TypeMap![Send + Sync],
	sender: mpsc::UnboundedSender<Command>,
}

#[derive(Clone)]
pub struct Socket {
	inner: sync::Arc<tokio::sync::RwLock<InnerSocket>>,
}

impl Socket {
	pub(crate) fn new(app: App, sender: mpsc::UnboundedSender<Command>) -> Self {
		let id = uuid::Uuid::new_v4();
		let state = state::TypeMap::default();

		let inner = sync::Arc::new(tokio::sync::RwLock::new(InnerSocket { id, app, state, sender }));

		Socket { inner }
	}
	pub(crate) async fn write(&self, msg: extract::ws::Message) -> error::Result<()> {
		let inner = self.inner.read().await;
		Ok(inner.sender.send(Command::Message(msg))?)
	}
	pub async fn send<T: serde::Serialize>(&self, method: &str, msg: T) -> error::Result<()> {
		let params = RpcParams::try_from(serde_json::to_value(msg)?)?;
		let message = serde_json::to_string(&RpcRequest::new(Id::Null, &method, params))?;
		Ok(self.write(extract::ws::Message::Text(message.into())).await?)
	}
	#[inline]
	pub async fn id(&self) -> uuid::Uuid {
		let inner = self.inner.read().await;
		inner.id
	}
	#[inline]
	pub async fn set_state<T: Send + Sync + Clone + 'static>(&self, value: T) -> bool {
		let inner = self.inner.read().await;
		inner.state.set(value)
	}
	#[inline]
	pub async fn get_state<T: Send + Sync + Clone + 'static>(&self) -> Option<T> {
		let inner = self.inner.read().await;
		inner.state.try_get::<T>().cloned()
	}
	pub async fn join(&self, room: &str) -> error::Result<()> {
		let inner = self.inner.read().await;

		let receiver = inner.app.room(room).await.subscribe();
		let room = room.to_string();

		inner.sender.send(Command::Join { room, receiver })?;

		Ok(())
	}
	pub async fn leave(&self, room: &str) -> error::Result<()> {
		let inner = self.inner.read().await;
		inner.sender.send(Command::Leave { room: room.to_string() })?;
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
