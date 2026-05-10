use crate::*;

use tokio::sync::broadcast;

#[derive(Clone)]
pub struct Room {
	sender: broadcast::Sender<RpcRequest>,
}

impl Room {
	pub(crate) fn new(sender: broadcast::Sender<RpcRequest>) -> Self {
		Self { sender }
	}
	pub(crate) fn subscribe(&self) -> broadcast::Receiver<RpcRequest> {
		self.sender.subscribe()
	}
	pub fn emit<T: serde::Serialize>(&self, method: &str, args: T) -> error::Result<()> {
		let params = RpcParams::try_from(serde_json::to_value(args)?)?;
		let message = RpcRequest::new(Id::Null, &method, params);
		let _ = self.sender.send(message);
		Ok(())
	}
}
