use crate::*;

use std::sync;

use axum::extract;
use axum::routing;

use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio::sync::mpsc;

use tokio_stream::StreamMap;
use tokio_stream::wrappers;

#[derive(Default)]
struct InnerApp {
	#[cfg(feature = "state")]
	state: TypeMap,
	callbacks: dashmap::DashMap<String, callback::Callback>,
	rooms: dashmap::DashMap<String, Room>,
	sockets: dashmap::DashMap<uuid::Uuid, Socket>,
}

#[derive(Clone, Default)]
pub struct App {
	inner: sync::Arc<InnerApp>,
}

impl App {
	#[inline(always)]
	pub fn new() -> Self {
		Self::default()
	}
	pub fn on<Args: Send + Sync + 'static, Kind: Send + Sync + 'static, F: callback::FunctionCall<Args, Kind> + Send + Sync + 'static>(&self, event: &str, handler: F) {
		let event = event.to_string();
		let handler = callback::Callback::new::<Args, Kind, F>(handler);

		self.inner.callbacks.insert(event, handler);
	}
	pub fn off(&self, event: &str) {
		self.inner.callbacks.remove(event);
	}
	pub fn route<T: Clone + Send + Sync + 'static>(&self) -> axum::routing::MethodRouter<T> {
		let app = self.clone();

		routing::any(move |ws: extract::WebSocketUpgrade| async move {
			ws.on_upgrade(move |ws| async move {
				socket_handler(app, ws).await;
			})
		})
	}
	pub fn build_route<T: Clone + Send + Sync + 'static>() -> (axum::routing::MethodRouter<T>, App) {
		let app = Self::new();
		let route = app.route();

		(route, app)
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
	pub fn room<T: ToString>(&self, room: T) -> Room {
		let room = room.to_string();

		self.inner.rooms.entry(room).or_insert_with(|| Room::new()).clone()
	}
	pub fn socket(&self, socket_id: uuid::Uuid) -> Option<Socket> {
		self.inner.sockets.get(&socket_id).map(|entry| entry.value().clone())
	}
	pub fn sockets(&self) -> Vec<Socket> {
		self.inner.sockets.iter().map(|entry| entry.value().clone()).collect()
	}
	pub(crate) fn get_callback(&self, event: &str) -> Option<callback::Callback> {
		match self.inner.callbacks.get(event) {
			Some(callback) => Some(callback.clone()),
			None => None,
		}
	}
	pub(crate) fn insert_socket(&self, socket: Socket) {
		self.inner.sockets.insert(socket.id(), socket);
	}
	pub(crate) fn remove_socket(&self, socket_id: uuid::Uuid) {
		self.inner.sockets.remove(&socket_id);
	}
}

async fn socket_handler(app: App, ws: extract::ws::WebSocket) {
	let (command_sender, command_receiver) = mpsc::unbounded_channel();
	let (ws_sender, mut ws_receiver) = ws.split();

	let socket = Socket::new(command_sender);

	app.insert_socket(socket.clone());

	{
		let app = app.clone();
		let socket = socket.clone();

		tokio::spawn(command_handler(app, socket, command_receiver, ws_sender));
	}

	if let Some(callback) = app.get_callback("connect") {
		let socket = socket.clone();
		let app = app.clone();
		let args = RpcParams::Null;

		let context = callback::CallContext { socket, app, args };

		if let Err(err) = callback.call(context).await {
			log::error!("Failed to execute connect callback: {:?}", err);
		}
	}

	loop {
		let Some(Ok(msg)) = ws_receiver.next().await else {
			break;
		};

		let extract::ws::Message::Text(msg) = msg else {
			continue;
		};

		match serde_json::from_str(&msg) {
			Ok(Message::Single(req)) => {
				let Some(response) = process_request(&app, &socket, req).await else {
					continue;
				};

				let payload = serde_json::to_string(&response).unwrap();

				if let Err(err) = socket.write(extract::ws::Message::Text(payload.into())) {
					log::error!("Failed to send response: {err:?}");
					break;
				}
			}
			Ok(Message::Batch(reqs)) => {
				let values = futures_util::future::join_all(reqs.into_iter().map(|req| process_request(&app, &socket, req))).await;
				let response = Message::Batch(values.into_iter().flatten().collect());

				let payload = serde_json::to_string(&response).unwrap();

				if let Err(err) = socket.write(extract::ws::Message::Text(payload.into())) {
					log::error!("Failed to send batch response: {err:?}");
					break;
				}
			}
			Err(err) => {
				log::error!("Failed to parse message: {}", err);

				let response = RpcResponse::parse_error(Id::Null, Value::Null);

				let payload = serde_json::to_string(&response).unwrap();

				if let Err(err) = socket.write(extract::ws::Message::Text(payload.into())) {
					log::error!("Failed to send parse error response: {err:?}");
					break;
				}
			}
		};
	}

	if let Some(callback) = app.get_callback("disconnect") {
		let socket = socket.clone();
		let app = app.clone();
		let args = RpcParams::Null;

		let context = callback::CallContext { socket, app, args };

		if let Err(err) = callback.call(context).await {
			log::error!("Failed to execute disconnect callback: {:?}", err);
		}
	}

	let _ = socket.close();

	app.remove_socket(socket.id());
}

async fn command_handler(app: App, socket: Socket, mut command_receiver: mpsc::UnboundedReceiver<Command>, mut ws_sender: futures_util::stream::SplitSink<extract::ws::WebSocket, extract::ws::Message>) {
	let mut rooms = StreamMap::new();

	loop {
		tokio::select! {
			command = command_receiver.recv() => {
				match command {
					Some(Command::JoinRoom { name, ack }) => {
						let room = app.room(&name);
						let stream = room.broadcast_stream();

						room.insert_socket(socket.clone());
						rooms.insert(name, stream);
						let _ = ack.send(());
					}
					Some(Command::LeaveRoom { name, ack }) => {
						{
							let room = app.room(&name);
							room.remove_socket(socket.id());
						}
						rooms.remove(&name);
						let _ = ack.send(());
					}
					Some(Command::SendMessage(msg)) => {
						if let Err(err) = ws_sender.send(msg).await {
							log::error!("Failed to send message to WebSocket: {:?}", err);
							continue;
						}
					}
					Some(Command::Close { ack }) => {
						let _ = ws_sender.close().await;
						let _ = ack.send(());
						break;
					}
					None => break,
				}
			}
			message = rooms.next(), if !rooms.is_empty() => {
				match message {
					Some((room, Ok(message))) => {
						let payload = serde_json::to_string(&message).unwrap();

						if let Err(err) = ws_sender.send(extract::ws::Message::Text(payload.into())).await {
							log::error!("Failed to send message to WebSocket in room {}: {:?}", room, err);
							continue;
						}
					}
					Some((room, Err(wrappers::errors::BroadcastStreamRecvError::Lagged(skipped)))) => {
						log::warn!("Socket lagged in room {} and skipped {} messages", room, skipped);
					}
					None => break,
				}
			}
		}
	}

	for room_name in rooms.keys() {
		let room = app.room(room_name);
		room.remove_socket(socket.id());
	}
}

async fn process_request(app: &App, socket: &Socket, req: RpcRequest) -> Option<RpcResponse> {
	if req.method == "connect" || req.method == "disconnect" {
		log::warn!("Method \"{}\" is reserved for internal lifecycle events", req.method);

		if req.id != Id::Null {
			return Some(RpcResponse::internal_error(req.id, Value::String("Method is reserved for internal lifecycle events".to_string())));
		}

		return None;
	}
	let Some(callback) = app.get_callback(&req.method) else {
		log::warn!("No callback found for method: {}", req.method);

		if req.id != Id::Null {
			return Some(RpcResponse::method_not_found(req.id, Value::Null));
		}

		return None;
	};

	let context = callback::CallContext {
		socket: socket.clone(),
		app: app.clone(),
		args: req.params,
	};

	let result = callback.call(context).await;

	if req.id == Id::Null {
		return None;
	}

	match result {
		Ok(result) => Some(RpcResponse::Success {
			jsonrpc: Some(Version::V2),
			id: req.id,
			result,
		}),
		Err(error) => Some(RpcResponse::Error { jsonrpc: Some(Version::V2), id: req.id, error }),
	}
}
