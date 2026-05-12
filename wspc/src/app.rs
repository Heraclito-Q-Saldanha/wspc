use crate::*;

use std::collections;
use std::sync;

use axum::extract;
use axum::routing;

use futures_util::StreamExt;

use tokio::sync::broadcast;
use tokio::sync::mpsc;

use tokio_stream::StreamMap;
use tokio_stream::wrappers;

#[derive(Default)]
struct InnerApp {
	state: state::TypeMap![Send + Sync],
	callbacks: collections::HashMap<String, callback::Callback>,
	rooms: collections::HashMap<String, broadcast::Sender<RpcRequest>>,
}

#[derive(Clone, Default)]
pub struct App {
	inner: sync::Arc<tokio::sync::RwLock<InnerApp>>,
}

impl App {
	pub async fn on<Args: Send + Sync + 'static, Kind: Send + Sync + 'static, F: callback::FunctionCall<Args, Kind> + Send + Sync + 'static>(&self, event: &str, handler: F) {
		let mut inner = self.inner.write().await;

		let event = event.to_string();
		let handler = callback::Callback::new::<Args, Kind, F>(handler);

		inner.callbacks.insert(event, handler);
	}

	pub async fn off(&self, event: &str) {
		let mut inner = self.inner.write().await;
		inner.callbacks.remove(event);
	}

	#[inline(always)]
	pub fn new() -> Self {
		Self::default()
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

	pub async fn room<T: ToString>(&self, room: T) -> Room {
		let mut inner = self.inner.write().await;
		let room = room.to_string();
		let sender = inner.rooms.entry(room).or_insert_with(|| broadcast::channel(1024).0).clone();
		Room::new(sender)
	}

	async fn get_callback(&self, event: &str) -> Option<callback::Callback> {
		let inner = self.inner.read().await;
		inner.callbacks.get(event).cloned()
	}
}

async fn socket_handler(app: App, mut ws: extract::ws::WebSocket) {
	let mut rooms = StreamMap::new();

	let (sender, mut receiver) = mpsc::unbounded_channel();

	let socket = Socket::new(app.clone(), sender);

	if let Some(callback) = app.get_callback("connect").await {
		let context = callback::CallContext {
			socket: socket.clone(),
			app: app.clone(),
			args: RpcParams::Null,
		};

		if let Err(err) = callback.call(context).await {
			log::error!("Failed to execute connect callback: {:?}", err);
		}
	}

	loop {
		tokio::select! {
			command = receiver.recv() => {
				match command {
					Some(Command::Join { room, receiver }) => {
						rooms.insert(room, wrappers::BroadcastStream::new(receiver));
					}
					Some(Command::Leave { room }) => {
						rooms.remove(&room);
					}
					Some(Command::Message(msg)) => {
						if let Err(err) = ws.send(msg).await {
							log::error!("Failed to send message: {:?}", err);
							break;
						}
					}
					None => break,
				}
			}
			message = rooms.next(), if !rooms.is_empty() => {
				let Some((room, message)) = message else {
					continue;
				};
				match message {
					Ok(message) => {
						let payload = serde_json::to_string(&message).unwrap();

						if let Err(err) = ws.send(extract::ws::Message::Text(payload.into())).await {
							log::error!("Failed to forward room message for {}: {:?}", room, err);
							break;
						}
					}
					Err(wrappers::errors::BroadcastStreamRecvError::Lagged(skipped)) => {
						log::warn!("Socket lagged in room {} and skipped {} messages", room, skipped);
					}
				}
			}
			message = ws.next() => {
				let Some(Ok(msg)) = message else {
					break;
				};
				let extract::ws::Message::Text(msg) = msg else {
					continue;
				};
				let msg: Message<RpcRequest> = match serde_json::from_str(&msg){
					Ok(msg) => msg,
					Err(err) => {
						let response = RpcResponse::parse_error(Id::Null, Value::Null);

						if let Ok(text) = serde_json::to_string(&response)  {
							let _ = ws.send(extract::ws::Message::Text(text.into())).await;
						};

						log::error!("Failed to parse message: {}", err);
						continue;
					}
				};

				match msg {
					Message::Single(req) => {
						let Some(response) = process_request(&app, &socket, req).await else {
							continue;
						};

						match serde_json::to_string(&response) {
							Ok(playload) => {
								if let Err(err) = ws.send(extract::ws::Message::Text(playload.into())).await {
									log::error!("Failed to send response: {err:?}");
									break;
								}
							},
							Err(err) => {
								log::error!("Failed to serialize response: {err:?}");
								continue;
							}
						};


					},
					Message::Batch(reqs) => {
						let values = futures_util::future::join_all(reqs.into_iter().map(|req| process_request(&app, &socket, req))).await;
						let response = Message::Batch(values.into_iter().flatten().collect());

						match serde_json::to_string(&response) {
							Ok(payload) => {
								if let Err(err) = ws.send(extract::ws::Message::Text(payload.into())).await {
									log::error!("Failed to send batch response: {err:?}");
									break;
								}
							},
							Err(err) => log::error!("Failed to serialize batch response: {err:?}")
						};
					}
				}
			}
		}
	}

	if let Some(callback) = app.get_callback("disconnect").await {
		let context = callback::CallContext { socket, app, args: RpcParams::Null };

		if let Err(err) = callback.call(context).await {
			log::error!("Failed to execute disconnect callback: {:?}", err);
		}
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
	let Some(callback) = app.get_callback(&req.method).await else {
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
