async fn send_message(app: wspc::App, socket: wspc::Socket, msg: String) {
	if let Some(room) = socket.get_state::<String>().await {
		app.room(&room).await.emit("message_received", (msg,)).unwrap();
	} else {
		log::info!("Received message without a room: {}", msg);
	}
}

async fn join_room(socket: wspc::Socket, room: String) {
	if let Some(current_room) = socket.get_state::<String>().await {
		let _ = socket.leave(&current_room).await;
	}
	socket.join(&room).await.unwrap();
	socket.set_state(room).await;
}

#[tokio::main]
async fn main() {
	simple_logger::init_with_level(log::Level::Info).unwrap();

	let (route, app) = wspc::App::build_route();

	app.on("join_room", join_room).await;
	app.on("send_message", send_message).await;

	let router = axum::Router::new().route("/ws", route);
	let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

	log::info!("Running in http://127.0.0.1:8080/ws");

	axum::serve(listener, router).await.unwrap();
}
