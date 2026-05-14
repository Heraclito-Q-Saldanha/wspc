fn send_message(app: wspc::App, socket: wspc::Socket, msg: String) {
	if let Some(room) = socket.get_state::<String>() {
		app.room(&room).emit("message_received", (msg,)).unwrap();
	} else {
		log::info!("Received message without a room: {}", msg);
	}
}

fn join_room(socket: wspc::Socket, room: String) {
	if let Some(current_room) = socket.get_state::<String>() {
		let _ = socket.leave(&current_room);
	}
	socket.join(&room).unwrap();
	socket.set_state(room);
}

#[tokio::main]
async fn main() {
	simple_logger::init_with_level(log::Level::Info).unwrap();

	let (route, app) = wspc::App::build_route();

	app.on("join_room", join_room);
	app.on("send_message", send_message);

	let router = axum::Router::new().route("/ws", route);
	let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

	log::info!("Running in http://127.0.0.1:8080/ws");

	axum::serve(listener, router).await.unwrap();
}
