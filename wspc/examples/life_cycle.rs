async fn connect(socket: wspc::Socket) {
	log::info!("Client {} connected", socket.id().await);
}

async fn disconnect(socket: wspc::Socket) {
	log::info!("Client {} disconnected", socket.id().await);
}

#[tokio::main]
async fn main() {
	simple_logger::init_with_level(log::Level::Info).unwrap();

	let (route, app) = wspc::App::build_route();

	app.on("connect", connect).await;
	app.on("disconnect", disconnect).await;

	let router = axum::Router::new().route("/ws", route);
	let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

	log::info!("Running in http://127.0.0.1:8080/ws");

	axum::serve(listener, router).await.unwrap();
}
