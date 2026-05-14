fn get_state(app: wspc::App) -> String {
	app.get_state::<String>().unwrap()
}

#[tokio::main]
async fn main() {
	simple_logger::init_with_level(log::Level::Info).unwrap();

	let (route, app) = wspc::App::build_route();

	app.set_state("my awesome state".to_string());
	app.on("get_state", get_state);

	let router = axum::Router::new().route("/ws", route);
	let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

	log::info!("Running in http://127.0.0.1:8080/ws");

	axum::serve(listener, router).await.unwrap();
}
