#[derive(serde::Deserialize)]
struct Inputs {
	a: i32,
	b: i32,
}

fn add(args: wspc::Params<Inputs>) -> i32 {
	log::info!("a: {}, b: {}", args.a, args.b);
	args.a + args.b
}

#[tokio::main]
async fn main() {
	simple_logger::init_with_level(log::Level::Info).unwrap();

	let (route, app) = wspc::App::build_route();

	app.on("add", add).await;

	let router = axum::Router::new().route("/ws", route);
	let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

	log::info!("Running in http://127.0.0.1:8080/ws");

	axum::serve(listener, router).await.unwrap();
}
