#[derive(wspc::IntoErrorResponse)]
enum MyError {
	#[response(message = "Value is even", code = 7)]
	IsEven,
	#[response(message = "Value is not even", code = 42)]
	NotIsEven,
}

fn err(value: i32) -> Result<(), MyError> {
	match value & 1 {
		0 => Err(MyError::IsEven),
		_ => Err(MyError::NotIsEven),
	}
}

#[tokio::main]
async fn main() {
	simple_logger::init_with_level(log::Level::Info).unwrap();

	let (route, app) = wspc::App::build_route();

	app.on("err", err);

	let router = axum::Router::new().route("/ws", route);
	let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

	log::info!("Running in http://127.0.0.1:8080/ws");

	axum::serve(listener, router).await.unwrap();
}
