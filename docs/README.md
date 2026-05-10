# wspc

`wspc` is a lightweight Rust library for building callback-driven JSON-RPC APIs over WebSockets with Axum.

### Features
- Automatic handler argument mapping
- Per-app and per-socket state
- Room broadcasting
- Easy Axum integration

### Example

```rust
fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[tokio::main]
async fn main() {
    let (route, app) = wspc::App::build_route();

    app.on("add", add).await;

    let router = axum::Router::new().route("/ws", route);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

    axum::serve(listener, router).await.unwrap();
}
```


This project is licensed under the [MIT License](../LICENSE)