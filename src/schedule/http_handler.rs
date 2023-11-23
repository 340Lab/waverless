use std::net::SocketAddr;

use async_trait::async_trait;
use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};

use crate::sys::{LogicalModule, RequestHandlerView};

#[async_trait]
pub trait RequestHandler: LogicalModule {
    async fn handle_request(&self, req_fn: &str) -> Response;
}

pub async fn start_http_handler(request_handler_view: RequestHandlerView) {
    let addr = SocketAddr::new(
        "0.0.0.0".parse().unwrap(),
        request_handler_view.p2p().nodes_config.this.1.addr.port() + 1,
    );
    tracing::info!("http start on {}", addr);
    let app = Router::new()
        .route("/:route", get(handler))
        .with_state(request_handler_view);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    tracing::info!("http end on {}", addr);
}

async fn handler(
    route: Path<String>,
    view: State<RequestHandlerView>, /* _post: String*/
) -> impl IntoResponse {
    view.0
        .request_handler()
        .handle_request(route.as_str())
        .await
}
