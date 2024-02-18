use crate::sys::{HttpHandlerView, LogicalModule};
use async_trait::async_trait;
use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;

pub type ReqId = usize;
pub struct LocalReqIdAllocator {
    id: AtomicUsize,
}
impl LocalReqIdAllocator {
    pub fn new() -> Self {
        Self {
            id: AtomicUsize::new(0),
        }
    }
    pub fn alloc(&self) -> ReqId {
        self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait]
pub trait HttpHandler: LogicalModule {
    // fn alloc_local_req_id(&self) -> ReqId;
    async fn handle_request(&self, req_fn: &str) -> Response;
    // async fn select_node(
    //     &self,
    //     req: proto::sche::FnEventScheRequest,
    // ) -> proto::sche::FnEventScheResponse;
}

pub async fn start_http_handler(request_handler_view: HttpHandlerView) {
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
    view: State<HttpHandlerView>, /* _post: String*/
) -> impl IntoResponse {
    view.http_handler().handle_request(route.as_str()).await
}
