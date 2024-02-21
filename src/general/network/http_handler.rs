use crate::{
    logical_module_view_impl,
    sys::{LogicalModule, LogicalModulesRef},
};
use async_trait::async_trait;
use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use tower_http::cors::CorsLayer;

use super::m_p2p::P2PModule;
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
    async fn handle_request(&self, req_fn: &str, http_text: String) -> Response;
    // async fn select_node(
    //     &self,
    //     req: proto::sche::FnEventScheRequest,
    // ) -> proto::sche::FnEventScheResponse;
}

logical_module_view_impl!(HttpHandlerView);
logical_module_view_impl!(HttpHandlerView, p2p, P2PModule);
logical_module_view_impl!(HttpHandlerView, http_handler, Box<dyn HttpHandler>);

pub async fn start_http_handler(modsref: LogicalModulesRef) {
    let view = HttpHandlerView::new(modsref);
    let addr = SocketAddr::new(
        "0.0.0.0".parse().unwrap(),
        view.p2p().nodes_config.this.1.addr.port() + 1,
    );
    tracing::info!("http start on {}", addr);
    let app = Router::new()
        // prometheus metrics
        // .route("metrics")
        .route("/:app/:fn", post(handler2))
        .route("/:route", post(handler))
        .layer(CorsLayer::permissive())
        .with_state(view);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    tracing::info!("http end on {}", addr);
}

async fn handler2(
    Path((app, func)): Path<(String, String)>,

    // Json(json): Json<String>,
    view: State<HttpHandlerView>, /* _post: String*/

    body: String,
) -> impl IntoResponse {
    view.http_handler()
        .handle_request(&format!("{app}/{func}"), body)
        .await
}

async fn handler(
    route: Path<String>,

    // Json(json): Json<String>,
    view: State<HttpHandlerView>, /* _post: String*/

    body: String,
) -> impl IntoResponse {
    view.http_handler()
        .handle_request(route.as_str(), body)
        .await
}
