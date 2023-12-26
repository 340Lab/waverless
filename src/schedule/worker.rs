use crate::{
    network::proto,
    result::WSResult,
    schedule::http_handler::start_http_handler,
    sys::{LogicalModule, LogicalModuleNewArgs, RequestHandlerView},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use ws_derive::LogicalModule;

use super::http_handler::ScheNode;

#[derive(LogicalModule)]
pub struct ScheWorker {
    request_handler_view: RequestHandlerView,
}

#[async_trait]
impl LogicalModule for ScheWorker {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            request_handler_view: RequestHandlerView::new(args.logical_modules_ref.clone()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start as worker");

        let view = self.request_handler_view.clone();
        Ok(vec![JoinHandleWrapper::from(tokio::spawn(async move {
            start_http_handler(view).await;
        }))])
    }
}

#[async_trait]
impl ScheNode for ScheWorker {
    async fn handle_request(&self, req_fn: &str) -> Response {
        self.request_handler_view
            .executor()
            .execute_http_app(req_fn)
            .await;

        StatusCode::OK.into_response()
    }
    async fn select_node(
        &self,
        _req: proto::sche::FnEventScheRequest,
    ) -> proto::sche::FnEventScheResponse {
        panic!("worker should not select node");
    }
}
