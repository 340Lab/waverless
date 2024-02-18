use crate::{
    general::network::http_handler::{start_http_handler, HttpHandler, LocalReqIdAllocator},
    result::WSResult,
    sys::{HttpHandlerView, LogicalModule, LogicalModuleNewArgs, WorkerHttpHandlerView},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use ws_derive::LogicalModule;

#[derive(LogicalModule)]
pub struct WorkerHttpHandler {
    view: WorkerHttpHandlerView,
    http_handler_view: HttpHandlerView,
    local_req_id_allocator: LocalReqIdAllocator,
}

#[async_trait]
impl LogicalModule for WorkerHttpHandler {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: WorkerHttpHandlerView::new(args.logical_modules_ref.clone()),
            http_handler_view: HttpHandlerView::new(args.logical_modules_ref.clone()),
            local_req_id_allocator: LocalReqIdAllocator::new(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start as worker");

        let view = self.http_handler_view.clone();
        Ok(vec![JoinHandleWrapper::from(tokio::spawn(async move {
            start_http_handler(view).await;
        }))])
    }
}

#[async_trait]
impl HttpHandler for WorkerHttpHandler {
    async fn handle_request(&self, app: &str) -> Response {
        self.view
            .executor()
            .handle_http_task(app, self.local_req_id_allocator.alloc(), "".to_owned())
            // .execute_http_app(FunctionCtxBuilder::new(
            //     app.to_owned(),
            //     self.local_req_id_allocator.alloc(),
            //     self.request_handler_view.p2p().nodes_config.this.0,
            // ))
            .await;

        StatusCode::OK.into_response()
    }
    // async fn select_node(
    //     &self,
    //     _req: proto::sche::FnEventScheRequest,
    // ) -> proto::sche::FnEventScheResponse {
    //     panic!("worker should not select node");
    // }
}
