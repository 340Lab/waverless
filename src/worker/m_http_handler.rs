use crate::{
    general::network::{
        http_handler::{start_http_handler, HttpHandler, LocalReqIdAllocator},
        m_p2p::P2PModule,
    },
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use ws_derive::LogicalModule;

use super::m_executor::Executor;

#[derive(LogicalModule)]
pub struct WorkerHttpHandler {
    view: WorkerHttpHandlerView,
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
            local_req_id_allocator: LocalReqIdAllocator::new(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start as worker");

        let view = self.view.clone();
        Ok(vec![JoinHandleWrapper::from(tokio::spawn(async move {
            start_http_handler(view.inner).await;
        }))])
    }
}

logical_module_view_impl!(WorkerHttpHandlerView);
logical_module_view_impl!(WorkerHttpHandlerView, p2p, P2PModule);
logical_module_view_impl!(WorkerHttpHandlerView, http_handler, Box<dyn HttpHandler>);
logical_module_view_impl!(WorkerHttpHandlerView, executor, Option<Executor>);

#[async_trait]
impl HttpHandler for WorkerHttpHandler {
    async fn handle_request(&self, route: &str, http_text: String) -> Response {
        tracing::debug!("handle_request {}", route);
        if let Some(res) = self
            .view
            .executor()
            .handle_http_task(route, self.local_req_id_allocator.alloc(), http_text)
            // .execute_http_app(FunctionCtxBuilder::new(
            //     app.to_owned(),
            //     self.local_req_id_allocator.alloc(),
            //     self.request_handler_view.p2p().nodes_config.this.0,
            // ))
            .await
        {
            (StatusCode::OK, res).into_response()
        } else {
            StatusCode::OK.into_response()
        }
    }
    // async fn select_node(
    //     &self,
    //     _req: proto::sche::FnEventScheRequest,
    // ) -> proto::sche::FnEventScheResponse {
    //     panic!("worker should not select node");
    // }
}
