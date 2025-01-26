use crate::general::app::m_executor::Executor;
use crate::{
    general::network::http_handler::{start_http_handler, HttpHandler},
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef},
    util::{JoinHandleWrapper, WithBind},
};
use async_trait::async_trait;
use axum::{response::Response, Router};
use parking_lot::Mutex;
use ws_derive::LogicalModule;

#[derive(LogicalModule)]
pub struct WorkerHttpHandler {
    view: WorkerHttpHandlerView,
    // local_req_id_allocator: LocalReqIdAllocator,
    building_router: Mutex<Option<Router>>, // valid when init
}

#[async_trait]
impl LogicalModule for WorkerHttpHandler {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: WorkerHttpHandlerView::new(args.logical_modules_ref.clone()),
            // local_req_id_allocator: LocalReqIdAllocator::new(),
            building_router: Mutex::new(Some(Router::new())),
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
logical_module_view_impl!(WorkerHttpHandlerView, executor, Executor);

#[async_trait]
impl HttpHandler for WorkerHttpHandler {
    fn building_router<'a>(&'a self) -> WithBind<'a, Router> {
        let guard = self.building_router.lock();
        WithBind::MutexGuardOpt(guard)
    }

    async fn handle_request(&self, _route: &str, _http_text: String) -> Response {
        // tracing::debug!("handle_request {}", route);
        unreachable!("handle_request deprecated");
    }
    // async fn select_node(
    //     &self,
    //     _req: proto::sche::FnEventScheRequest,
    // ) -> proto::sche::FnEventScheResponse {
    //     panic!("worker should not select node");
    // }
}
