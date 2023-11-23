use async_trait::async_trait;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use ws_derive::LogicalModule;

use crate::{
    result::WSResult,
    schedule::http_handler::start_http_handler,
    sys::{LogicalModule, LogicalModuleNewArgs, RequestHandlerView},
    util::JoinHandleWrapper,
};

use super::{executor::Executor, http_handler::RequestHandler};

#[derive(LogicalModule)]
pub struct ScheWorker {
    executor: Executor,
    request_handler_view: RequestHandlerView,
}

#[async_trait]
impl LogicalModule for ScheWorker {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            executor: Executor::new(),
            request_handler_view: RequestHandlerView::new(args.logical_modules_ref.clone()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let view = self.request_handler_view.clone();
        Ok(vec![JoinHandleWrapper::from(tokio::spawn(async move {
            start_http_handler(view).await;
        }))])
    }
}

#[async_trait]
impl RequestHandler for ScheWorker {
    async fn handle_request(&self, req_fn: &str) -> Response {
        self.executor.execute(req_fn).await;

        StatusCode::OK.into_response()
    }
}
