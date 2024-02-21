use async_trait::async_trait;
use axum::{
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Redirect, Response},
};
use prometheus_client::encoding::text::encode;
use ws_derive::LogicalModule;
// use

use crate::{
    general::network::{
        http_handler::{self, HttpHandler},
        m_p2p::P2PModule,
    },
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef},
    util::JoinHandleWrapper,
};

use super::{m_master::Master, m_metric_observor::MetricObservor};

logical_module_view_impl!(MasterHttpHandlerView);
logical_module_view_impl!(MasterHttpHandlerView, p2p, P2PModule);
logical_module_view_impl!(MasterHttpHandlerView, http_handler, Box<dyn HttpHandler>);
logical_module_view_impl!(MasterHttpHandlerView, master, Option<Master>);
logical_module_view_impl!(
    MasterHttpHandlerView,
    metric_observor,
    Option<MetricObservor>
);

#[derive(LogicalModule)]
pub struct MasterHttpHandler {
    // local_req_id_allocator: LocalReqIdAllocator,
    // view: ScheMasterView,
    view: MasterHttpHandlerView,
}

#[async_trait]
impl LogicalModule for MasterHttpHandler {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: MasterHttpHandlerView::new(args.logical_modules_ref.clone()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start as master");

        let view = self.view.clone();
        Ok(vec![JoinHandleWrapper::from(tokio::spawn(async move {
            http_handler::start_http_handler(view.inner).await;
        }))])
    }
}

impl MasterHttpHandler {
    fn handle_prometheus(&self) -> Response {
        let mut body = String::new();
        tracing::debug!("handle_prometheus");
        encode(&mut body, &self.view.metric_observor().registry).unwrap();
        let mut resp = (StatusCode::OK, body).into_response();
        // hyper::header::CONTENT_TYPE,
        // "application/openmetrics-text; version=1.0.0; charset=utf-8",
        let _ = resp.headers_mut().insert(
            "content-type",
            HeaderValue::from_static("application/openmetrics-text; version=1.0.0; charset=utf-8"),
        );
        resp
    }
}

#[async_trait]
impl HttpHandler for MasterHttpHandler {
    // fn alloc_local_req_id(&self) -> ReqId {
    //     self.local_req_id_allocator.alloc()
    // }
    async fn handle_request(&self, app: &str, _http_text: String) -> Response {
        tracing::debug!("handle_request {}", app);
        if app == "metrics" {
            return self.handle_prometheus();
        }
        // 选择节点
        let node = self.view.master().handle_http_schedule(app).await;

        // if self.view.p2p().nodes_config.this.0 == node {
        //     // println!("run");
        //     // 本节点执行
        //     self.view
        //         .executor()
        //         .handle_http_task(
        //             app.to_owned(), //     FunctionCtxBuilder::new(
        //                             //     app.to_owned(),
        //                             //     self.local_req_id_allocator.alloc(),
        //                             //     node,
        //                             // )
        //         )
        //         .await;
        //     // println!("end run");

        //     StatusCode::OK.into_response()
        // } else {
        // 转发
        let mut target_node = self
            .view
            .p2p()
            .nodes_config
            .peers
            .get(&(node as u32))
            .unwrap()
            .addr;
        target_node.set_port(target_node.port() + 1);
        tracing::debug!(
            "redirect to http://hanbaoaaa.xyz/waverless_api{}/{}",
            target_node,
            app
        );
        Redirect::temporary(&*format!(
            "http://hanbaoaaa.xyz/waverless_api{}/{}",
            node, app
        ))
        .into_response()
        // }
    }
    // async fn select_node(
    //     &self,
    //     _req: proto::sche::FnEventScheRequest,
    // ) -> proto::sche::FnEventScheResponse {
    //     proto::sche::FnEventScheResponse { target_node: 2 }
    // }
}
