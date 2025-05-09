use async_trait::async_trait;
use axum::{
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Redirect, Response},
    Router,
};
use parking_lot::Mutex;
use prometheus_client::encoding::text::encode;
use ws_derive::LogicalModule;
// use

use crate::{
    general::{
        app::AppMetaManager,
        network::{
            http_handler::{self, HttpHandler},
            m_p2p::P2PModule,
        },
    },
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef},
    util::{JoinHandleWrapper, WithBind},
};

use super::{m_master::Master, m_metric_observor::MetricObservor};

logical_module_view_impl!(MasterHttpHandlerView);
logical_module_view_impl!(MasterHttpHandlerView, p2p, P2PModule);
logical_module_view_impl!(MasterHttpHandlerView, master, Option<Master>);
logical_module_view_impl!(
    MasterHttpHandlerView,
    metric_observor,
    Option<MetricObservor>
);
logical_module_view_impl!(MasterHttpHandlerView, appmeta_manager, AppMetaManager);

#[derive(LogicalModule)]
pub struct MasterHttpHandler {
    // local_req_id_allocator: LocalReqIdAllocator,
    // view: ScheMasterView,
    view: MasterHttpHandlerView,
    building_router: Mutex<Option<Router>>, // valid when init
}

#[async_trait]
impl LogicalModule for MasterHttpHandler {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: MasterHttpHandlerView::new(args.logical_modules_ref.clone()),
            building_router: Mutex::new(Some(Router::new())),
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

// fn construct_target_path(node_config: &NodeConfig, sub_api: &str) -> String {
//     if let Some(d) = node_config.get_http_domain() {
//         format!("{}/{}", d, sub_api)
//     } else {
//         let mut addr = node_config.addr;
//         addr.set_port(addr.port() + 1);
//         format!("http://{}/{}", addr, sub_api)
//     }
// }

#[async_trait]
impl HttpHandler for MasterHttpHandler {
    fn building_router<'a>(&'a self) -> WithBind<'a, Router> {
        let guard = self.building_router.lock();
        WithBind::MutexGuardOpt(guard)
    }
    // fn alloc_local_req_id(&self) -> ReqId {
    //     self.local_req_id_allocator.alloc()
    // }
    async fn handle_request(&self, app: &str, _http_text: String) -> Response {
        tracing::debug!("master handle_request {}", app);
        if app == "metrics" {
            return self.handle_prometheus();
        }

        // let view = self.view.clone();
        // if !view.p2p().nodes_config.this.1.is_master() {
        //     tracing::debug!("this is_master");
        //     match self.view.appmeta_manager().app_available(app).await {
        //         Ok(true) => {}
        //         Ok(false) => {
        //             return (StatusCode::NOT_FOUND, "app not found").into_response();
        //         }
        //         Err(e) => {
        //             return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        //         }
        //     }
        // }

        // check app is available
        // match self.view.appmeta_manager().app_available(app).await {
        //     Ok(true) => {}
        //     Ok(false) => {
        //         return (StatusCode::NOT_FOUND, "app not found").into_response();
        //     }
        //     Err(e) => {
        //         return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        //     }
        // }

        // 选择节点
        let node = self.view.master().handle_http_schedule(app).await;
        tracing::debug!("scheduled node is {:?}", node);

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
        let target_node = self
            .view
            .p2p()
            .nodes_config
            .peers
            .get(&(node as u32))
            .unwrap();

        tracing::debug!("scheduled target_node is {:?}", target_node);
        let url = target_node.http_url();
        let url = if url.ends_with('/') {
            // 如果是，去除末尾的斜杠
            &url[..url.len() - 1]
        } else {
            // 否则，返回原URL
            &url
        };
        let target_path = format!("{}/{}", url, app);

        // target_node.set_port(target_node.port() + 1);
        tracing::debug!("redirect to {}", target_path);
        Redirect::temporary(&target_path).into_response()
        // }
    }
    // async fn select_node(
    //     &self,
    //     _req: proto::sche::FnEventScheRequest,
    // ) -> proto::sche::FnEventScheResponse {
    //     proto::sche::FnEventScheResponse { target_node: 2 }
    // }
}
