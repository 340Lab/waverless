use super::m_p2p::P2PModule;
use crate::{
    apis::{
        self, AddServiceReq, AddServiceResp, ApiHandler, DeleteServiceReq, DeleteServiceResp,
        GetServiceListResp, RunServiceActionReq, RunServiceActionResp,
    },
    logical_module_view_impl,
    master::m_http_handler::MasterHttpHandler,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef},
    util::WithBind,
    worker::m_http_handler::WorkerHttpHandler,
};
use async_trait::async_trait;
use axum::{
    extract::Path,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use std::{net::SocketAddr, sync::OnceLock};
use std::{ops::Deref, sync::atomic::AtomicUsize};
use tower_http::cors::CorsLayer;
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
    fn building_router<'a>(&'a self) -> WithBind<'a, Router>;
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
// logical_module_view_impl!(HttpHandlerView, appmeta_manager, AppMetaManager);

pub struct ApiHandlerImpl;

#[async_trait]
impl ApiHandler for ApiHandlerImpl {
    async fn handle_add_service(&self, _req: AddServiceReq) -> AddServiceResp {
        let _view = http_handler_view();
        // request for selection info when name is empty
        // if req.service.name.len() == 0 {
        //     let nodes = view
        //         .p2p()
        //         .nodes_config
        //         .peers
        //         .iter()
        //         .map(|v| format!("{}", v.0))
        //         .chain(vec![format!(
        //             "{}",
        //             http_handler_view().p2p().nodes_config.this_node()
        //         )])
        //         .collect::<Vec<_>>();
        //     return AddServiceResp::Template { nodes };
        // }

        AddServiceResp::Fail {
            msg: "not implemented".to_owned(),
        }
        // if view.p2p().nodes_config
        // view.appmeta_manager().add_service(req).await
    }

    async fn handle_delete_service(&self, _req: DeleteServiceReq) -> DeleteServiceResp {
        DeleteServiceResp::Fail {
            msg: "not implemented".to_owned(),
        }
    }

    async fn handle_get_service_list(&self) -> GetServiceListResp {
        // let view = http_handler_view();
        // GetServiceListResp::Exist {
        //     services: view.appmeta_manager().get_app_meta_basicinfo_list(),
        // }
        GetServiceListResp::Exist { services: vec![] }
    }

    async fn handle_run_service_action(&self, _req: RunServiceActionReq) -> RunServiceActionResp {
        // http_handler_view()
        //     .appmeta_manager()
        //     .run_service_action(req)
        //     .await
        RunServiceActionResp::Fail {
            msg: "not implemented".to_owned(),
        }
    }
}

lazy_static::lazy_static!(
    static ref HTTP_HANDLER_VIEW: OnceLock<HttpHandlerView> = OnceLock::new();
);

fn http_handler_view() -> &'static HttpHandlerView {
    HTTP_HANDLER_VIEW.get().unwrap()
}

pub async fn start_http_handler(modsref: LogicalModulesRef) {
    let view: HttpHandlerView = HttpHandlerView::new(modsref);
    let view_clone = view.clone();
    let _ = HTTP_HANDLER_VIEW.get_or_init(move || view_clone);

    let addr = SocketAddr::new(
        "0.0.0.0".parse().unwrap(),
        view.p2p().nodes_config.this.1.addr.port() + 1,
    );
    tracing::info!("http start on {}", addr);
    let app = view
        .http_handler()
        .building_router()
        .option_mut()
        .take()
        .unwrap();
    // prometheus metrics
    // .route("metrics")
    //
    let app = if view.p2p().nodes_config.this_node() == view.p2p().nodes_config.get_master_node() {
        apis::add_routers(app)
    } else {
        app
    };

    let app = app
        // .route("/:app/:fn", post(handler2))
        .route("/:route", post(handler))
        .layer(CorsLayer::permissive());

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    tracing::info!("http end on {}", addr);
}

// async fn handler2(Path((app, func)): Path<(String, String)>, body: String) -> impl IntoResponse {
//     http_handler_view()
//         .http_handler()
//         .handle_request(&format!("{app}/{func}"), body)
//         .await
// }

async fn handler(route: Path<String>, body: String) -> impl IntoResponse {
    http_handler_view()
        .http_handler()
        .handle_request(route.as_str(), body)
        .await
}

pub struct HttpHandlerDispatch(Box<dyn HttpHandler>);

impl Deref for HttpHandlerDispatch {
    type Target = Box<dyn HttpHandler>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl HttpHandlerDispatch {
    pub fn new(arg: LogicalModuleNewArgs) -> Self {
        if arg.nodes_config.this.1.is_master() {
            Self(Box::new(MasterHttpHandler::new(arg)))
        } else {
            Self(Box::new(WorkerHttpHandler::new(arg)))
        }
    }
}
