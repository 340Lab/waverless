use std::{collections::hash_map::DefaultHasher, hash::Hasher};

use async_trait::async_trait;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
};
use ws_derive::LogicalModule;

use super::{executor::Executor, http_handler::RequestHandler};
use crate::{
    result::WSResult,
    schedule::http_handler::start_http_handler,
    sys::{LogicalModule, LogicalModuleNewArgs, NodeID, RequestHandlerView},
    util::JoinHandleWrapper,
};

#[derive(LogicalModule)]
pub struct ScheMaster {
    executor: Executor,
    // each_fn_caching: HashMap<String, HashSet<NodeId>>,
    node_selector: Box<dyn NodeSelector>,
    request_handler_view: RequestHandlerView,
    // view: ScheMasterView,
}

trait NodeSelector: Send + Sync + 'static {
    fn select_node(&self, all_node_cnt: usize, fn_name: &str) -> NodeID;
}

struct HashNodeSelector;

impl NodeSelector for HashNodeSelector {
    fn select_node(&self, all_node_cnt: usize, fn_name: &str) -> NodeID {
        // hash
        let mut hasher = DefaultHasher::new();
        hasher.write(fn_name.as_bytes());
        let n = hasher.finish();

        (n % all_node_cnt as u64 + 1) as NodeID
    }
}

#[async_trait]
impl LogicalModule for ScheMaster {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            executor: Executor::new(),
            // each_fn_caching: HashMap::new(),
            node_selector: Box::new(HashNodeSelector),
            request_handler_view: RequestHandlerView::new(args.logical_modules_ref.clone()),
            // view: ,
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
impl RequestHandler for ScheMaster {
    async fn handle_request(&self, req_fn: &str) -> Response {
        // 选择节点
        // let node = self.node_selector.select_node(
        //     self.request_handler_view.p2p().nodes_config.peers.len() + 1,
        //     req_fn,
        // );
        let node = 1;
        // tracing::info!("select node: {} for {}", node, req_fn);
        // tracing::info!(
        //     "this {} {}",
        //     self.request_handler_view.p2p().nodes_config.this.0,
        //     self.request_handler_view.p2p().nodes_config.this.0 == node
        // );
        // let node = 1;
        if self.request_handler_view.p2p().nodes_config.this.0 == node {
            // println!("run");
            // 本节点执行
            self.executor.execute(req_fn).await;
            // println!("end run");

            StatusCode::OK.into_response()
        } else {
            // 转发
            let mut target_node = self
                .request_handler_view
                .p2p()
                .nodes_config
                .peers
                .get(&(node as u32))
                .unwrap()
                .addr;
            target_node.set_port(target_node.port() + 1);
            Redirect::to(&*format!("http://{}/{}", target_node, req_fn)).into_response()
        }
    }
}
