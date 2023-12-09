use std::{collections::hash_map::DefaultHasher, hash::Hasher};

use async_trait::async_trait;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
};
use ws_derive::LogicalModule;
// use

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

trait NodeWeighteFetcher: Send + Sync + 'static {
    // NOTE: get weight return node weight
    // larger is better
    fn get_node_weight(&self, id: NodeID) -> f64;
}

struct StrawNodeSelector {
    weight_fetcher: Box<dyn NodeWeighteFetcher>,
}

impl StrawNodeSelector {
    fn new(weight_fetcher: Box<dyn NodeWeighteFetcher>) -> Self {
        Self { weight_fetcher }
    }
}

// NOTE: Straw2 algorithm
impl NodeSelector for StrawNodeSelector {
    fn select_node(&self, all_node_cnt: usize, fn_name: &str) -> NodeID {
        // NOTE: 1 is an impossible value for straw
        let mut max_straw: f64 = 1.0;
        let mut node_id: NodeID = 1;
        // NOTE: node id is [1,all_node_cnt]
        for i in 1..all_node_cnt + 1 {
            let mut hasher = DefaultHasher::new();
            hasher.write(fn_name.as_bytes());
            hasher.write_u64(i as u64);
            let hash = hasher.finish() % 63336;
            let weight = self.weight_fetcher.get_node_weight(i as NodeID);
            let straw = ((hash as f64) / 65536.0).ln() / weight;
            if (max_straw - 1.0).abs() < 0.000001 || max_straw < straw {
                max_straw = straw;
                node_id = i as NodeID;
            }
        }
        return node_id;
    }
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
            executor: Executor::new(args.nodes_config.file_dir),
            // each_fn_caching: HashMap::new(),
            node_selector: Box::new(HashNodeSelector),
            request_handler_view: RequestHandlerView::new(args.logical_modules_ref.clone()),
            // view: ,
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start as master");

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
        let node = self.node_selector.select_node(
            self.request_handler_view.p2p().nodes_config.peers.len() + 1,
            req_fn,
        );

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
