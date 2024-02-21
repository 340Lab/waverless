use std::{collections::hash_map::DefaultHasher, hash::Hasher};

use async_trait::async_trait;
use ws_derive::LogicalModule;

use crate::{
    general::network::{
        m_p2p::{P2PModule, RPCHandler, RPCResponsor},
        proto::{self, sche::MakeSchePlanResp},
    },
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef, NodeID},
    util::JoinHandleWrapper,
};

use super::m_master_kv::MasterKv;

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

pub trait NodeSelector: Send + Sync + 'static {
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

logical_module_view_impl!(MasterView);
logical_module_view_impl!(MasterView, p2p, P2PModule);
logical_module_view_impl!(MasterView, master, Option<Master>);
logical_module_view_impl!(MasterView, master_kv, Option<MasterKv>);

#[derive(LogicalModule)]
pub struct Master {
    // each_fn_caching: HashMap<String, HashSet<NodeId>>,
    node_selector: Box<dyn NodeSelector>,
    rpc_handler: RPCHandler<proto::sche::MakeSchePlanReq>,
    view: MasterView,
}

#[async_trait]
impl LogicalModule for Master {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: MasterView::new(args.logical_modules_ref.clone()),
            rpc_handler: RPCHandler::default(),
            node_selector: Box::new(HashNodeSelector),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start as master");

        let view = self.view.clone();
        self.rpc_handler
            .regist(&self.view.p2p(), move |responser, r| {
                let view = view.clone();
                let _ = tokio::spawn(async move {
                    view.master().handle_remote_schedule(responser, r).await;
                });
                Ok(())
            });

        Ok(vec![])
    }
}

impl Master {
    pub async fn handle_http_schedule(&self, _app: &str) -> NodeID {
        self.select_node()
    }
    pub async fn handle_remote_schedule(
        &self,
        responsor: RPCResponsor<proto::sche::MakeSchePlanReq>,
        req: proto::sche::MakeSchePlanReq,
    ) {
        let mut resp = MakeSchePlanResp {
            sche_target_node: req
                .app_fns
                .iter()
                .map(|_app_fn| self.select_node())
                .collect(),
            data_target_node: 0,
        };
        resp.data_target_node = resp.sche_target_node[0];
        // let res = self.request_handler().select_node(r).await;
        // let _ = responser.send_resp(res).await;
        if let Err(err) = responsor.send_resp(resp).await {
            tracing::error!("send resp failed with err: {}", err);
        };
    }
    fn select_node(&self) -> NodeID {
        2
    }
}
