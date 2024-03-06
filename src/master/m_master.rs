use std::{collections::hash_map::DefaultHasher, hash::Hasher, time::Duration};

use async_trait::async_trait;
use ws_derive::LogicalModule;

use crate::{
    general::network::{
        m_p2p::{P2PModule, RPCCaller},
        proto::{
            self,
            sche::{distribute_task_req::Trigger, DistributeTaskReq},
        },
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
    pub rpc_caller_distribute_task: RPCCaller<proto::sche::DistributeTaskReq>,
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
            node_selector: Box::new(HashNodeSelector),
            rpc_caller_distribute_task: RPCCaller::default(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start as master");
        self.rpc_caller_distribute_task.regist(&self.view.p2p());

        Ok(vec![])
    }
}

impl Master {
    pub async fn handle_http_schedule(&self, _app: &str) -> NodeID {
        self.select_node()
    }
    pub async fn schedule_one_trigger(&self, app: String, func: String, trigger_data: Trigger) {
        match self
            .view
            .master()
            .rpc_caller_distribute_task
            .call(
                self.view.p2p(),
                self.select_node(),
                DistributeTaskReq {
                    app,
                    func,
                    task_id: 0, // TODO: Context task id for one request
                    trigger: Some(trigger_data),
                },
                Duration::from_secs(60).into(),
            )
            .await
        {
            Ok(_) => {}
            Err(err) => {
                tracing::error!("schedule_one_trigger err: {:?}", err);
            }
        }
    }
    fn select_node(&self) -> NodeID {
        2
    }
}
