use std::{collections::hash_map::DefaultHasher, hash::Hasher, time::Duration};

use async_trait::async_trait;
use axum::response::Redirect;
use rand::Rng;
use ws_derive::LogicalModule;

use crate::{
    config::NodesConfig,
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

#[allow(dead_code)]
trait NodeWeighteFetcher: Send + Sync + 'static {
    // NOTE: get weight return node weight
    // larger is better
    fn get_node_weight(&self, id: NodeID) -> f64;
}

#[allow(dead_code)]
struct StrawNodeSelector {
    weight_fetcher: Box<dyn NodeWeighteFetcher>,
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

#[allow(dead_code)]
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

#[derive(LogicalModule)]
pub struct Master {
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
            rpc_caller_distribute_task: RPCCaller::default(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start as master");
        self.rpc_caller_distribute_task.regist(&self.view.p2p());

        Ok(vec![])
    }
}

pub enum ScheduleWorkload {
    JavaAppConstruct,
}

pub struct TargetNode(pub NodeID);

impl TargetNode {
    pub fn http_redirect(&self, nodesconf: &NodesConfig) -> Redirect {
        let conf = nodesconf.get_nodeconfig(self.0);
        Redirect::temporary(&conf.http_url())
    }
}

impl Master {
    pub fn schedule(&self, wl: ScheduleWorkload) -> TargetNode {
        match wl {
            ScheduleWorkload::JavaAppConstruct => {
                let workers = self.view.p2p().nodes_config.get_worker_nodes();
                // random select one
                let mut rng = rand::thread_rng();
                let idx = rng.gen_range(0..workers.len());
                TargetNode(*workers.iter().nth(idx).unwrap())
            }
        }
    }
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
        let workers = self.view.p2p().nodes_config.get_worker_nodes();
        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0..workers.len());
        workers.iter().nth(idx).unwrap().clone()
    }
}
