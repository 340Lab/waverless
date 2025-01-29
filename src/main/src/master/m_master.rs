use std::{
    collections::hash_map::DefaultHasher,
    hash::Hasher,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use axum::response::Redirect;
use rand::Rng;
use ws_derive::LogicalModule;

use crate::{
    config::NodesConfig,
    general::{
        app::{AffinityPattern, AffinityRule, AppMetaManager, AppType, DataEventTrigger, FnMeta},
        network::{
            m_p2p::{P2PModule, RPCCaller},
            proto::{
                self,
                sche::{self, distribute_task_req::Trigger, DistributeTaskReq},
            },
            proto_ext::ProtoExtDataEventTrigger,
        },
    },
    logical_module_view_impl,
    result::{WSResult, WsFuncError},
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
logical_module_view_impl!(MasterView, appmeta_manager, AppMetaManager);

#[derive(Clone)]
pub struct FunctionTriggerContext {
    pub app_name: String,
    pub fn_name: String,
    pub data_unique_id: Vec<u8>,
    pub target_nodes: Vec<NodeID>,
    pub timeout: Duration,
    pub event_type: DataEventTrigger,
}

#[derive(LogicalModule)]
pub struct Master {
    pub rpc_caller_distribute_task: RPCCaller<proto::sche::DistributeTaskReq>,
    view: MasterView,
    task_id_allocator: AtomicU32,
    ope_id_allocator: AtomicU32,
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
            task_id_allocator: AtomicU32::new(0),
            ope_id_allocator: AtomicU32::new(0),
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
        tracing::debug!("node_id : {:?}", self.0);
        let conf = nodesconf.get_nodeconfig(self.0);
        tracing::debug!("conf.http_url() : {:?}", &conf.http_url().clone());
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
    pub fn select_node(&self) -> NodeID {
        let workers = self.view.p2p().nodes_config.get_worker_nodes();
        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0..workers.len());
        workers.iter().nth(idx).unwrap().clone()
    }

    /// Trigger a function execution on target nodes
    ///
    /// # Arguments
    /// * `ctx` - The context containing function and target information
    ///
    /// # Returns
    /// * `WSResult<()>` - Result indicating success or failure
    pub async fn trigger_func_call(&self, ctx: FunctionTriggerContext) -> WSResult<()> {
        // Validate function exists and is executable
        let app_meta = self
            .view
            .appmeta_manager()
            .get_app_meta(&ctx.app_name)
            .await?
            .ok_or_else(|| WsFuncError::AppNotFound {
                app: ctx.app_name.clone(),
            })?;

        let fn_meta =
            app_meta
                .0
                .get_fn_meta(&ctx.fn_name)
                .ok_or_else(|| WsFuncError::FuncNotFound {
                    app: ctx.app_name.clone(),
                    func: ctx.fn_name.clone(),
                })?;

        if !fn_meta.sync_async.asyncable() {
            return Err(WsFuncError::FuncHttpNotSupported {
                fname: ctx.fn_name,
                fmeta: fn_meta.clone(),
            }
            .into());
        }

        // Generate task and operation IDs
        let task_id = self.task_id_allocator.fetch_add(1, Ordering::Relaxed);
        let opeid = self.ope_id_allocator.fetch_add(1, Ordering::Relaxed);

        // Create trigger using the ProtoExtDataEventTrigger trait
        let trigger = DataEventTrigger::Write.into_proto_trigger(ctx.data_unique_id, opeid);

        // Create and send tasks to target nodes
        for &node in &ctx.target_nodes {
            let req = sche::DistributeTaskReq {
                app: ctx.app_name.clone(),
                func: ctx.fn_name.clone(),
                task_id,
                trigger: Some(trigger.clone()),
            };

            // Send request with timeout
            let _ = tokio::time::timeout(
                ctx.timeout,
                self.rpc_caller_distribute_task
                    .call(self.view.p2p(), node, req, Some(ctx.timeout)),
            )
            .await;
        }

        Ok(())
    }
}
