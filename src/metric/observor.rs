use std::collections::HashSet;

use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use ws_derive::LogicalModule;

use crate::{
    network::proto,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, MetricObservorView, NodeID},
    util::JoinHandleWrapper,
};

// pub struct NodeRscMetric {
//     used_cpu: f64,
//     total_cpu: f64,
//     used_memory: f64,
//     total_memory: f64,
// }

pub struct NodeFnCacheMetric(HashSet<String>);

#[derive(LogicalModule)]
pub struct MetricObservor {
    node_rsc_metric: SkipMap<NodeID, proto::metric::RscMetric>,
    view: MetricObservorView,
    // node_fn_cache_metric: SkipMap<NodeId, Mutex<NodeFnCacheMetric>>,
}

#[async_trait]
impl LogicalModule for MetricObservor {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            node_rsc_metric: SkipMap::new(),
            view: MetricObservorView::new(args.logical_modules_ref.clone()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        // self.view.p2p().regist_dispatch(m, f)
        let view = self.view.clone();
        self.view.p2p().regist_dispatch(
            proto::metric::RscMetric::default(),
            move |responser, msg| {
                let ob = view.metric_observor().as_ref().unwrap();
                tracing::info!("recv rsc metric from node {} {:?}", responser.node_id, msg);
                let _ = ob.insert_node_rsc_metric(responser.node_id, msg);
                Ok(())
            },
        );
        Ok(vec![])
    }
}

impl MetricObservor {
    pub fn insert_node_rsc_metric(&self, nid: NodeID, msg: proto::metric::RscMetric) {
        let _ = self.node_rsc_metric.insert(nid, msg);
    }
}
