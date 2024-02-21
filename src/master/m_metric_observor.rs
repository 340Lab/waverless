use crate::{
    general::network::{
        m_p2p::{MsgHandler, P2PModule},
        proto,
    },
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef, NodeID},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use prometheus_client::registry::Registry;
use std::collections::HashSet;
use ws_derive::LogicalModule;

use self::prometheus::{Metrics, RscLabels, RscType};

// pub struct NodeRscMetric {
//     used_cpu: f64,
//     total_cpu: f64,
//     used_memory: f64,
//     total_memory: f64,
// }

pub mod prometheus {
    use std::sync::atomic::AtomicU64;

    use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
    use prometheus_client::metrics::counter::Counter;
    use prometheus_client::metrics::family::Family;
    use prometheus_client::metrics::gauge::Gauge;
    use prometheus_client::registry::Registry;

    use crate::sys::NodeID;

    // Define a type representing a metric label set, i.e. a key value pair.
    //
    // You could as well use `(String, String)` to represent a label set,
    // instead of the custom type below.
    #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
    pub struct RequestLabels {
        // Use your own enum types to represent label values.
        pub method: Method,
        // Or just a plain string.
        pub path: String,
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
    pub enum Method {
        GET,
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
    pub struct RscLabels {
        pub node_id: NodeID,
        pub rsc_type: RscType,
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
    pub enum RscType {
        CpuAll,
        MemAll,
        CpuUsed,
        MemUsed,
    }

    pub struct Metrics {
        pub requests: Family<RequestLabels, Counter>,
        pub rscs: Family<RscLabels, Gauge<f64, AtomicU64>>,
    }

    pub fn new_registry_and_metrics() -> (Metrics, Registry) {
        let mut registry = Registry::default();
        let metrics = Metrics {
            requests: Family::default(),
            rscs: Family::default(),
        };
        registry.register(
            "requests",
            "Function requests record",
            metrics.requests.clone(),
        );
        registry.register("rscs", "Resource usage record", metrics.rscs.clone());
        (metrics, registry)
    }
}

pub struct NodeFnCacheMetric(HashSet<String>);

logical_module_view_impl!(MetricObservorView);
logical_module_view_impl!(MetricObservorView, p2p, P2PModule);
logical_module_view_impl!(MetricObservorView, metric_observor, Option<MetricObservor>);

#[derive(LogicalModule)]
pub struct MetricObservor {
    pub registry: Registry,
    metrics: Metrics,
    // node_rsc_metric: SkipMap<NodeID, proto::metric::RscMetric>,
    view: MetricObservorView,
    msg_handler: MsgHandler<proto::metric::RscMetric>,
}

#[async_trait]
impl LogicalModule for MetricObservor {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        let (metrics, registry) = prometheus::new_registry_and_metrics();
        Self {
            registry,
            metrics,

            view: MetricObservorView::new(args.logical_modules_ref.clone()),
            msg_handler: MsgHandler::default(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        // self.view.p2p().regist_dispatch(m, f)
        let view = self.view.clone();
        self.msg_handler
            .regist(self.view.p2p(), move |responser, msg| {
                let ob = view.metric_observor();
                // tracing::info!("recv rsc metric from node {} {:?}", responser.node_id, msg);
                let _ = ob.insert_node_rsc_metric(responser.node_id, msg);
                Ok(())
            });

        Ok(vec![])
    }
}

impl MetricObservor {
    fn insert_node_rsc_metric(&self, nid: NodeID, msg: proto::metric::RscMetric) {
        // let _ = self.node_rsc_metric.insert(nid, msg);
        let _ = self
            .metrics
            .rscs
            .get_or_create(&RscLabels {
                node_id: nid,
                rsc_type: RscType::CpuAll,
            })
            .set(msg.cpu_all as f64);
        let _ = self
            .metrics
            .rscs
            .get_or_create(&RscLabels {
                node_id: nid,
                rsc_type: RscType::MemAll,
            })
            .set(msg.mem_all as f64);
        let _ = self
            .metrics
            .rscs
            .get_or_create(&RscLabels {
                node_id: nid,
                rsc_type: RscType::CpuUsed,
            })
            .set(msg.cpu_used as f64);
        let _ = self
            .metrics
            .rscs
            .get_or_create(&RscLabels {
                node_id: nid,
                rsc_type: RscType::MemUsed,
            })
            .set(msg.mem_used as f64);
    }
}
