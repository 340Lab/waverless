use async_trait::async_trait;
use sysinfo::{CpuExt, CpuRefreshKind, RefreshKind, System, SystemExt};
use ws_derive::LogicalModule;

use crate::{
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef},
    util::JoinHandleWrapper,
};

use super::network::{
    m_p2p::{MsgSender, P2PModule},
    proto,
};

logical_module_view_impl!(MetricPublisherView);
logical_module_view_impl!(MetricPublisherView, p2p, P2PModule);
// logical_module_view_impl!(MetricPublisherView, metric_observor, Option<MetricObservor>);
logical_module_view_impl!(MetricPublisherView, metric_publisher, MetricPublisher);

#[derive(LogicalModule)]
pub struct MetricPublisher {
    msg_sender: MsgSender<proto::metric::RscMetric>,
    view: MetricPublisherView,
}

#[async_trait]
impl LogicalModule for MetricPublisher {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: MetricPublisherView::new(args.logical_modules_ref.clone()),
            msg_sender: MsgSender::default(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let view = self.view.clone();
        // continuous send metrics to master
        Ok(vec![JoinHandleWrapper::from(tokio::spawn(async move {
            // start_http_handler(view).await;
            report_metric_task(view).await;
        }))])
    }
}

async fn report_metric_task(view: MetricPublisherView) {
    // let mut m = machine_info::Machine::new();
    // let info = m.system_info();
    // let total_mem = info.memory as u32;
    // let total_cpu = (info.processor.frequency * (info.total_processors as u64)) as u32;
    let mut sys = System::new_with_specifics(
        RefreshKind::new()
            .with_cpu(CpuRefreshKind::everything())
            .with_memory(),
    );
    // First we update all information of our `System` struct.
    sys.refresh_all();
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        sys.refresh_all();
        // let status = m.system_status().unwrap();

        let cpu_all = sys.cpus()[0].frequency() * sys.cpus().len() as u64;
        let cpu_used =
            sys.cpus().iter().map(|c| c.cpu_usage()).sum::<f32>() / sys.cpus().len() as f32;

        let metric = proto::metric::RscMetric {
            cpu_used,
            mem_used: sys.used_memory() as f32,
            cpu_all: cpu_all as f32,
            mem_all: sys.total_memory() as f32,
        };
        // println!("send metrics to master");
        // let node_config = view.p2p().nodes_config;

        // if node_config.this_node() == node_config.get_master_node() {
        //     view.metric_observor()
        //         .insert_node_rsc_metric(view.p2p().nodes_config.this.0, metric);
        // } else {
        let _res = view
            .metric_publisher()
            .msg_sender
            .send(
                view.p2p(),
                view.p2p().nodes_config.get_master_node(),
                metric,
            )
            .await;
        // .send_resp(1, 0, metric).await;
        // }
    }
}
