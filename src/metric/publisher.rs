use async_trait::async_trait;
use sysinfo::{CpuExt, CpuRefreshKind, RefreshKind, System, SystemExt};
use ws_derive::LogicalModule;

use crate::{
    network::proto,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, MetricPublisherView},
    util::JoinHandleWrapper,
};

#[derive(LogicalModule)]
pub struct MetricPublisher {
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
        if let Some(obs) = view.metric_observor() {
            obs.insert_node_rsc_metric(view.p2p().nodes_config.this.0, metric);
        } else {
            let _res = view.p2p().send_resp(1, 0, metric).await;
        }
    }
}
