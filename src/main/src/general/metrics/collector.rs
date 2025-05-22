// waverless/src/main/src/general/metrics/collector.rs
use crate::general::{LogicalModule, LogicalModuleNewArgs};
use crate::result::WSResult;
use crate::util::JoinHandleWrapper;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time;
use sysinfo::{System, SystemExt, ProcessExt};
use std::process;
use super::types::NodeMetrics;
use async_trait::async_trait;

/// 指标收集器，负责收集本节点的性能指标
/// 在Worker节点上运行时，会自动将指标通过RPC发送给Master节点
pub struct MetricsCollector {
    // 本地收集的指标
    metrics: Arc<Mutex<Registry>>,
    
    // 进程指标
    process_cpu: Arc<Gauge>,
    process_memory: Arc<Gauge>,
    
    // 模块引用
    args: LogicalModuleNewArgs,
    
    // 是否是 Master 节点
    is_master: bool,
    
    // 当前进程ID
    pid: i32,
}

#[async_trait]
impl LogicalModule for MetricsCollector {
    fn inner_new(args: LogicalModuleNewArgs) -> Self {
        let process_cpu = Arc::new(Gauge::default());
        let process_memory = Arc::new(Gauge::default());
        
        let mut registry = Registry::default();
        
        // 注册进程指标
        registry.register(
            "waverless_process_cpu_percent",
            "当前进程CPU使用率（百分比）",
            process_cpu.clone(),
        );
        registry.register(
            "waverless_process_memory_bytes",
            "当前进程内存使用量（字节）",
            process_memory.clone(),
        );
        
        let is_master = args.nodes_config.this.1.is_master();
        let pid = process::id() as i32;
        
        Self {
            metrics: Arc::new(Mutex::new(registry)),
            process_cpu,
            process_memory,
            args: args.clone(),
            is_master,
            pid,
        }
    }
    
    async fn init(&self) -> WSResult<()> {
        // 初始化时立即收集一次指标
        self.collect_process_metrics();
        tracing::info!("指标收集器已初始化 (节点类型: {})", 
            if self.is_master { "Master" } else { "Worker" });
        Ok(())
    }
    
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let process_cpu = self.process_cpu.clone();
        let process_memory = self.process_memory.clone();
        let is_master = self.is_master;
        let args = self.args.clone();
        let pid = self.pid;
        
        // 启动定期收集任务
        let handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(15));
            let mut sys = System::new_all();
            
            loop {
                interval.tick().await;
                
                // 收集进程指标
                sys.refresh_all();
                
                // 获取当前进程
                if let Some(process) = sys.process(pid) {
                    // 进程CPU使用率
                    let cpu_usage = process.cpu_usage();
                    process_cpu.set((cpu_usage * 100.0) as i64); // 转为百分比整数
                    
                    // 进程内存使用
                    let mem_usage = process.memory();
                    process_memory.set(mem_usage as i64);
                    
                    tracing::debug!("收集到本地指标: CPU={}%, 内存={}字节", 
                        cpu_usage, mem_usage);
                }
                
                // 如果是 Worker 节点，上报指标给 Master
                if !is_master {
                    let node_id = args.nodes_config.this.0;
                    let node_addr = args.nodes_config.this.1.addr.to_string();
                    
                    let metrics = NodeMetrics {
                        node_id,
                        node_addr,
                        process_cpu_percent: (process_cpu.get() as f64) / 100.0,
                        process_memory_bytes: process_memory.get(),
                        timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    };
                    
                    // 使用 P2P 模块发送 RPC 请求
                    if let Ok(modules) = args.inner.upgrade() {
                        if let Some(modules) = modules.as_ref() {
                            let p2p = &modules.p2p;
                            let master_node = args.nodes_config.get_master_node();
                            
                            // 发送 RPC 请求给 Master
                            match p2p.send_rpc_request(
                                master_node,
                                "report_metrics",
                                serde_json::to_string(&metrics).unwrap(),
                            ).await {
                                Ok(_) => tracing::debug!("成功发送指标到Master节点"),
                                Err(e) => tracing::warn!("发送指标到Master节点失败: {:?}", e),
                            }
                        }
                    }
                }
            }
        });
        
        tracing::info!("指标收集器已启动");
        Ok(vec![JoinHandleWrapper::new(handle)])
    }
}

/// 公共方法
impl MetricsCollector {
    /// 收集进程指标
    pub fn collect_process_metrics(&self) {
        let mut sys = System::new_all();
        sys.refresh_all();
        
        if let Some(process) = sys.process(self.pid) {
            self.process_cpu.set((process.cpu_usage() * 100.0) as i64);
            self.process_memory.set(process.memory() as i64);
        }
    }
    
    /// 获取指标 Registry
    pub fn registry(&self) -> Arc<Mutex<Registry>> {
        self.metrics.clone()
    }
}