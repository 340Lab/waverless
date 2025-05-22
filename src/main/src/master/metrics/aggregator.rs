// waverless/src/main/src/master/metrics/aggregator.rs
use crate::general::{LogicalModule, LogicalModuleNewArgs};
use crate::general::metrics::types::{NodeMetrics, AggregatedMetrics};
use crate::result::WSResult;
use crate::util::JoinHandleWrapper;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time;
use tokio::net::TcpListener;
use tokio::io::{AsyncWriteExt};
use std::collections::HashMap;
use async_trait::async_trait;
use prometheus_client::encoding::text::encode;
use prometheus_client::registry::Registry;
use prometheus_client::metrics::gauge::Gauge;

/// Master节点上的指标聚合器
/// 1. 负责聚合所有节点的指标
/// 2. 提供Prometheus HTTP采样接口
pub struct MetricsAggregator {
    // 聚合的指标数据
    metrics: Arc<Mutex<AggregatedMetrics>>,
    
    // Prometheus指标注册表
    registry: Arc<Mutex<Registry>>,
    
    // 各节点CPU使用率指标
    node_cpu_gauges: Arc<Mutex<HashMap<u32, Gauge>>>,
    
    // 各节点内存使用量指标
    node_memory_gauges: Arc<Mutex<HashMap<u32, Gauge>>>,
    
    // 模块引用
    args: LogicalModuleNewArgs,
    
    // HTTP监听端口
    http_port: u16,
}

#[async_trait]
impl LogicalModule for MetricsAggregator {
    fn inner_new(args: LogicalModuleNewArgs) -> Self {
        // 默认监听在9100端口
        let http_port = 9100;
        
        Self {
            metrics: Arc::new(Mutex::new(AggregatedMetrics::default())),
            registry: Arc::new(Mutex::new(Registry::default())),
            node_cpu_gauges: Arc::new(Mutex::new(HashMap::new())),
            node_memory_gauges: Arc::new(Mutex::new(HashMap::new())),
            args,
            http_port,
        }
    }
    
    async fn init(&self) -> WSResult<()> {
        // 注册RPC处理器，处理来自Worker节点的指标报告
        if let Ok(modules) = self.args.inner.upgrade() {
            if let Some(modules) = modules.as_ref() {
                // 获取P2P模块
                let p2p = &modules.p2p;
                
                // 注册RPC处理器
                let metrics = self.metrics.clone();
                p2p.register_rpc_handler("report_metrics", Box::new(move |payload| {
                    let metrics_clone = metrics.clone();
                    Box::pin(async move {
                        if let Ok(node_metrics) = serde_json::from_str::<NodeMetrics>(payload) {
                            MetricsAggregator::handle_metrics_report(metrics_clone, node_metrics);
                            Ok("success".to_string())
                        } else {
                            Err("Invalid metrics format".to_string())
                        }
                    })
                }));
                
                tracing::info!("已注册RPC处理器: report_metrics");
            }
        }
        
        tracing::info!("指标聚合器已初始化，将在端口 {} 提供Prometheus指标", self.http_port);
        Ok(())
    }
    
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let mut handles = Vec::new();
        
        // 任务1: 启动定期清理过期指标的任务
        let metrics = self.metrics.clone();
        let clean_handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60));
            
            loop {
                interval.tick().await;
                
                // 清理超过5分钟没有更新的节点指标
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                
                let mut metrics_lock = metrics.lock().unwrap();
                let before_count = metrics_lock.node_metrics.len();
                metrics_lock.node_metrics.retain(|_, node_metric| {
                    // 保留5分钟内更新的指标
                    now - node_metric.timestamp < 300
                });
                let after_count = metrics_lock.node_metrics.len();
                
                if before_count != after_count {
                    tracing::info!("清理过期节点指标: {} -> {}", before_count, after_count);
                }
            }
        });
        handles.push(JoinHandleWrapper::new(clean_handle));
        
        // 任务2: 启动HTTP服务器，提供Prometheus指标采集端点
        let metrics = self.metrics.clone();
        let registry = self.registry.clone();
        let node_cpu_gauges = self.node_cpu_gauges.clone();
        let node_memory_gauges = self.node_memory_gauges.clone();
        let http_port = self.http_port;
        
        let http_handle = tokio::spawn(async move {
            // 绑定TCP监听器
            let addr = format!("0.0.0.0:{}", http_port);
            let listener = match TcpListener::bind(&addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("无法绑定到HTTP端口 {}: {}", http_port, e);
                    return;
                }
            };
            
            tracing::info!("Prometheus指标HTTP服务器已启动在: {}", addr);
            
            loop {
                match listener.accept().await {
                    Ok((mut socket, addr)) => {
                        let metrics = metrics.clone();
                        let registry = registry.clone();
                        let node_cpu_gauges = node_cpu_gauges.clone();
                        let node_memory_gauges = node_memory_gauges.clone();
                        
                        tokio::spawn(async move {
                            tracing::debug!("收到来自 {} 的HTTP请求", addr);
                            
                            // 更新指标表
                            {
                                let metrics_lock = metrics.lock().unwrap();
                                let mut registry_lock = registry.lock().unwrap();
                                let mut cpu_gauges_lock = node_cpu_gauges.lock().unwrap();
                                let mut memory_gauges_lock = node_memory_gauges.lock().unwrap();
                                
                                // 清空注册表
                                *registry_lock = Registry::default();
                                
                                // 添加所有节点的指标
                                for (node_id, node_metric) in &metrics_lock.node_metrics {
                                    // 创建或获取该节点的CPU指标
                                    let cpu_gauge = cpu_gauges_lock.entry(*node_id)
                                        .or_insert_with(Gauge::default);
                                    cpu_gauge.set((node_metric.process_cpu_percent * 100.0) as i64);
                                    
                                    // 创建或获取该节点的内存指标
                                    let memory_gauge = memory_gauges_lock.entry(*node_id)
                                        .or_insert_with(Gauge::default);
                                    memory_gauge.set(node_metric.process_memory_bytes);
                                    
                                    // 注册到指标表
                                    registry_lock.register(
                                        format!("waverless_node_{}_cpu_percent", node_id),
                                        format!("节点 {} ({}) 的CPU使用率", node_id, node_metric.node_addr),
                                        cpu_gauge.clone(),
                                    );
                                    
                                    registry_lock.register(
                                        format!("waverless_node_{}_memory_bytes", node_id),
                                        format!("节点 {} ({}) 的内存使用量", node_id, node_metric.node_addr),
                                        memory_gauge.clone(),
                                    );
                                }
                            }
                            
                            // 生成Prometheus格式的响应
                            let mut buffer = String::new();
                            encode(&mut buffer, &registry.lock().unwrap())
                                .expect("Failed to encode metrics");
                            
                            // 发送HTTP响应
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                                buffer.len(),
                                buffer
                            );
                            
                            if let Err(e) = socket.write_all(response.as_bytes()).await {
                                tracing::warn!("发送HTTP响应失败: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("接受HTTP连接失败: {}", e);
                    }
                }
            }
        });
        handles.push(JoinHandleWrapper::new(http_handle));
        
        tracing::info!("指标聚合器已启动");
        Ok(handles)
    }
}

// 公共方法和工具函数
impl MetricsAggregator {
    /// 处理来自Worker节点的指标报告
    fn handle_metrics_report(metrics: Arc<Mutex<AggregatedMetrics>>, node_metrics: NodeMetrics) {
        let mut aggregated = metrics.lock().unwrap();
        let node_id = node_metrics.node_id;
        aggregated.node_metrics.insert(node_id, node_metrics);
        tracing::debug!("收到节点 {} 的指标报告", node_id);
    }
    
    /// 获取所有节点的聚合指标
    pub fn get_aggregated_metrics(&self) -> Arc<Mutex<AggregatedMetrics>> {
        self.metrics.clone()
    }
}