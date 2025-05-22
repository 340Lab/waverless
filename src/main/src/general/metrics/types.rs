use serde::{Serialize, Deserialize};
use std::collections::HashMap;

// 节点指标数据
#[derive(Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub node_id: u32,
    pub node_addr: String,
    
    // 进程指标
    pub process_cpu_percent: f64,
    pub process_memory_bytes: i64,
    
    pub timestamp: u64,
}

// 聚合的指标数据
#[derive(Default)]
pub struct AggregatedMetrics {
    // 节点ID到指标的映射
    pub node_metrics: HashMap<u32, NodeMetrics>,
}