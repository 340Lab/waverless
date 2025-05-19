use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use std::sync::Mutex;
use lazy_static::lazy_static;
use axum::response::{IntoResponse, Response};
use sysinfo::{System, SystemExt};

lazy_static! {
    // 全局指标注册表
    pub static ref METRICS_REGISTRY: Mutex<Registry> = {
        let mut registry = Registry::default();

        // HTTP请求总数
        registry.register(
            "waverless_http_requests_total",
            "HTTP请求总数",
            HTTP_REQUESTS_TOTAL.clone()
        );
        // 函数调用总数
        registry.register(
            "waverless_function_calls_total",
            "函数调用总数",
            FUNCTION_CALLS_TOTAL.clone()
        );
        // 批处理任务数
        registry.register(
            "waverless_batch_tasks_total",
            "批处理任务数",
            BATCH_TASKS_TOTAL.clone()
        );
        // 节点内存使用量
        registry.register(
            "waverless_node_memory_usage_bytes",
            "节点内存使用量",
            NODE_MEMORY_USAGE.clone()
        );

        Mutex::new(registry)
    };

    // 指标定义
    pub static ref HTTP_REQUESTS_TOTAL: Counter = Counter::default();
    pub static ref FUNCTION_CALLS_TOTAL: Counter = Counter::default();
    pub static ref BATCH_TASKS_TOTAL: Counter = Counter::default();
    pub static ref NODE_MEMORY_USAGE: Gauge = Gauge::default();
}

// /metrics 路由的 handler
pub async fn metrics_handler() -> impl IntoResponse {
    // 每次请求时更新内存指标
    let mut sys = System::new_all();
    sys.refresh_memory();
   let _= NODE_MEMORY_USAGE.set((sys.used_memory() * 1024) as i64); // 转为字节

    let mut buffer = String::new();
    let registry = METRICS_REGISTRY.lock().unwrap();
    encode(&mut buffer, &registry).unwrap();
    buffer
}