use prometheus::{register_int_counter, IntCounter, register_int_gauge, IntGauge, Encoder, TextEncoder};
use lazy_static::lazy_static;
use axum::response::IntoResponse;
use sysinfo::{System, SystemExt};

lazy_static! {
    // HTTP请求总数
    pub static ref HTTP_REQUESTS_TOTAL: IntCounter = register_int_counter!(
        "waverless_http_requests_total",
        "HTTP请求总数"
    ).unwrap();

    // 函数调用总数
    pub static ref FUNCTION_CALLS_TOTAL: IntCounter = register_int_counter!(
        "waverless_function_calls_total",
        "函数调用总数"
    ).unwrap();

    // 批处理任务数
    pub static ref BATCH_TASKS_TOTAL: IntCounter = register_int_counter!(
        "waverless_batch_tasks_total",
        "批处理任务数"
    ).unwrap();

    // 节点内存使用量（单位：字节）
    pub static ref NODE_MEMORY_USAGE: IntGauge = register_int_gauge!(
        "waverless_node_memory_usage_bytes",
        "节点内存使用量"
    ).unwrap();
}

// /metrics 路由的 handler
pub async fn metrics_handler() -> impl IntoResponse {
    // 每次请求时更新内存指标
    let mut sys = System::new_all();
    sys.refresh_memory();
    NODE_MEMORY_USAGE.set(sys.used_memory() as i64); // 转为字节

    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}