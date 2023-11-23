use crate::{schedule, start_tracing};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_http_handler() {
    start_tracing();
    schedule::start_http_handler().await;
}
