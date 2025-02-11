use crate::general::network::{
    proto::BatchDataRequest,
    proto::BatchDataResponse,
    m_p2p::RPCResponsor,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing;

/// 共享状态,用于记录最新的请求响应器
/// 当收到新的请求时,会更新响应器并自动处理旧的请求
#[derive(Clone)]
pub struct SharedWithBatchHandler {
    /// 当前活跃的响应器
    /// 使用 Arc<Mutex> 保证线程安全
    responsor: Arc<Mutex<Option<RPCResponsor<BatchDataRequest>>>>,
}

impl SharedWithBatchHandler {
    /// 创建新的共享状态
    #[must_use]
    pub fn new() -> Self {
        Self {
            responsor: Arc::new(Mutex::new(None)),
        }
    }

    /// 更新响应器
    /// 如果存在旧的响应器,会自动返回成功
    /// 
    /// # 参数
    /// * `responsor` - 新的响应器
    pub async fn update_responsor(&self, responsor: RPCResponsor<BatchDataRequest>) {
        let mut guard = self.responsor.lock().await;
        if let Some(old_responsor) = guard.take() {
            // 旧的responsor直接返回成功
            if let Err(e) = old_responsor.send_resp(BatchDataResponse {
                request_id: None,  // 这里需要正确的 request_id
                version: 0,  // 这里需要正确的版本号
                success: true,
                error_message: String::new(),
            }).await {
                tracing::error!("Failed to respond to old request: {}", e);
            }
        }
        *guard = Some(responsor);
    }

    /// 获取最终的响应器
    /// 用于在所有数据都写入完成后发送最终响应
    pub async fn get_final_responsor(&self) -> Option<RPCResponsor<BatchDataRequest>> {
        self.responsor.lock().await.take()
    }
}

/// 批量数据传输状态
/// 用于管理单个批量数据传输请求的生命周期
pub struct BatchReceiveState {
    /// 写入任务句柄
    pub handle: super::dataitem::WriteSplitDataTaskHandle,
    /// 共享状态,用于处理请求响应
    pub shared: SharedWithBatchHandler,
}

impl BatchReceiveState {
    /// 创建新的批量数据传输状态
    /// 
    /// # 参数
    /// * `handle` - 写入任务句柄
    /// * `shared` - 共享状态
    pub fn new(handle: super::dataitem::WriteSplitDataTaskHandle, shared: SharedWithBatchHandler) -> Self {
        Self {
            handle,
            shared,
        }
    }
}
