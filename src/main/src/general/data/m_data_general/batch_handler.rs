use crate::general::network::{
    proto::BatchDataRequest,
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
            if let Err(e) = old_responsor.response(Ok(())).await {
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
    /// 任务组,持有以保持其生命周期
    /// 当 BatchReceiveState 被 drop 时,任务组也会被 drop
    /// 确保所有相关资源都被正确释放
    pub task_group: super::dataitem::WriteSplitDataTaskGroup,
}

impl BatchReceiveState {
    /// 创建新的批量数据传输状态
    /// 
    /// # 参数
    /// * `handle` - 写入任务句柄
    /// * `task_group` - 任务组
    pub fn new(handle: super::dataitem::WriteSplitDataTaskHandle, 
               task_group: super::dataitem::WriteSplitDataTaskGroup) -> Self {
        Self {
            handle,
            shared: SharedWithBatchHandler::new(),
            task_group,
        }
    }
}
