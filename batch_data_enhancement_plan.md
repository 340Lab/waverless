# 批量数据处理改进计划

## 1. 删除代码 [根据review.md]

### 1.1 src/main/src/general/data/m_data_general/batch.rs
1. 删除 BatchManager 结构体及其实现
2. 删除 BatchTransfer 结构体及其实现

### 1.2 src/main/src/general/data/m_data_general/mod.rs
1. 删除 DataGeneral 中的 batch_manager 字段
2. 删除 DataGeneral::new() 中的相关初始化代码

## 2. 错误处理增强 [根据review.md]

### 2.1 修改 src/main/src/result.rs
```rust
pub enum WsDataError {
    BatchTransferFailed {
        request_id: proto::BatchRequestId,
        reason: String,
    },
    BatchTransferNotFound {
        request_id: proto::BatchRequestId,
    },
    BatchTransferError {
        request_id: proto::BatchRequestId,
        msg: String,
    },
    WriteDataFailed {
        request_id: proto::BatchRequestId,
    },
    SplitTaskFailed {
        request_id: proto::BatchRequestId,
        idx: DataSplitIdx,
    },
    VersionMismatch {
        expected: u64,
        actual: u64,
    },
}
```

## 3. 新增代码 [根据review.md]

### 3.1 src/main/src/general/data/m_data_general/task.rs

#### WriteSplitDataTaskHandle
```rust
pub struct WriteSplitDataTaskHandle {
    tx: mpsc::Sender<tokio::task::JoinHandle<()>>,
    write_type: WriteSplitDataType,
    version: u64,
}

enum WriteSplitDataType {
    File { path: PathBuf },
    Mem { shared_mem: SharedMemHolder },
}
```

#### WriteSplitDataTaskGroup
```rust
enum WriteSplitDataTaskGroup {
    ToFile {
        unique_id: UniqueId,
        file_path: PathBuf,
        tasks: Vec<tokio::task::JoinHandle<()>>,
        rx: mpsc::Receiver<tokio::task::JoinHandle<()>>,
        expected_size: usize,
        current_size: usize,
    },
    ToMem {
        unique_id: UniqueId,
        shared_mem: SharedMemHolder,
        tasks: Vec<tokio::task::JoinHandle<()>>,
        rx: mpsc::Receiver<tokio::task::JoinHandle<()>>,
        expected_size: usize,
        current_size: usize,
    }
}
```

### 3.2 src/main/src/general/data/m_data_general/mod.rs

#### SharedWithBatchHandler [根据review.md]
```rust
#[derive(Clone)]
struct SharedWithBatchHandler {
    responsor: Arc<Mutex<Option<RPCResponsor<BatchDataRequest>>>>,
}

impl SharedWithBatchHandler {
    fn new() -> Self {
        Self {
            responsor: Arc::new(Mutex::new(None)),
        }
    }

    async fn update_responsor(&self, responsor: RPCResponsor<BatchDataRequest>) {
        let mut guard = self.responsor.lock().await;
        if let Some(old_responsor) = guard.take() {
            // 旧的responsor直接返回成功
            if let Err(e) = old_responsor.response(Ok(())).await {
                tracing::error!("Failed to respond to old request: {}", e);
            }
        }
        *guard = Some(responsor);
    }

    async fn get_final_responsor(&self) -> Option<RPCResponsor<BatchDataRequest>> {
        self.responsor.lock().await.take()
    }
}
```

#### BatchReceiveState [根据review.md]
```rust
// 由DataGeneral持有，存储在DashMap<proto::BatchRequestId, BatchReceiveState>中
// 用于管理每个批量数据传输请求的状态
struct BatchReceiveState {
    handle: WriteSplitDataTaskHandle,  // 写入任务句柄
    shared: SharedWithBatchHandler,    // 共享响应器
}
```

impl DataGeneral {
    pub fn new() -> Self {
        Self {
            batch_receive_states: DashMap::new(),
            // ... 其他字段初始化
        }
    }
}

## 4. 功能实现 [根据design.canvas]

### 4.1 process_tasks() 实现 [阻塞循环]
```rust
impl WriteSplitDataTaskGroup {
    async fn process_tasks(&mut self) -> WSResult<proto::DataItem> {
        loop {
            // 1. 检查完成状态
            if let Some(item) = self.try_complete() {
                return Ok(item);
            }

            // 2. 等待新任务或已有任务完成
            tokio::select! {
                Some(new_task) = match self {
                    Self::ToFile { rx, .. } |
                    Self::ToMem { rx, .. } => rx.recv()
                } => {
                    match self {
                        Self::ToFile { tasks, .. } |
                        Self::ToMem { tasks, .. } => {
                            tasks.push(new_task);
                        }
                    }
                }
                Some(completed_task) = futures::future::select_all(match self {
                    Self::ToFile { tasks, .. } |
                    Self::ToMem { tasks, .. } => tasks
                }) => {
                    // 检查任务是否成功完成
                    if let Err(e) = completed_task.0 {
                        tracing::error!("Task failed: {}", e);
                        return Err(WSError::WsDataError(WsDataError::BatchTransferFailed {
                            request_id: match self {
                                Self::ToFile { unique_id, .. } |
                                Self::ToMem { unique_id, .. } => unique_id.clone()
                            },
                            reason: format!("Task failed: {}", e)
                        }));
                    }
                    // 从任务列表中移除已完成的任务
                    match self {
                        Self::ToFile { tasks, current_size, .. } |
                        Self::ToMem { tasks, current_size, .. } => {
                            tasks.remove(completed_task.1);
                            // 更新当前大小
                            *current_size += DEFAULT_BLOCK_SIZE;
                        }
                    }
                }
                None = match self {
                    Self::ToFile { rx, .. } |
                    Self::ToMem { rx, .. } => rx.recv()
                } => {
                    // 通道关闭,直接退出
                    break;
                }
            }
        }

        Err(WSError::WsDataError(WsDataError::BatchTransferFailed {
            request_id: match self {
                Self::ToFile { unique_id, .. } |
                Self::ToMem { unique_id, .. } => unique_id.clone()
            },
            reason: "Channel closed".to_string()
        }))
    }
}
```

### 4.2 try_complete() 实现 [同步检查]
```rust
impl WriteSplitDataTaskGroup {
    fn try_complete(&self) -> Option<proto::DataItem> {
        match self {
            Self::ToFile { current_size, expected_size, file_path, .. } => {
                if *current_size >= *expected_size {
                    Some(proto::DataItem::new_file_data(file_path.clone()))
                } else {
                    None
                }
            }
            Self::ToMem { current_size, expected_size, shared_mem, .. } => {
                if *current_size >= *expected_size {
                    Some(proto::DataItem::new_mem_data(shared_mem.clone()))
                } else {
                    None
                }
            }
        }
    }
}
```

## 5. 日志增强 [根据错误处理规范]

### 5.1 关键点日志
```rust
// 文件写入错误
tracing::error!("Failed to write file data at offset {}: {}", offset, e);

// 内存写入错误
tracing::error!("Failed to write memory data at offset {}: {}", offset, e);

// 任务提交错误
tracing::error!("Failed to submit task: channel closed, idx: {:?}", idx);

// 任务组创建
tracing::debug!(
    "Creating new task group: unique_id={:?}, block_type={:?}, version={}",
    unique_id, block_type, version
);

// 响应器更新错误
tracing::error!("Failed to respond to old request: {}", e);
```

## 6. 测试计划

### 6.1 单元测试
1. WriteSplitDataTaskHandle
   - 版本号获取
   - 分片任务提交
   - 任务等待

2. WriteSplitDataTaskGroup
   - 任务组创建
   - 任务处理循环
   - 完成状态检查

3. DataItemSource
   - 内存数据读取
   - 文件数据读取
   - 块类型判断

4. SharedWithBatchHandler
   - 响应器更新
   - 旧响应器处理
   - 最终响应器获取

### 6.2 集成测试
1. 文件写入流程
2. 内存写入流程
3. 错误处理
4. 并发控制
