# 代码修改清单

## 1. 删除代码
```rust
// 1. src/main/src/general/data/m_data_general/batch.rs 中删除
// 1.1 删除 BatchManager
pub(super) struct BatchManager {
    transfers: DashMap<proto::BatchRequestId, BatchTransfer>,
    sequence: AtomicU64,
}

impl BatchManager {
    pub fn new() -> Self
    pub fn next_sequence(&self) -> u64
    pub async fn create_transfer(...)
    pub async fn handle_block(...)
}

// 1.2 删除 BatchTransfer
pub(super) struct BatchTransfer {
    pub unique_id: Vec<u8>,
    pub version: u64,
    pub block_type: proto::BatchDataBlockType,
    pub total_blocks: u32,
    data_sender: mpsc::Sender<WSResult<(DataSplitIdx, proto::DataItem)>>,
    write_task: JoinHandle<WSResult<proto::DataItem>>,
    pub tx: Option<mpsc::Sender<WSResult<proto::DataItem>>>,
}

impl BatchTransfer {
    pub async fn new(...)
    pub async fn add_block(...)
    pub async fn complete(...)
    fn calculate_splits(...)
}

// 2. src/main/src/general/data/m_data_general/mod.rs 中删除
struct DataGeneral {
    batch_manager: Arc<BatchManager>,  // 删除此字段
}

// DataGeneral::new() 中删除
batch_manager: Arc::new(BatchManager::new()),
```

## 2. 新增代码

### src/main/src/result.rs
```rust
pub enum WsDataError {
    // 修改错误类型
    BatchTransferFailed {
        request_id: proto::BatchRequestId,  // 改为 request_id
        reason: String,
    },
    BatchTransferNotFound {
        request_id: proto::BatchRequestId,  // 改为 request_id
    },
    BatchTransferError {
        request_id: proto::BatchRequestId,  // 改为 request_id
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

### src/main/src/general/data/m_data_general/task.rs
```rust
// 写入任务句柄，用于提交新的分片任务
pub struct WriteSplitDataTaskHandle {
    tx: mpsc::Sender<tokio::task::JoinHandle<()>>,
    write_type: WriteSplitDataType,
    version: u64,  // 添加版本号字段
}

// 写入类型
enum WriteSplitDataType {
    File {
        path: PathBuf,
    },
    Mem {
        shared_mem: SharedMemHolder,
    },
}

impl WriteSplitDataTaskHandle {
    // 获取版本号
    pub fn version(&self) -> u64 {
        self.version
    }

    // 提交新的分片任务
    pub async fn submit_split(&self, idx: DataSplitIdx, data: proto::DataItem) -> WSResult<()> {
        let task = match &self.write_type {
            WriteSplitDataType::File { path } => {
                let path = path.clone();
                let offset = idx.offset;
                let data = data.as_bytes().to_vec();
                tokio::spawn(async move {
                    if let Err(e) = tokio::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .open(&path)
                        .await
                        .and_then(|mut file| async move {
                            file.seek(SeekFrom::Start(offset)).await?;
                            file.write_all(&data).await
                        })
                        .await
                    {
                        tracing::error!("Failed to write file data at offset {}: {}", offset, e);
                    }
                })
            }
            WriteSplitDataType::Mem { shared_mem } => {
                let mem = shared_mem.clone();
                let offset = idx.offset as usize;
                let data = data.as_bytes().to_vec();
                tokio::spawn(async move {
                    if let Err(e) = mem.write(offset, &data).await {
                        tracing::error!("Failed to write memory data at offset {}: {}", offset, e);
                    }
                })
            }
        };

        self.tx.send(task).await.map_err(|e| {
            tracing::error!("Failed to submit task: channel closed, idx: {:?}", idx);
            WSError::WsDataError(WsDataError::BatchTransferFailed {
                request_id: idx.into(), // 需要实现 From<DataSplitIdx> for BatchRequestId
                reason: "Failed to submit task: channel closed".to_string()
            })
        })
    }

    /// 等待所有已提交的写入任务完成
    pub async fn wait_all_tasks(self) -> WSResult<()> {
        // 关闭发送端,不再接收新任务
        drop(self.tx);

        Ok(())
    }
}

// 写入任务组
enum WriteSplitDataTaskGroup {
    // 文件写入模式
    ToFile {
        unique_id: UniqueId,                             // 任务唯一标识
        file_path: PathBuf,                              // 文件路径
        tasks: Vec<tokio::task::JoinHandle<()>>,         // 写入任务列表
        rx: mpsc::Receiver<tokio::task::JoinHandle<()>>, // 任务接收通道
        expected_size: usize,                            // 预期总大小
        current_size: usize,                             // 当前写入大小
    },
    // 内存写入模式
    ToMem {
        unique_id: UniqueId,                             // 任务唯一标识
        shared_mem: SharedMemHolder,                     // 共享内存
        tasks: Vec<tokio::task::JoinHandle<()>>,         // 写入任务列表
        rx: mpsc::Receiver<tokio::task::JoinHandle<()>>, // 任务接收通道
        expected_size: usize,                            // 预期总大小
        current_size: usize,                             // 当前写入大小
    }
}

impl WriteSplitDataTaskGroup {
    // 创建新任务组
    async fn new(
        unique_id: UniqueId,
        splits: Vec<Range<usize>>,
        block_type: proto::BatchDataBlockType,
        version: u64,  // 添加版本号参数
    ) -> (Self, WriteSplitDataTaskHandle) {
        // 计算预期总大小
        let expected_size = splits.iter().map(|range| range.len()).sum();
        
        // 创建通道
        let (tx, rx) = mpsc::channel(32);
        
        match block_type {
            proto::BatchDataBlockType::File => {
                let file_path = PathBuf::from(format!("{}.data", 
                    base64::engine::general_purpose::STANDARD.encode(&unique_id)));
                
                let handle = WriteSplitDataTaskHandle {
                    tx,
                    write_type: WriteSplitDataType::File {
                        path: file_path.clone(),
                    },
                    version,  // 设置版本号
                };
                
                let group = Self::ToFile {
                    unique_id,
                    file_path,
                    tasks: Vec::new(),
                    rx,
                    expected_size,
                    current_size: 0,
                };
                
                (group, handle)
            }
            _ => {
                let shared_mem = new_shared_mem(&splits).unwrap_or_default();
                
                let handle = WriteSplitDataTaskHandle {
                    tx,
                    write_type: WriteSplitDataType::Mem {
                        shared_mem: shared_mem.clone(),
                    },
                    version,  // 设置版本号
                };
                
                let group = Self::ToMem {
                    unique_id,
                    shared_mem,
                    tasks: Vec::new(),
                    rx,
                    expected_size,
                    current_size: 0,
                };
                
                (group, handle)
            }
        }
    }

    // 任务处理循环
    async fn process_tasks(&mut self) -> WSResult<proto::DataItem> {
        loop {
            // 检查是否已完成所有写入
            if let Some(result) = self.try_complete() {
                return Ok(result);
            }

            // 等待新任务或已有任务完成
            tokio::select! {
                Some(new_task) = match self {
                    Self::ToFile { rx, .. } |
                    Self::ToMem { rx, .. } => rx.recv()
                } => {
                    match self {
                        Self::ToFile { tasks, .. } |
                        Self::ToMem { tasks, .. } => {
                            tasks.push(new_task);
                            // 不需要更新current_size,因为是在任务完成时更新
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
                            *current_size += DEFAULT_BLOCK_SIZE;  // 每个任务写入一个块
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

    /// 检查是否已完成所有写入
    fn try_complete(&self) -> Option<proto::DataItem> {
        match self {
            Self::ToFile { current_size, expected_size, file_path, .. } => {
                if *current_size >= *expected_size {
                    // 所有数据已写入,返回文件数据项
                    Some(proto::DataItem::new_file_data(file_path.clone()))
                } else {
                    None
                }
            }
            Self::ToMem { current_size, expected_size, shared_mem, .. } => {
                if *current_size >= *expected_size {
                    // 所有数据已写入,返回内存数据项
                    Some(proto::DataItem::new_mem_data(shared_mem.clone()))
                } else {
                    None
                }
            }
        }
    }
}
```

### src/main/src/general/data/m_data_general/mod.rs
```rust
/// 共享状态，用于记录最新的请求响应器
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

/// 批量数据传输状态
struct BatchReceiveState {
    handle: WriteSplitDataTaskHandle,
    shared: SharedWithBatchHandler,
}

pub struct DataGeneral {
    // 批量数据接收状态管理
    batch_receive_states: DashMap<proto::BatchRequestId, BatchReceiveState>,
    // ... 其他字段
}

impl DataGeneral {
    pub fn new() -> Self {
        Self {
            batch_receive_states: DashMap::new(),
            // ... 其他字段初始化
        }
    }

    /// 处理批量数据写入请求
    pub async fn rpc_handle_batch_data(
        &self,
        request: BatchDataRequest,
        responsor: RPCResponsor<BatchDataRequest>,
    ) -> WSResult<()> {
        let state = if let Some(state) = self.batch_receive_states.get(&request.request_id) {
            // 验证版本号
            if state.handle.version() != request.version {
                tracing::error!(
                    "Version mismatch for transfer {:?}, expected {}, got {}",
                    request.request_id,
                    state.handle.version(),
                    request.version
                );
                return Err(WSError::WsDataError(WsDataError::BatchTransferError {
                    request_id: request.request_id,
                    msg: format!("Version mismatch, expected {}, got {}", 
                        state.handle.version(), request.version)
                }));
            }
            state
        } else {
            // 创建新的写入任务组
            let (group, handle) = WriteSplitDataTaskGroup::new(
                request.unique_id.clone(),
                calculate_splits(request.total_blocks),
                request.block_type,
                request.version,  // 传递版本号
            ).await?;

            // 创建共享状态
            let shared = SharedWithBatchHandler::new();
            let state = BatchReceiveState { handle: handle.clone(), shared: shared.clone() };

            // 启动等待完成的任务
            let request_id = request.request_id.clone();  // 使用 request_id
            let batch_receive_states = self.batch_receive_states.clone();
            tokio::spawn(async move {
                // 等待所有任务完成
                if let Err(e) = handle.wait_all_tasks().await {
                    tracing::error!(
                        "Failed to complete transfer {:?}: {}",
                        request_id,  // 使用 request_id
                        e
                    );
                    // 获取最后的responsor并返回错误
                    if let Some(final_responsor) = shared.get_final_responsor().await {
                        if let Err(e) = final_responsor.response(Err(e)).await {
                            tracing::error!("Failed to send error response: {}", e);
                        }
                    }
                    // 清理状态
                    batch_receive_states.remove(&request_id);  // 使用 request_id
                    return;
                }

                // 获取最后的responsor并返回成功
                if let Some(final_responsor) = shared.get_final_responsor().await {
                    if let Err(e) = final_responsor.response(Ok(())).await {
                        tracing::error!("Failed to send success response: {}", e);
                    }
                }
                // 清理状态
                batch_receive_states.remove(&request_id);  // 使用 request_id
            });

            // 插入新状态
            self.batch_receive_states.insert(request.request_id.clone(), state);
            self.batch_receive_states.get(&request.request_id).unwrap()
        };

        // 2. 使用WriteSplitDataTaskHandle提交写入任务
        let offset = request.block_index as usize * DEFAULT_BLOCK_SIZE;  // 使用 block_index

        if let Err(e) = state.handle.submit_split(offset, request.data).await {
            tracing::error!(
                "Failed to submit split for transfer {:?}, block {}: {}",
                request.request_id,
                request.block_index,  // 使用 block_index
                e
            );
            return Err(e);
        }

        // 3. 更新共享状态中的responsor
        state.shared.update_responsor(responsor).await;

        tracing::debug!(
            "Successfully submitted block {} for transfer {:?}",
            request.block_index,
            request.request_id
        );

        Ok(())
    }
}