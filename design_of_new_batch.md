# 项目分析与修改计划


### 变更

#### 核心接口定义
```rust


#### WriteSplitDataTaskGroup 核心实现
```rust
// 写入任务相关错误
#[derive(Debug)]
pub enum WsDataErr {
    WriteDataFailed {
        unique_id: Vec<u8>,
    },
    SplitTaskFailed {
        idx: DataSplitIdx,
    },
}

// 写入任务句柄，用于提交新的分片任务
pub struct WriteSplitDataTaskHandle {
    tx: mpsc::Sender<tokio::task::JoinHandle<()>>,
    write_type: WriteSplitDataType,
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
    // 提交新的分片任务
    pub async fn submit_split(&self, idx: DataSplitIdx, data: proto::DataItem) {
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

        if let Err(e) = self.tx.send(task).await {
            tracing::error!("Failed to submit task: channel closed, idx: {:?}", idx);
        }
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
        manager: Arc<WriteSplitDataManager>,             // 管理器引用
    },
    // 内存写入模式
    ToMem {
        unique_id: UniqueId,                             // 任务唯一标识
        shared_mem: SharedMemHolder,                     // 共享内存
        tasks: Vec<tokio::task::JoinHandle<()>>,         // 写入任务列表
        rx: mpsc::Receiver<tokio::task::JoinHandle<()>>, // 任务接收通道
        expected_size: usize,                            // 预期总大小
        current_size: usize,                             // 当前写入大小
        manager: Arc<WriteSplitDataManager>,             // 管理器引用
    }
}

impl WriteSplitDataTaskGroup {
    // 创建新任务组
    async fn new(
        unique_id: UniqueId,
        splits: Vec<Range<usize>>,
        block_type: proto::BatchDataBlockType,
        manager: Arc<WriteSplitDataManager>,
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
                };
                
                let group = Self::ToFile {
                    unique_id,
                    file_path,
                    tasks: Vec::new(),
                    rx,
                    expected_size,
                    current_size: 0,
                    manager: manager.clone(),
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
                };
                
                let group = Self::ToMem {
                    unique_id,
                    shared_mem,
                    tasks: Vec::new(),
                    rx,
                    expected_size,
                    current_size: 0,
                    manager: manager.clone(),
                };
                
                (group, handle)
            }
        }
    }

    // 处理任务完成
    async fn handle_completion(&self) {
        match self {
            Self::ToFile { unique_id, manager, .. } |
            Self::ToMem { unique_id, manager, .. } => {
                // 从管理器中移除句柄
                manager.remove_handle(unique_id);
            }
        }
    }

    // 任务处理循环
    async fn process_tasks(&mut self) -> WSResult<proto::DataItem> {
        loop {
            // 检查是否已完成所有写入
            if let Some(result) = self.try_complete() {
                // 处理完成，清理资源
                self.handle_completion().await;
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
                        }
                    }
                }
                else => {
                    // 通道关闭，清理资源
                    self.handle_completion().await;
                    break;
                }
            }
        }

        Err(WSError::WsDataError(WsDataErr::WriteDataFailed {
            unique_id: match self {
                Self::ToFile { unique_id, .. } |
                Self::ToMem { unique_id, .. } => unique_id.clone(),
            }
        }))
    }
}

// WriteSplitDataManager 管理器
pub struct WriteSplitDataManager {
    // 只存储任务句柄
    handles: DashMap<proto::BatchRequestId, WriteSplitDataTaskHandle>,
}

impl WriteSplitDataManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            handles: DashMap::new(),
        })
    }

    // 注册新的任务句柄
    pub fn register_handle(
        &self,
        request_id: proto::BatchRequestId,
        handle: WriteSplitDataTaskHandle,
    ) -> WSResult<()> {
        // 检查是否已存在
        if self.handles.contains_key(&request_id) {
            return Err(WSError::WsDataError(WsDataErr::WriteDataFailed {
                request_id,
            }));
        }

        // 存储句柄
        self.handles.insert(request_id, handle);
        Ok(())
    }

    // 获取已存在的任务句柄
    pub fn get_handle(&self, request_id: &proto::BatchRequestId) -> Option<WriteSplitDataTaskHandle> {
        self.handles.get(request_id).map(|h| h.clone())
    }

    // 移除任务句柄
    pub fn remove_handle(&self, request_id: &proto::BatchRequestId) {
        self.handles.remove(request_id);
    }
}

## 修改 使用情况以适配新接口 计划

### 1. 修改 get_or_del_data 函数

```diff
 pub async fn get_or_del_data(&self, GetOrDelDataArg { meta, unique_id, ty }: GetOrDelDataArg) 
     -> WSResult<(DataSetMetaV2, HashMap<DataItemIdx, proto::DataItem>)> 
 {
     let want_idxs: Vec<DataItemIdx> = WantIdxIter::new(&ty, meta.data_item_cnt() as DataItemIdx).collect();
     
    let mut groups = Vec::new();
    let mut idxs = Vec::new();
    let p2p = self.view.p2p();
    let mut ret = HashMap::new();

    for idx in want_idxs {
        // 为每个数据项创建独立的任务组
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let splits = vec![0..1];
        let splits = vec![0..1];
        let (mut group, handle) = WriteSplitDataTaskGroup::new(
            unique_id.clone(),
            splits,
            match ty {
                GetOrDelDataArgType::Delete => proto::BatchDataBlockType::Delete,
                _ => proto::BatchDataBlockType::Memory,
            },
            Arc::clone(&self.manager),
        ).await;

        let p2p = p2p.clone();
        let unique_id = unique_id.clone();
        let data_node = meta.get_data_node(idx);
        let delete = matches!(ty, GetOrDelDataArgType::Delete);
        let rpc_call = self.rpc_call_get_data.clone();

        let handle_clone = handle.clone();
        let handle = tokio::spawn(async move {
            let resp = rpc_call.call(
                p2p,
                data_node,
                proto::GetOneDataRequest {
                    unique_id: unique_id.to_vec(),
                    idxs: vec![idx as u32],
                    delete,
                    return_data: true,
                },
                Some(Duration::from_secs(60)),
            ).await?;

            if !resp.success {
                tracing::error!("Failed to get data for idx {}: {}", idx, resp.message);
                return Err(WsDataError::GetDataFailed {
                    unique_id: unique_id.to_vec(),
                    msg: resp.message,
                }.into());
            }

            handle_clone.submit_split(0, resp.data[0].clone()).await;
            Ok::<_, WSError>(())
        });

        groups.push(group);
        idxs.push((idx, handle));
    }

    // 等待所有RPC任务完成
    for (group, (idx, handle)) in groups.into_iter().zip(idxs.into_iter()) {
        if let Err(e) = handle.await.map_err(|e| WSError::from(e))?.map_err(|e| e) {
            tracing::error!("RPC task failed for idx {}: {}", idx, e);
            continue;
        }

        match group.join().await {
            Ok(data_item) => {
                ret.insert(idx, data_item);
            }
            Err(e) => {
                tracing::error!("Task group join failed for idx {}: {}", idx, e);
            }
        }
    }

    Ok(ret)
}
```

### 2. Batch数据处理流程更新

#### 2.1 WriteSplitDataTaskHandle扩展 等待全部完成的函数

```rust
impl WriteSplitDataTaskHandle {
    ...

    /// 等待所有已提交的写入任务完成
    pub async fn wait_all_tasks(self) -> WSResult<()> {
    }
}
```

#### 2.2 BatchTransfer 实现

```rust
/// 数据源接口
#[async_trait]
pub trait DataSource: Send + Sync + 'static {
    /// 获取数据总大小
    async fn size(&self) -> WSResult<usize>;
    /// 读取指定范围的数据
    async fn read_chunk(&self, offset: usize, size: usize) -> WSResult<Vec<u8>>;
    /// 获取数据块类型
    fn block_type(&self) -> BatchDataBlockType;
}

/// 批量传输数据
pub async fn batch_transfer(
    unique_id: Vec<u8>,
    version: u64,
    target_node: NodeID,
    data: Arc<DataSource>,
    view: DataGeneralView,
) -> WSResult<()> {
    let total_size = data.size().await?;
    let total_blocks = (total_size + DEFAULT_BLOCK_SIZE - 1) / DEFAULT_BLOCK_SIZE;
    let semaphore = Arc::new(Semaphore::new(32));
    let mut handles = Vec::new();
    
    // 发送所有数据块
    for block_idx in 0..total_blocks {
        // 获取信号量许可
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        
        let offset = block_idx as usize * DEFAULT_BLOCK_SIZE;
        let size = DEFAULT_BLOCK_SIZE.min(total_size - offset);
        
        // 读取数据块
        let block_data = data.read_chunk(offset, size).await?;
        
        // 构造请求
        let request = proto::BatchDataRequest {
            request_id: Some(proto::BatchRequestId {
                node_id: target_node as u32,
                sequence: block_idx as u32,
            }),
            block_type: data.block_type() as i32,
            block_index: block_idx as u32,
            data: block_data,
            operation: proto::DataOpeType::Write as i32,
            unique_id: unique_id.clone(),
            version,
        };
        
        // 发送请求
        let view = view.clone();
        let handle = tokio::spawn(async move {
            let _permit = permit; // 持有permit直到任务完成
            let resp = view.data_general().rpc_call_batch_data.call(
                view.p2p(),
                target_node,
                request,
                Some(Duration::from_secs(30)),
            ).await?;
            
            if !resp.success {
                return Err(WsDataError::BatchTransferFailed {
                    node: target_node,
                    batch: block_idx as u32,
                    reason: resp.error_message,
                }.into());
            }
            
            Ok(())
        });
        
        handles.push(handle);
    }
    
    // 等待所有请求完成
    for handle in handles {
        handle.await??;
    }
    
    Ok(())
}
```

#### 2.3 DataGeneral RPC处理实现

```rust
/// 默认数据块大小 (4MB)
const DEFAULT_BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// 批量数据传输状态
struct BatchTransferState {
    handle: WriteSplitDataTaskHandle,
    shared: SharedWithBatchHandler,
}

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

impl DataGeneral {
    /// 创建新的DataGeneral实例
    pub fn new() -> Self {
        Self {
            batch_receive_states: DashMap::new(),
            // ...其他字段
        }
    }
}

impl DataGeneral {
    /// 处理批量数据写入请求
    /// 
    /// # 处理流程
    /// 1. 从batch_receive_states查询或创建传输状态
    /// 2. 使用WriteSplitDataTaskHandle提交写入任务
    /// 3. 等待写入完成并返回结果
    pub async fn rpc_handle_batch_data(
        &self,
        request: BatchDataRequest,
        responsor: RPCResponsor<BatchDataRequest>,
    ) -> WSResult<()> {
        // 1. 从batch_receive_states查询或创建传输状态
        let state = if let Some(state) = self.batch_receive_states.get(&request.unique_id) {
            // 验证版本号
            if state.handle.version() != request.version {
                tracing::error!(
                    "Version mismatch for transfer {}, expected {}, got {}",
                    hex::encode(&request.unique_id),
                    state.handle.version(),
                    request.version
                );
                return Err(WSError::BatchError(WsBatchErr::VersionMismatch {
                    expected: state.handle.version(),
                    actual: request.version,
                }));
            }
            state
        } else {
            // 创建新的写入任务组
            let (group, handle) = WriteSplitDataTaskGroup::new(
                request.unique_id.clone(),
                calculate_splits(request.total_blocks),
                request.block_type,
            ).await?;

            // 创建共享状态
            let shared = SharedWithBatchHandler::new();
            let state = BatchTransferState { handle: handle.clone(), shared: shared.clone() };

            // 启动等待完成的任务
            let unique_id = request.unique_id.clone();
            let batch_receive_states = self.batch_receive_states.clone();
            tokio::spawn(async move {
                // 等待所有任务完成
                if let Err(e) = handle.wait_all_tasks().await {
                    tracing::error!(
                        "Failed to complete transfer {}: {}",
                        hex::encode(&unique_id),
                        e
                    );
                    // 获取最后的responsor并返回错误
                    if let Some(final_responsor) = shared.get_final_responsor().await {
                        if let Err(e) = final_responsor.response(Err(e)).await {
                            tracing::error!("Failed to send error response: {}", e);
                        }
                    }
                    // 清理状态
                    batch_receive_states.remove(&unique_id);
                    return;
                }

                // 获取最后的responsor并返回成功
                if let Some(final_responsor) = shared.get_final_responsor().await {
                    if let Err(e) = final_responsor.response(Ok(())).await {
                        tracing::error!("Failed to send success response: {}", e);
                    }
                }
                // 清理状态
                batch_receive_states.remove(&unique_id);
            });

            // 插入新状态
            self.batch_receive_states.insert(request.unique_id.clone(), state);
            self.batch_receive_states.get(&request.unique_id).unwrap()
        };

        // 2. 使用WriteSplitDataTaskHandle提交写入任务
        let offset = request.block_idx as usize * DEFAULT_BLOCK_SIZE;

        if let Err(e) = state.handle.submit_split(offset, request.data).await {
            tracing::error!(
                "Failed to submit split for transfer {}, block {}: {}",
                hex::encode(&request.unique_id),
                request.block_idx,
                e
            );
            return Err(e);
        }

        // 3. 更新共享状态中的responsor
        state.shared.update_responsor(responsor).await;

        tracing::debug!(
            "Successfully submitted block {} for transfer {}",
            request.block_idx,
            hex::encode(&request.unique_id)
        );

        Ok(())
    }
}

/// 计算数据分片范围
fn calculate_splits(total_blocks: u32) -> Vec<Range<usize>> {
    let mut splits = Vec::with_capacity(total_blocks as usize);
    for i in 0..total_blocks {
        let start = i as usize * DEFAULT_BLOCK_SIZE;
        let end = start + DEFAULT_BLOCK_SIZE;
        splits.push(start..end);
    }
    splits
}

/// 数据源实现
pub struct FileDataSource {
    path: PathBuf,
    file: Option<File>,
}

impl FileDataSource {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            file: None,
        }
    }
}

#[async_trait]
impl DataSource for FileDataSource {
    async fn size(&self) -> WSResult<usize> {
        tokio::fs::metadata(&self.path)
            .await
            .map(|m| m.len() as usize)
            .map_err(|e| WsDataError::ReadSourceFailed {
                source: format!("{}", self.path.display()),
                error: e.to_string(),
            }.into())
    }

    async fn read_chunk(&self, offset: usize, size: usize) -> WSResult<Vec<u8>> {
        let mut file = tokio::fs::File::open(&self.path).await
            .map_err(|e| WsDataError::ReadSourceFailed {
                source: format!("{}", self.path.display()),
                error: e.to_string(),
            })?;
        
        file.seek(SeekFrom::Start(offset as u64)).await
            .map_err(|e| WsDataError::ReadSourceFailed {
                source: format!("{}", self.path.display()),
                error: e.to_string(),
            })?;
        
        let mut buf = vec![0; size];
        file.read_exact(&mut buf).await
            .map_err(|e| WsDataError::ReadSourceFailed {
                source: format!("{}", self.path.display()),
                error: e.to_string(),
            })?;
        
        Ok(buf)
    }

    fn block_type(&self) -> BatchDataBlockType {
        BatchDataBlockType::File
    }
}

pub struct MemDataSource {
    data: Arc<[u8]>,
}

impl MemDataSource {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data: data.into()
        }
    }
}

#[async_trait]
impl DataSource for MemDataSource {
    async fn size(&self) -> WSResult<usize> {
        Ok(self.data.len())
    }

    async fn read_chunk(&self, offset: usize, size: usize) -> WSResult<Vec<u8>> {
        if offset + size > self.data.len() {
            return Err(WsDataError::ReadSourceFailed {
                source: "memory".into(),
                error: "read beyond bounds".into(),
            }.into());
        }
        Ok(self.data[offset..offset + size].to_vec())
    }

    fn block_type(&self) -> BatchDataBlockType {
        BatchDataBlockType::Memory
    }
}
