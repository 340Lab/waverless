# 项目分析与修改计划

### 现有

#### DataGeneral
- 功能：数据管理核心模块
- 职责：
  1. 提供数据读写接口
  2. 管理元数据
  3. 协调各子模块功能
  4. 错误处理和恢复
  5. 资源生命周期管理

#### DataSplit
- 功能：数据分片管理
- 核心组件：
  1. EachNodeSplit：单节点分片信息
     ```protobuf
     message EachNodeSplit {
         uint32 node_id = 1;
         uint32 data_offset = 2;
         uint32 data_size = 3;
     }
     ```
  2. DataSplit：分片集合
     ```protobuf
     message DataSplit {
         repeated EachNodeSplit splits = 1;
     }
     ```

#### BatchTransfer
- 功能：管理单个批量传输的状态
- 核心字段：
  ```rust
  struct BatchTransfer {
      unique_id: Vec<u8>,
      version: u64,
      block_type: BatchDataBlockType,
      total_blocks: u32,
      received_blocks: DashMap<u32, Vec<u8>>,
      tx: Option<mpsc::Sender<WSResult<DataItem>>>
  }
  ```
- 主要方法：
  1. `new()`: 创建新的传输任务
  2. `add_block()`: 添加数据块
  3. `complete()`: 完成传输处理
  4. `calculate_splits()`: 计算数据分片

#### WriteSplitDataTaskGroup
- 功能：管理数据分片写入任务组
- 实现类型：
  1. ToFile：文件写入任务组
     - 文件路径管理
     - 文件操作错误处理
     - 磁盘同步策略
  2. ToMem：内存写入任务组
     - SharedMemHolder管理
     - 内存访问安全
     - 资源自动回收


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

// WriteSplitDataTaskGroup 管理器
pub struct WriteSplitDataManager {
    // 只存储任务句柄
    handles: DashMap<UniqueId, WriteSplitDataTaskHandle>,
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
        unique_id: UniqueId,
        handle: WriteSplitDataTaskHandle,
    ) -> WSResult<()> {
        // 检查是否已存在
        if self.handles.contains_key(&unique_id) {
            return Err(WSError::WsDataError(WsDataErr::WriteDataFailed {
                unique_id,
            }));
        }

        // 存储句柄
        self.handles.insert(unique_id, handle);
        Ok(())
    }

    // 获取已存在的任务句柄
    pub fn get_handle(&self, unique_id: &UniqueId) -> Option<WriteSplitDataTaskHandle> {
        self.handles.get(unique_id).map(|h| h.clone())
    }

    // 移除任务句柄
    pub fn remove_handle(&self, unique_id: &UniqueId) {
        self.handles.remove(unique_id);
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

### 2. BatchTransfer 的 new 方法
