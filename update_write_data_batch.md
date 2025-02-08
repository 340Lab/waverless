# 更新写入数据批处理函数

## 1. 删除代码分析（>500字）

我们需要删除以下代码：

```rust
// 在 src/main/src/general/data/m_data_general/mod.rs 中
async fn transfer_data(
    &self,
    node_id: NodeID,
    unique_id: Vec<u8>,
    version: u64,
    data: proto::DataItem,
    data_item_idx: usize,
    batch_size: usize,
) -> WSResult<()>
```

删除原因分析：
1. 功能重叠：transfer_data 函数与设计文档中的 batch_transfer 函数功能重叠，但实现不符合规范
2. 参数不一致：
   - transfer_data 使用了 data_item_idx 和 batch_size 参数，这在设计中并不需要
   - 缺少了 DataSource trait 的抽象
3. 错误处理：
   - 原实现的错误处理不符合四层架构的要求
   - 缺少对版本号的验证
4. 并发控制：
   - 原实现使用了固定的信号量大小(10)
   - 新设计中使用32作为并发限制
5. 代码组织：
   - 原实现将所有逻辑放在一个函数中
   - 新设计通过 DataSource trait 实现更好的抽象
6. 资源管理：
   - 原实现没有很好地管理资源生命周期
   - 新设计通过 Arc<DataSource> 更好地管理资源

删除这段代码不会影响其他功能，因为：
1. write_data_batch 函数会调用新的 batch_transfer 函数
2. 错误处理逻辑会更加完善
3. 并发控制更加合理
4. 代码结构更加清晰

## 2. 新增代码

### 2.1 DataSource Trait
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
```

### 2.2 批量传输函数
```rust
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

### 2.3 更新 write_data_batch 函数
```rust
pub async fn write_data_batch(
    &self,
    unique_id: &[u8],
    version: u64,
    data: proto::DataItem,
    data_item_idx: usize,
    node_id: NodeID,
    batch_size: usize,
) -> WSResult<()> {
    // 创建 DataSource
    let data_source = Arc::new(DataItemSource::new(data));
    
    // 调用 batch_transfer 函数处理数据传输
    batch_transfer(
        unique_id.to_vec(),
        version,
        node_id,
        data_source,
        self.view.clone(),
    ).await
}
```

## 3. 实现说明

1. 严格按照设计文档实现
2. 保持四层架构设计
3. 遵循错误处理规范
4. 使用规范中定义的数据类型
5. 保持代码清晰可维护

## 4. 下一步计划

1. 实现 DataItemSource 结构体
2. 添加必要的单元测试
3. 完善错误处理
4. 添加详细的文档注释
