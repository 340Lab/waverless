# 更新 batch_transfer 函数

## 1. 改动目标
更新 batch_transfer 函数，使其严格遵循设计文档规范。

## 2. 相关文件
1. `/root/prjs/waverless/src/main/src/general/data/m_data_general/mod.rs`
   - batch_transfer 函数
   - write_data_batch 函数
   - DataItemSource 结构

## 3. 设计文档分析
1. review.md:
   - 保持使用 dyn trait 接口
   - 使用新的错误类型 WsDataError::BatchTransferFailed
   - 不删除现有功能代码

2. design.canvas:
   - batch_sender_group 组件定义了接口规范
   - 使用 DEFAULT_BLOCK_SIZE 常量 (4MB)
   - 保持四层架构设计

## 4. 改动步骤
1. 添加块大小常量：
   ```rust
   /// 默认数据块大小 (4MB)
   const DEFAULT_BLOCK_SIZE: usize = 4 * 1024 * 1024;
   ```

2. 保持 batch_transfer 函数签名：
   ```rust
   async fn batch_transfer(
       unique_id: Vec<u8>,
       version: u64,
       target_node: NodeID,
       data: Arc<dyn DataSource + Send + Sync>,
       view: DataGeneralView,
   ) -> WSResult<()>
   ```

3. 使用正确的错误类型：
   ```rust
   WsDataError::BatchTransferFailed {
       request_id: proto::BatchRequestId {
           node_id: target_node as u32,
           sequence: block_idx as u32,
       },
       reason: String,
   }
   ```

## 5. 改动分析
1. 符合分层设计：
   - 接收层：保持 dyn trait 接口
   - 写入任务层：使用 DEFAULT_BLOCK_SIZE
   - 本地存储层：支持文件和内存数据
   - 结果返回层：使用新的错误类型

2. 保持兼容性：
   - 函数签名不变
   - 错误处理规范化
   - 分块大小标准化

## 6. 删除内容分析
本次改动不涉及删除操作，只是规范化和标准化现有代码。

## 7. 后续任务
1. 添加更多错误处理日志
2. 更新相关文档
3. 添加单元测试
