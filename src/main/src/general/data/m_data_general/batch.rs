/// Batch Data Transfer Interface
///
/// # Design Overview
/// The batch interface is designed for efficient large-scale data transfer from data holders (writers)
/// to the system. It differs from the regular data interface in several key aspects:
///
/// ## Batch Interface
/// - Purpose: Optimized for data holders to push complete datasets
/// - Key Feature: Supports streaming transfer during data writing process
/// - Use Case: Allows transfer before local sharding is complete
/// - Operation: Uses fixed-size block transfer with real-time processing
///
/// ## Data Interface (For Comparison)
/// - Purpose: General-purpose data read/write operations
/// - Write Flow: Data is sharded and distributed across nodes
/// - Read Flow: Shards are collected from nodes and reassembled
/// - Operation: Requires complete data and consistency checks
///
/// # Implementation Details
/// The batch interface implements this through:
/// - Efficient block-based streaming transfer
/// - Concurrent processing of received blocks
/// - Support for both memory and file-based transfers
/// - Real-time block validation and assembly
///
/// For detailed implementation of the regular data interface, see the data.rs module.
use super::*;
use crate::general::network::proto;
use base64::Engine;
use crate::general::network::m_p2p::RPCResponsor;
use tokio::io::AsyncWriteExt;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use std::ops::Range;

impl proto::DataItem {
    pub fn size(&self) -> usize {
        match &self.data_item_dispatch {
            Some(proto::data_item::DataItemDispatch::RawBytes(bytes)) => bytes.len(),
            Some(proto::data_item::DataItemDispatch::File(file_data)) => file_data.file_content.len(),
            None => 0,
        }
    }
}

/// 管理单个批量传输的状态
pub(super) struct BatchTransfer {
    pub unique_id: Vec<u8>,
    pub version: u64,
    pub block_type: proto::BatchDataBlockType,
    pub total_blocks: u32,
    // 使用 channel 进行数据传输
    data_sender: mpsc::Sender<WSResult<(DataSplitIdx, proto::DataItem)>>,
    // 写入任务
    write_task: JoinHandle<WSResult<proto::DataItem>>,
    // 完成通知 channel
    pub tx: Option<mpsc::Sender<WSResult<proto::DataItem>>>,
}

impl BatchTransfer {
    pub async fn new(
        unique_id: Vec<u8>,
        version: u64,
        block_type: proto::BatchDataBlockType,
        total_blocks: u32,
        block_size: usize,
        tx: mpsc::Sender<WSResult<proto::DataItem>>,
    ) -> WSResult<Self> {
        // 创建数据传输 channel
        let (data_sender, data_receiver) = mpsc::channel(total_blocks as usize);
        
        // 计算数据分片
        let splits = Self::calculate_splits(total_blocks as usize * block_size, block_size);
        
        // 为异步任务克隆 unique_id
        let unique_id_for_task = unique_id.clone();
        
        // 创建写入任务
        let write_task = tokio::spawn(async move {
            let group = WriteSplitDataTaskGroup::new(
                unique_id_for_task,
                splits,
                data_receiver,
                block_type,
            ).await?;
            
            group.join().await
        });

        Ok(Self {
            unique_id,
            version,
            block_type,
            total_blocks,
            data_sender,
            write_task,
            tx: Some(tx),
        })
    }

    pub async fn add_block(&self, index: u32, data: Vec<u8>) -> WSResult<bool> {
        if index >= self.total_blocks {
            return Ok(false);
        }

        // 通过 channel 发送数据块
        self.data_sender.send(Ok((
            index as usize,
            proto::DataItem::new_raw_bytes(data),
        ))).await.map_err(|_| WsDataError::BatchTransferError {
            unique_id: self.unique_id.clone(),
            msg: "failed to send data block".to_string(),
        })?;

        Ok(index == self.total_blocks - 1)
    }

    pub async fn complete(mut self) -> WSResult<()> {
        // 定义错误转换函数
        let join_error = |e| WsDataError::BatchTransferError {
            unique_id: self.unique_id.clone(),
            msg: format!("write task join failed: {}", e),
        };
        
        let write_error = |e| WsDataError::BatchTransferError {
            unique_id: self.unique_id.clone(),
            msg: format!("write data failed: {}", e),
        };
        
        let send_error = || WsDataError::BatchTransferError {
            unique_id: self.unique_id.clone(),
            msg: "send result failed".to_string(),
        };

        drop(self.data_sender);
        
        if let Some(tx) = self.tx.take() {
            let join_result = self.write_task.await
                .map_err(join_error)?;
                
            let data_item = join_result
                .map_err(write_error)?;
                
            tx.send(Ok(data_item)).await
                .map_err(|_| send_error())?;
        }
        Ok(())
    }

    // 辅助函数：计算数据分片
    fn calculate_splits(total_size: usize, block_size: usize) -> Vec<Range<usize>> {
        let mut splits = Vec::new();
        let mut offset = 0;
        while offset < total_size {
            let end = (offset + block_size).min(total_size);
            splits.push(offset..end);
            offset = end;
        }
        splits
    }
}

/// 管理所有进行中的批量传输
pub(super) struct BatchManager {
    transfers: DashMap<proto::BatchRequestId, BatchTransfer>,
    sequence: AtomicU64,
}

impl BatchManager {
    pub fn new() -> Self {
        Self {
            transfers: DashMap::new(),
            sequence: AtomicU64::new(0),
        }
    }

    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn create_transfer(
        &self,
        unique_id: Vec<u8>,
        version: u64,
        block_type: proto::BatchDataBlockType,
        total_blocks: u32,
        tx: mpsc::Sender<WSResult<proto::DataItem>>,
    ) -> WSResult<proto::BatchRequestId> {
        let request_id = proto::BatchRequestId {
            node_id: 0, // TODO: Get from config
            sequence: self.next_sequence(),
        };

        let transfer = BatchTransfer::new(
            unique_id.clone(),
            version,
            block_type,
            total_blocks,
            1024 * 1024, // 1MB block size
            tx,
        ).await?;

        self.transfers.insert(request_id.clone(), transfer);
        Ok(request_id)
    }

    pub async fn handle_block(
        &self,
        request_id: proto::BatchRequestId,
        block_index: u32,
        data: Vec<u8>,
    ) -> WSResult<bool> {
        if let Some(transfer) = self.transfers.get(&request_id) {
            let is_complete = transfer.add_block(block_index, data).await?;
            if is_complete {
                // Remove and complete the transfer
                if let Some((_, transfer)) = self.transfers.remove(&request_id) {
                    transfer.complete().await?
                }
            }
            Ok(is_complete)
        } else {
            Err(WsDataError::BatchTransferNotFound {
                node_id: request_id.node_id,
                sequence: request_id.sequence,
            }
            .into())
        }
    }
}

impl DataGeneral {
    /// 发起批量数据传输
    pub(super) async fn call_batch_data(
        &self,
        node_id: NodeID,
        unique_id: Vec<u8>,
        version: u64,
        data: proto::DataItem,
        block_type: proto::BatchDataBlockType,
    ) -> WSResult<proto::BatchDataResponse> {
        // 将数据分割成块
        let block_size = 1024 * 1024; // 1MB per block
        let data_bytes = match data {
            proto::DataItem { data_item_dispatch: Some(proto::data_item::DataItemDispatch::RawBytes(bytes)) } => bytes,
            proto::DataItem { data_item_dispatch: Some(proto::data_item::DataItemDispatch::File(file_data)) } => file_data.file_content,
            _ => return Err(WsDataError::InvalidDataType.into()),
        };

        let total_blocks = (data_bytes.len() + block_size - 1) / block_size;
        
        // 创建channel用于接收响应
        let (tx, mut rx) = mpsc::channel(1);
        
        // 创建传输任务
        let request_id = self.batch_manager.create_transfer(
            unique_id.clone(),
            version,
            block_type,
            total_blocks as u32,
            tx,
        ).await?;

        // 发送数据块
        for (i, chunk) in data_bytes.chunks(block_size).enumerate() {
            let request = proto::BatchDataRequest {
                request_id: Some(request_id.clone()),
                block_type: block_type as i32,
                block_index: i as u32,
                data: chunk.to_vec(),
                operation: proto::DataOpeType::Write as i32,
                unique_id: unique_id.clone(),
                version,
            };

            let response = self
                .rpc_call_batch_data
                .call(
                    self.view.p2p(),
                    node_id,
                    request,
                    Some(Duration::from_secs(30)),
                )
                .await?;

            if !response.success {
                return Ok(response);
            }
        }

        // 等待所有块处理完成
        match rx.recv().await {
            Some(Ok(_data_item)) => Ok(proto::BatchDataResponse {
                request_id: Some(request_id),
                success: true,
                error_message: String::new(),
                version,
            }),
            Some(Err(err)) => Ok(proto::BatchDataResponse {
                request_id: Some(request_id),
                success: false,
                error_message: err.to_string(),
                version,
            }),
            None => Ok(proto::BatchDataResponse {
                request_id: Some(request_id),
                success: false,
                error_message: "transfer channel closed unexpectedly".to_string(),
                version,
            }),
        }
    }

    /// 处理批量数据请求
    
    pub(super) async fn rpc_handle_batch_data(
        &self,
        responsor: RPCResponsor<proto::BatchDataRequest>,
        req: proto::BatchDataRequest,
    ) -> WSResult<()> {
        // Step 1: 获取数据元信息
        let meta = match self.view.get_metadata(&req.unique_id, false).await {
            Ok(meta) => meta,
            Err(err) => {
                tracing::warn!("get data meta failed: {}", err);
                responsor
                    .send_resp(proto::BatchDataResponse {
                        request_id: req.request_id,
                        success: false,
                        error_message: format!("get data meta failed: {}", err),
                        version: 0,
                    })
                    .await?;
                return Ok(());
            }
        };

        // Step 2: 复用 get_data 逻辑获取数据
        let get_arg = GetOrDelDataArg {
            meta: Some(meta.clone()),
            unique_id: req.unique_id.clone(),
            ty: GetOrDelDataArgType::All,
        };
        
        let data_result = match self.get_or_del_data(get_arg).await {
            Ok((_, data)) => data,
            Err(err) => {
                tracing::warn!("get data failed: {}", err);
                responsor
                    .send_resp(proto::BatchDataResponse {
                        request_id: req.request_id,
                        success: false,
                        error_message: format!("get data failed: {}", err),
                        version: meta.version,
                    })
                    .await?;
                return Ok(());
            }
        };

        // Step 3: 创建数据分片并设置写入任务
        let mut splits = Vec::new();
        let mut offset = 0;
        
        for item in data_result.values() {
            let size = item.size();
            splits.push(offset..offset + size);
            offset += size;
        }

        // 创建channel用于传输数据
        let (tx, rx) = mpsc::channel(splits.len());
        
        // 发送数据到channel
        for (idx, item) in data_result.into_iter() {
            if let Err(err) = tx.send(Ok((idx as usize, item))).await {
                tracing::error!("send data to channel failed: {}", err);
                responsor
                    .send_resp(proto::BatchDataResponse {
                        request_id: req.request_id,
                        success: false,
                        error_message: format!("internal error: {}", err),
                        version: meta.version,
                    })
                    .await?;
                return Ok(());
            }
        }
        drop(tx); // 关闭发送端

        // Step 4: 根据请求类型选择写入方式并执行
        let task_group = match WriteSplitDataTaskGroup::new(
            req.unique_id,
            splits,
            rx,
            proto::BatchDataBlockType::from_i32(req.block_type).unwrap_or(proto::BatchDataBlockType::Memory),
        )
        .await
        {
            Ok(group) => group,
            Err(err) => {
                tracing::warn!("create write task group failed: {}", err);
                responsor
                    .send_resp(proto::BatchDataResponse {
                        request_id: req.request_id,
                        success: false,
                        error_message: format!("create write task group failed: {}", err),
                        version: meta.version,
                    })
                    .await?;
                return Ok(());
            }
        };

        // Step 5: 等待所有写入任务完成
        match task_group.join().await {
            Ok(_) => {
                responsor
                    .send_resp(proto::BatchDataResponse {
                        request_id: req.request_id,
                        success: true,
                        error_message: String::new(),
                        version: meta.version,
                    })
                    .await?;
                Ok(())
            }
            Err(err) => {
                tracing::warn!("write data failed: {}", err);
                responsor
                    .send_resp(proto::BatchDataResponse {
                        request_id: req.request_id,
                        success: false,
                        error_message: format!("write data failed: {}", err),
                        version: meta.version,
                    })
                    .await?;
                Ok(())
            }
        }
    }
}
