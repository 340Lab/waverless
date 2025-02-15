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
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Semaphore;
use std::sync::Arc;
use std::time::Duration;
use crate::general::data::m_data_general::dataitem::DataItemSource;

impl proto::DataItem {
    pub fn size(&self) -> usize {
        match &self.data_item_dispatch {
            Some(proto::data_item::DataItemDispatch::RawBytes(bytes)) => bytes.len(),
            Some(proto::data_item::DataItemDispatch::File(file_data)) => file_data.file_content.len(),
            None => 0,
        }
    }
}

impl DataGeneral {
    /// 发起批量数据传输
    pub async fn call_batch_data(
        &self,
        node_id: NodeID,
        unique_id: Vec<u8>,
        version: u64,
        data: proto::DataItem,
    ) -> WSResult<proto::BatchDataResponse> {
        // 调用 batch_transfer 函数处理数据传输
        async fn batch_transfer(
            unique_id: Vec<u8>,
            version: u64,
            target_node: NodeID,
            data: Arc<DataItemSource>,
            view: DataGeneralView,
        ) -> WSResult<()> {
            let total_size = match data.as_ref() {
                DataItemSource::Memory { data } => data.len(),
                DataItemSource::File { path } => {
                    tokio::fs::metadata(path).await?.len() as usize
                }
            };
            let total_blocks = (total_size + DEFAULT_BLOCK_SIZE - 1) / DEFAULT_BLOCK_SIZE;
            let semaphore = Arc::new(Semaphore::new(32));
            let mut handles: Vec<tokio::task::JoinHandle<WSResult<()>>> = Vec::new();

            // 发送所有数据块
            for block_idx in 0..total_blocks {
                // 获取信号量许可
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let offset = block_idx as usize * DEFAULT_BLOCK_SIZE;
                let size = DEFAULT_BLOCK_SIZE.min(total_size - offset);

                // 读取数据块
                let block_data = match data.as_ref() {
                    DataItemSource::Memory { data } => data[offset..offset + size].to_vec(),
                    DataItemSource::File { path } => {
                        let mut file = tokio::fs::File::open(path).await?;
                        let mut buffer = vec![0; size];
                        let _ = file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
                        let _ = file.read_exact(&mut buffer).await?;
                        buffer
                    }
                };

                // 构造请求
                let request = proto::BatchDataRequest {
                    request_id: Some(proto::BatchRequestId {
                        node_id: target_node as u32,
                        sequence: block_idx as u64, // 修复：使用 u64
                    }),
                    dataset_unique_id: unique_id.clone(),
                    data_item_idx: 0, // 因为是整体传输，所以使用0
                    block_type: match data.as_ref() {
                        DataItemSource::Memory { .. } => proto::BatchDataBlockType::Memory as i32,
                        DataItemSource::File { .. } => proto::BatchDataBlockType::File as i32,
                    },
                    block_index: block_idx as u32,
                    data: block_data,
                    operation: proto::DataOpeType::Write as i32,
                    unique_id: unique_id.clone(),
                    version,
                    total_size: total_size as u64,
                };

                // 发送请求
                let view = view.clone();
                let handle = tokio::spawn(async move {
                    let _permit = permit; // 持有permit直到任务完成
                    let resp = view.data_general()
                        .rpc_call_batch_data
                        .call(
                            view.p2p(),
                            target_node,
                            request,
                            Some(Duration::from_secs(30)),
                        )
                        .await?;
                    
                    if !resp.success {
                        return Err(WsDataError::BatchTransferError {
                            request_id: proto::BatchRequestId {
                                node_id: target_node as u32,
                                sequence: block_idx as u64, // 修复：使用 u64
                            },
                            msg: resp.error_message,
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

        let data = Arc::new(DataItemSource::new(data));
        batch_transfer(unique_id.clone(), version, node_id, data, self.view.clone()).await?;

        Ok(proto::BatchDataResponse {
            request_id: Some(proto::BatchRequestId {
                node_id: node_id,
                sequence: 0,
            }),
            success: true,
            error_message: String::new(),
            version,
        })
    }
}
