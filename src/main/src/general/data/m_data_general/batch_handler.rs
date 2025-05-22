use super::{DataGeneral, DataSetMetaV2};
use crate::{
    general::{
        data::m_data_general::dataitem::WriteSplitDataTaskGroup,
        network::{
            m_p2p::RPCResponsor,
            proto::{self, BatchDataRequest, BatchDataResponse, DataItem},
            proto_ext::{DataItemExt, ProtoExtDataItem},
        },
    },
    result::{WSError, WSResult, WSResultExt, WsDataError},
    sys::NodeID,
};
use async_trait::async_trait;
use std::{
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{futures::Notified, oneshot, Mutex, Notify, RwLock};
use tracing;

/// 默认数据块大小 (1MB)
pub const DEFAULT_BLOCK_SIZE: usize = 1 * 1024 * 1024;

enum BatchDoneMsg {
    Done {
        version: u64,
        request_id: proto::BatchRequestId, // as the context index
        required_result: Option<proto::DataItem>,
    },
    Error {
        version: u64,
        error_message: String,
        request_id: proto::BatchRequestId,
        //required_result: Option<proto::DataItem>,
    },
    Replaced {
        version: u64,
        request_id: proto::BatchRequestId,
        // required_result: Option<proto::DataItem>,
    },
}

#[async_trait]
trait BatchDoneResponsor: Send {
    async fn done(&self, msg: BatchDoneMsg);
}

#[derive(Clone)]
struct BatchInProcessResponsor {
    /// use option bacause maybe don't need the return in delete mode
    tx: tokio::sync::mpsc::Sender<Option<proto::DataItem>>,
}

impl BatchInProcessResponsor {
    pub fn new_pair() -> (Self, tokio::sync::mpsc::Receiver<Option<proto::DataItem>>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        (Self { tx }, rx)
    }
}

#[async_trait]
impl BatchDoneResponsor for BatchInProcessResponsor {
    async fn done(&self, msg: BatchDoneMsg) {
        match msg {
            BatchDoneMsg::Done {
                required_result, ..
            } => self.tx.send(required_result).await.unwrap(),
            BatchDoneMsg::Error {
                request_id,
                error_message,
                ..
            } => {
                // drop the channel, so the receiver will know the error
                panic!("batch one recev {:?} error: {}", request_id, error_message);
            }
            BatchDoneMsg::Replaced { .. } => {}
        }
        // self.tx.send(()).await.unwrap();
    }
}

#[async_trait]
impl BatchDoneResponsor for RPCResponsor<BatchDataRequest> {
    async fn done(&self, msg: BatchDoneMsg) {
        let (request_id, success, error_message, version) = match msg {
            BatchDoneMsg::Done {
                request_id,
                version,
                ..
            } => (request_id, true, String::new(), version),
            BatchDoneMsg::Error {
                error_message,
                request_id,
                version,
                ..
            } => (request_id, false, error_message, version),
            BatchDoneMsg::Replaced {
                request_id,
                version,
                ..
            } => (request_id, true, String::new(), version),
        };
        let _ = self
            .send_resp(BatchDataResponse {
                request_id: Some(request_id),
                version,
                success,
                error_message,
            })
            .await
            .todo_handle("send back batch data response");
    }

    // async fn replaced(&self, request_id: proto::BatchRequestId, version: u64) {
    //     if let Err(e) = self
    //         .send_resp(BatchDataResponse {
    //             request_id: Some(request_id), // 这里需要正确的 request_id
    //             version,                      // 这里需要正确的版本号
    //             success: true,
    //             error_message: String::new(),
    //         })
    //         .await
    //     {
    //         tracing::error!("Failed to respond to old request: {}", e);
    //     }
    // }
}
/// 共享状态,用于记录最新的请求响应器
/// 当收到新的请求时,会更新响应器并自动处理旧的请求
#[derive(Clone)]
pub struct SharedWithBatchHandler {
    /// 当前活跃的响应器
    /// 使用 Arc<Mutex> 保证线程安全
    responsor: Arc<Mutex<Option<Box<dyn BatchDoneResponsor>>>>, //RPCResponsor<BatchDataRequest>>
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
    pub async fn update_responsor(
        &self,
        request_id: proto::BatchRequestId,
        version: u64,
        responsor: Box<dyn BatchDoneResponsor>,
    ) {
        let mut guard = self.responsor.lock().await;
        if let Some(old_responsor) = guard.take() {
            // 旧的responsor直接返回成功
            old_responsor
                .done(BatchDoneMsg::Replaced {
                    version,
                    request_id,
                })
                .await;
        }
        *guard = Some(responsor);
    }

    /// 获取最终的响应器
    /// 用于在所有数据都写入完成后发送最终响应
    pub async fn get_final_responsor(&self) -> Option<Box<dyn BatchDoneResponsor>> {
        self.responsor.lock().await.take()
    }
}

/// 批量数据传输状态
/// 用于管理单个批量数据传输请求的生命周期
pub struct BatchReceiveState {
    /// 写入任务句柄
    pub handle: RwLock<Option<super::dataitem::WriteSplitDataTaskHandle>>,
    /// 共享状态,用于处理请求响应
    pub shared: SharedWithBatchHandler,
    /// version, same as handle.version()
    pub version: u64,
}

impl BatchReceiveState {
    /// 创建新的批量数据传输状态
    ///
    /// # 参数
    /// * `handle` - 写入任务句柄
    /// * `shared` - 共享状态
    pub fn new(
        handle: super::dataitem::WriteSplitDataTaskHandle,
        shared: SharedWithBatchHandler,
    ) -> Self {
        Self {
            version: handle.version(),
            handle: RwLock::new(Some(handle)),
            shared,
        }
    }
}

#[derive(Clone)]
pub enum GetOrDelType {
    Get,
    DelReturnData,
    DelReturnNoData,
}

impl GetOrDelType {
    pub fn return_data(&self) -> bool {
        matches!(self, GetOrDelType::Get | GetOrDelType::DelReturnData)
    }
    pub fn delete(&self) -> bool {
        matches!(
            self,
            GetOrDelType::DelReturnData | GetOrDelType::DelReturnNoData
        )
    }
}
// trait BatchRecvNotifier {}

impl DataGeneral {
    // 处理批量数据写入请求
    pub async fn rpc_handle_batch_data(
        &self,
        responsor: RPCResponsor<proto::BatchDataRequest>,
        req: proto::BatchDataRequest,
    ) -> WSResult<()> {
        tracing::debug!(
            "rpc_handle_batch_data with batchid({:?})",
            req.request_id.clone().unwrap()
        );
        // 预先克隆闭包外需要的字段
        // let block_index = req.block_index;
        // let data = req.data.clone();
        // let request_id = req.request_id.clone().unwrap();

        self.handle_batch_data_one(
            req.unique_id,
            req.request_id.unwrap(),
            req.total_size as usize,
            req.block_type.unwrap(),
            req.version,
            req.block_index as usize,
            Box::new(responsor),
            req.data_item_idx as u8,
        )
        .await?;

        Ok(())
    }

    pub async fn handle_batch_data_one(
        &self,
        unique_id: Vec<u8>,
        request_id: proto::BatchRequestId,
        total_size: usize,
        partial_block: proto::DataItem,
        version: u64,
        block_index: usize,
        responsor: Box<dyn BatchDoneResponsor>,
        _item_idx: u8,
    ) -> WSResult<()> {
        // 1. 查找或创建状态
        let view = self.view.clone();
        let data_type = partial_block.get_data_type();
        let request_id_init = request_id.clone();
        let state = match self
            .batch_receive_states
            .get_or_init(request_id.clone(), async move {
                // 创建任务组和句柄
                let (mut group, handle) = match WriteSplitDataTaskGroup::new(
                    &view,
                    unique_id.clone(),
                    total_size,
                    data_type,
                    version,
                )
                .await
                {
                    Ok((group, handle)) => (group, handle),
                    Err(e) => {
                        tracing::error!("Failed to create task group: {:?}", e);
                        return Err(e);
                    }
                };

                // // // 启动process_tasks
                // let _ = tokio::spawn(async move {

                // });

                let state = Arc::new(BatchReceiveState::new(
                    handle,
                    SharedWithBatchHandler::new(),
                ));
                let state_clone = state.clone();

                // response task
                let _ = tokio::spawn(async move {
                    tracing::debug!("rpc_handle_batch_data response task started");
                    let resdata = match group.process_tasks().await {
                        Ok(item) => item,
                        Err(e) => {
                            panic!("Failed to process tasks: {}", e);
                        }
                    };
                    // 等待所有任务完成
                    // let resdata = match waiter.await {
                    //     Ok(data) => {
                    //         tracing::debug!(
                    //             "rpc_handle_batch_data response task wait all tasks done"
                    //         );
                    //         data
                    //     }
                    //     Err(e) => {
                    //         tracing::error!("Failed to wait for tasks: {}", e);
                    //         todo!("use responsor to send error response");
                    //         return;
                    //     }
                    // };

                    tracing::debug!("rpc_handle_batch_data response task wait all tasks done");

                    // 发送最终响应
                    if let Some(final_responsor) = state_clone.shared.get_final_responsor().await {
                        // if let Err(e) =
                        final_responsor
                            .done(BatchDoneMsg::Done {
                                request_id: request_id_init.clone(),
                                version: state_clone.version,
                                required_result: Some(resdata),
                            })
                            .await;
                        // {
                        //     tracing::error!("Failed to send final response: {}", e);
                        // }
                    }

                    // 清理状态
                    let _ = view
                        .data_general()
                        .batch_receive_states
                        .remove(&request_id_init);
                });

                Ok(state)
            })
            .await
        {
            Err(e) => {
                return Err(WSError::WsDataError(WsDataError::BatchTransferError {
                    request_id: request_id.clone(),
                    msg: format!("Failed to initialize batch state: {}", e),
                }))
            }
            Ok(state) => state,
        };

        tracing::debug!("rpc_handle_batch_data ready with write_split_data_task_group");

        // 2. 提交分片数据
        // let data_item = proto::DataItem {
        //     data_item_dispatch: Some(proto::data_item::DataItemDispatch::RawBytes(data)),
        //     ..Default::default()
        // };

        let bytes = partial_block.into_data_bytes();
        tracing::debug!(
            "submit_split with data split idx: {}, at node: {}, partial {:?}",
            block_index,
            self.view.p2p().nodes_config.this_node(),
            &bytes[0..30]
        );
        let keepon = {
            let handle_read = state.handle.read().await;
            let Some(handle) = handle_read.as_ref() else {
                return Err(WSError::WsDataError(WsDataError::DataSplitTaskError {
                    msg: format!("Failed to submit task: submit_split count to the end"),
                }));
            };
            handle
                .submit_split(block_index as usize * DEFAULT_BLOCK_SIZE, bytes)
                .await?
        };

        if !keepon {
            // remove state.handle, make the mem count to one
            let _ = state.handle.write().await.take();
        }

        // 3. 更新响应器
        state
            .shared
            .update_responsor(request_id, state.version, responsor)
            .await;

        Ok(())
    }

    fn next_batch_id(&self, nodeid: NodeID) -> proto::BatchRequestId {
        static NEXT_BATCH_ID: AtomicU64 = AtomicU64::new(1); // 从1开始,保留0作为特殊值
        proto::BatchRequestId {
            node_id: nodeid,
            sequence: NEXT_BATCH_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub async fn batch_get_or_del_data(
        &self,
        unique_id: Vec<u8>,
        dataset_meta: &DataSetMetaV2,
        idxs: &Vec<u8>,
        opetype: GetOrDelType,
    ) -> WSResult<Option<Vec<proto::DataItem>>> {
        let mut waiters = Vec::new();
        for &idx in idxs {
            // check cache first

            // allocate a batch request id
            let splits = &dataset_meta.datas_splits[idx as usize];
            let request_id = self.next_batch_id(self.view.p2p().nodes_config.this_node());
            let total_size = splits.total_size();
            let (responsor, waiter) = BatchInProcessResponsor::new_pair();
            waiters.push(waiter);
            if let Some(fp_) = &dataset_meta.filepath[idx as usize] {
                // try check the target file
                let fp = self.view.os().file_path.join(fp_);
                if fp.exists() {
                    responsor
                        .done(BatchDoneMsg::Done {
                            version: dataset_meta.version,
                            request_id: request_id.clone(),
                            required_result: Some(DataItem::new_file_data(&*fp_, fp.is_dir())),
                        })
                        .await;
                    tracing::debug!(
                        "access cached file uid({:?}) idx({}) path({:?}) success",
                        &unique_id,
                        idx,
                        fp
                    );
                    continue;
                } else {
                    tracing::debug!(
                        "access cached file uid({:?}) idx({}) path({:?}) failed, will get from remote",
                        &unique_id,
                        idx,
                        fp
                    );
                }
                // if fp.is_dir() {
                //     responsor
                //         .done(BatchDoneMsg::Done {
                //             version: dataset_meta.version,
                //             request_id: request_id,
                //             required_result: Some(DataItem::new_file_data(&*fp_, true)),
                //         })
                //         .await;
                //     continue;
                // } else {
            } else {
                // try check the cache
                let cache_data = self.cache_in_memory.get(&unique_id);
                if let Some(cache_data) = cache_data {
                    responsor
                        .done(BatchDoneMsg::Done {
                            version: dataset_meta.version,
                            request_id: request_id.clone(),
                            required_result: Some(DataItem::new_mem_data(cache_data)),
                        })
                        .await;
                    tracing::debug!(
                        "access cached mem uid({:?}) idx({}) success",
                        &unique_id,
                        idx
                    );
                    continue;
                } else {
                    tracing::debug!(
                        "access cached mem uid({:?}) idx({}) failed, will get from remote",
                        &unique_id,
                        idx
                    );
                }
            }

            tracing::debug!(
                "batch_get_or_del_data with receving uid({}) data idx({}) with length({})",
                unique_id.len(),
                idx,
                total_size
            );

            // 发起多个batch read, 拿到返回结果后并行调用 rpc_handle_batch_data
            for (split_idx, split) in splits.splits.iter().enumerate() {
                // get one data request
                let view = self.view.clone();
                let unique_id = unique_id.clone();
                let request_id = request_id.clone();
                let opetype = opetype.clone();
                let responsor = responsor.clone();
                let version = dataset_meta.version;
                // let node_id = split.node_id as NodeID;
                let split = split.clone();
                // let data_offset = split.data_offset;
                let _ = tokio::spawn(async move {
                    // first read the partial block from target node
                    let mut partial_block = view
                        .data_general()
                        .rpc_call_get_data
                        .call(
                            view.p2p(),
                            split.node_id,
                            proto::GetOneDataRequest {
                                unique_id: unique_id.clone(),
                                idxs: vec![idx as u32],
                                delete: opetype.delete(),
                                return_data: opetype.return_data(),
                            },
                            Some(Duration::from_secs(60)),
                        )
                        .await
                        .unwrap_or_else(|err| {
                            panic!("batch one fetch {:?} error: {}", request_id, err);
                        });

                    if partial_block.data.len() != 1 {
                        let err_msg = format!(
                            "batch one fetch partial_block wrong count, key({:?}), idx({}), count({}), supposed split range({}-{})",
                            std::str::from_utf8(&unique_id).map(|v|v.to_string()).unwrap_or(format!("{:?}", unique_id)),
                            idx,
                            partial_block.data.len(),
                            split.data_offset,
                            split.data_offset + split.data_size
                        );
                        tracing::warn!("{}", err_msg);
                        responsor
                            .done(BatchDoneMsg::Error {
                                version: version,
                                error_message: err_msg,
                                request_id: request_id.clone(),
                                // required_result: None,
                            })
                            .await;
                        return;
                    }

                    tracing::debug!(
                        "batch one recev partial_block, idx({}), type({:?}), size({})",
                        idx,
                        partial_block.data[0].get_data_type(),
                        partial_block.data[0].inmem_size()
                    );

                    if !partial_block.success {
                        tracing::error!(
                            "batch one recev {:?} error: {}",
                            request_id,
                            partial_block.message
                        );
                    }

                    if opetype.return_data() {
                        view.data_general()
                            .handle_batch_data_one(
                                unique_id.clone(),
                                request_id.clone(),
                                total_size,
                                partial_block.data.pop().unwrap(),
                                version,
                                split_idx,
                                Box::new(responsor),
                                idx,
                            )
                            .await
                            .unwrap();
                    }
                });
            }
        }

        let res = if opetype.return_data() {
            let mut results = Vec::new();
            for (i, waiter) in waiters.iter_mut().enumerate() {
                let Some(res) = waiter.recv().await else {
                    tracing::error!(
                        "batch one recev error, uid({:?}), idx({})",
                        unique_id,
                        idxs[i]
                    );
                    return Err(WSError::WsDataError(WsDataError::DataSplitTaskError {
                        msg: format!("Failed to submit task: submit_split count to the end"),
                    }));
                };
                results.push(res.unwrap());
            }
            Some(results)
        } else {
            None
        };
        Ok(res)
    }
}
