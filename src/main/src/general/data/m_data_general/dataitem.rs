use crate::general::data::m_data_general::DataItemIdx;
use crate::general::data::m_data_general::GetOrDelDataArgType;
use crate::general::network::proto;
use crate::general::network::proto_ext::ProtoExtDataItem;
use crate::result::WSError;
use crate::result::WSResult;
use crate::result::WsDataError;
use crate::result::WsIoErr;
use crate::result::WsRuntimeErr;
use base64::Engine;
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::btree_set;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use super::CacheModeVisitor;
use super::DataSplitIdx;

// iterator for wanted dataitem idxs
pub(super) enum WantIdxIter<'a> {
    PartialMany {
        iter: btree_set::Iter<'a, DataItemIdx>,
    },
    PartialOne {
        idx: DataItemIdx,
        itercnt: u8,
    },
    Other {
        ty: GetOrDelDataArgType,
        itercnt: u8,
        len: u8,
    },
}

impl<'a> WantIdxIter<'a> {
    pub(super) fn new(ty: &'a GetOrDelDataArgType, itemcnt: DataItemIdx) -> Self {
        match ty {
            GetOrDelDataArgType::PartialMany { idxs } => Self::PartialMany { iter: idxs.iter() },
            GetOrDelDataArgType::Delete | GetOrDelDataArgType::All => Self::Other {
                ty: ty.clone(),
                itercnt: 0,
                len: itemcnt,
            },
            GetOrDelDataArgType::PartialOne { idx } => Self::PartialOne { 
                idx: *idx,
                itercnt: 0,
            },
        }
    }
}

impl<'a> Iterator for WantIdxIter<'a> {
    type Item = DataItemIdx;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            WantIdxIter::PartialMany { iter, .. } => iter.next().map(|v| *v as DataItemIdx),
            WantIdxIter::PartialOne { idx, itercnt } => {
                if *itercnt == 0 {
                    *itercnt += 1;
                    Some(*idx)
                } else {
                    None
                }
            }
            WantIdxIter::Other { ty, itercnt, len } => match ty {
                GetOrDelDataArgType::Delete | GetOrDelDataArgType::All => {
                    if itercnt == len {
                        None
                    } else {
                        let ret = *itercnt;
                        *itercnt += 1;
                        Some(ret)
        }
    }
                GetOrDelDataArgType::PartialMany { .. }
                | GetOrDelDataArgType::PartialOne { .. } => {
                    panic!("PartialMany should be handled by iter")
}
            },
        }
    }
}

pub struct SharedMemHolder {
    data: Arc<Vec<u8>>,
}

impl SharedMemHolder {
    pub fn try_take_data(self) -> Option<Vec<u8>> {
        // SAFETY:
        // 1. We're only replacing the Arc with an empty Vec
        // 2. The original Arc will be dropped properly
        // 3. This is safe as long as this is the only reference to the Arc
        // unsafe {
        // let ptr = &self.data as *const Arc<Vec<u8>> as *mut Arc<Vec<u8>>;
        if Arc::strong_count(&self.data) == 1 {
            Some(Arc::try_unwrap(self.data).unwrap())
        } else {
            None
        }
    }
    // }
}

pub struct SharedMemOwnedAccess {
    data: Arc<Vec<u8>>,
    range: Range<usize>,
}

impl SharedMemOwnedAccess {
    pub unsafe fn as_bytes_mut(&self) -> &mut [u8] {
        // SAFETY:
        // 1. We have &mut self, so we have exclusive access to this data
        // 2. The underlying memory is valid for the entire Arc allocation
        let full_slice = unsafe {
            std::slice::from_raw_parts_mut(self.data.as_ptr() as *mut u8, self.data.len())
        };
        &mut full_slice[self.range.clone()]
    }
}

pub fn new_shared_mem(splits: &Vec<Range<usize>>) -> (SharedMemHolder, Vec<SharedMemOwnedAccess>) {
    let len = splits.iter().map(|range| range.len()).sum();
    let data = Arc::new(vec![0; len]);
    let owned_accesses = splits
        .iter()
        .map(|range| SharedMemOwnedAccess {
            data: Arc::clone(&data),
            range: range.clone(),
        })
        .collect();
    tracing::debug!("new_shared_mem, total_len: {}, splits: {:?}", len, splits);
    (SharedMemHolder { data }, owned_accesses)
}

pub enum WriteSplitDataTaskGroup {
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
    },
}

impl WriteSplitDataTaskGroup {
    pub async fn new(
        unique_id: UniqueId,
        splits: Vec<Range<usize>>,
        block_type: proto::BatchDataBlockType,
        version: u64,
    ) -> (Self, WriteSplitDataTaskHandle) {
        let expected_size = splits.iter().map(|range| range.len()).sum();
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
                    version,
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
                    version,
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

    async fn process_tasks(&mut self) -> WSResult<proto::DataItem> {
        let mut pending_tasks = FuturesUnordered::new();
        
        match self {
            Self::ToFile { tasks, .. } |
            Self::ToMem { tasks, .. } => {
                for task in tasks.drain(..) {
                    pending_tasks.push(task);
                }
            }
        }

        loop {
            // 1. 检查完成状态
            match self.try_complete()? {
                Some(item) => return Ok(item),
                None => {}  // 继续等待
            }

            // 2. 等待新任务或已有任务完成
            tokio::select! {
                Some(new_task) = match self {
                    Self::ToFile { rx, .. } |
                    Self::ToMem { rx, .. } => rx.recv()
                } => {
                    pending_tasks.push(new_task);
                }
                Some(completed_result) = pending_tasks.next() => {
                    if let Err(e) = completed_result {
                        tracing::error!("Task failed: {}", e);
                        return Err(WSError::WsDataError(WsDataError::BatchTransferFailed {
                            request_id: match self {
                                Self::ToFile { unique_id, .. } |
                                Self::ToMem { unique_id, .. } => unique_id.clone()
                            },
                            reason: format!("Task failed: {}", e)
                        }));
                    }
                    match self {
                        Self::ToFile { current_size, .. } |
                        Self::ToMem { current_size, .. } => {
                            *current_size += DEFAULT_BLOCK_SIZE;  // 每个任务写入一个块
                        }
                    }
                }
                None = match self {
                    Self::ToFile { rx, .. } |
                    Self::ToMem { rx, .. } => rx.recv()
                } => {
                    while let Some(completed_result) = pending_tasks.next().await {
                        if let Err(e) = completed_result {
                            tracing::error!("Task failed during cleanup: {}", e);
                            return Err(WSError::WsDataError(WsDataError::BatchTransferFailed {
                                request_id: match self {
                                    Self::ToFile { unique_id, .. } |
                                    Self::ToMem { unique_id, .. } => unique_id.clone()
                                },
                                reason: format!("Task failed during cleanup: {}", e)
                            }));
                        }
                        match self {
                            Self::ToFile { current_size, .. } |
                            Self::ToMem { current_size, .. } => {
                                *current_size += DEFAULT_BLOCK_SIZE;
                            }
                        }
                    }
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

    /// 检查写入完成状态
    /// 
    /// 返回:
    /// - Ok(Some(item)) - 写入完成,返回数据项
    /// - Ok(None) - 写入未完成
    /// - Err(e) - 写入出错
    fn try_complete(&self) -> WSResult<Option<proto::DataItem>> {
        match self {
            Self::ToFile { current_size, expected_size, file_path, unique_id, .. } => {
                if *current_size > *expected_size {
                    Err(WSError::WsDataError(WsDataError::BatchTransferError {
                        request_id: unique_id.clone(),
                        msg: format!("Written size {} exceeds expected size {}", current_size, expected_size)
                    }))
                } else if *current_size == *expected_size {
                    Ok(Some(proto::DataItem::new_file_data(file_path.clone())))
                } else {
                    Ok(None)
                }
            }
            Self::ToMem { current_size, expected_size, shared_mem, unique_id, .. } => {
                if *current_size > *expected_size {
                    Err(WSError::WsDataError(WsDataError::BatchTransferError {
                        request_id: unique_id.clone(),
                        msg: format!("Written size {} exceeds expected size {}", current_size, expected_size)
                    }))
                } else if *current_size == *expected_size {
                    Ok(Some(proto::DataItem::new_mem_data(shared_mem.clone())))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// 写入分片任务的句柄
/// 用于提交新的分片任务和等待任务完成
pub struct WriteSplitDataTaskHandle {
    /// 发送任务的通道
    tx: mpsc::Sender<tokio::task::JoinHandle<()>>,
    /// 写入类型(文件或内存)
    write_type: WriteSplitDataType,
    /// 数据版本号
    /// 用于防止数据覆盖和保证数据一致性:
    /// 1. 防止旧版本数据覆盖新版本数据
    /// 2. 客户端可以通过比较版本号确认数据是否最新
    version: u64,
}

impl WriteSplitDataTaskHandle {
    /// 获取当前数据版本号
    pub fn version(&self) -> u64 {
        self.version
    }

    /// 提交新的分片任务
    /// 
    /// # 参数
    /// * `idx` - 分片索引,表示数据在整体中的偏移位置
    /// * `data` - 分片数据
    /// 
    /// # 返回
    /// * `Ok(())` - 任务提交成功
    /// * `Err(e)` - 任务提交失败,可能是通道已关闭
    pub async fn submit_split(&self, idx: DataSplitIdx, data: proto::DataItem) -> WSResult<()> {
        let task = match &self.write_type {
            WriteSplitDataType::File { path } => {
                let path = path.clone();
                let offset = idx;
                let data = data.as_bytes().to_vec();
                // 启动异步任务写入文件
                // 使用 spawn 是因为文件 IO 可能比较慢,不应该阻塞当前任务
                tokio::spawn(async move {
                    if let Err(e) = tokio::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .open(&path)
                        .await
                        .and_then(|mut file| async move {
                            use tokio::io::{AsyncSeekExt, AsyncWriteExt};
                            file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
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
                let offset = idx;
                let data = data.as_bytes().to_vec();
                // 启动异步任务写入内存
                // 使用 spawn 是因为需要保证所有写入操作都在同一个线程上执行
                // 避免多线程并发写入同一块内存导致的数据竞争
                tokio::spawn(async move {
                    unsafe {
                        let slice = std::slice::from_raw_parts_mut(
                            mem.data.as_ptr() as *mut u8,
                            mem.data.len()
                        );
                        slice[offset..offset + data.len()].copy_from_slice(&data);
                    }
                })
            }
        };

        self.tx.send(task).await.map_err(|e| {
            tracing::error!("Failed to submit task: channel closed, idx: {:?}", idx);
            WSError::WsDataError(WsDataError::BatchTransferFailed {
                request_id: idx.into(),
                reason: "Failed to submit task: channel closed".to_string()
            })
        })
    }

    /// 等待所有已提交的写入任务完成
    /// 关闭发送端,不再接收新任务
    pub async fn wait_all_tasks(self) -> WSResult<()> {
        drop(self.tx);
        Ok(())
    }
}

/// 写入类型
/// 支持写入文件或内存两种模式
pub enum WriteSplitDataType {
    /// 文件写入模式
    File {
        /// 目标文件路径
        path: PathBuf,
    },
    /// 内存写入模式
    Mem {
        /// 共享内存区域
        shared_mem: SharedMemHolder,
    },
}
