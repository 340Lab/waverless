use crate::general::data::m_data_general::UniqueId;
use crate::general::network::proto;
use crate::general::data::m_data_general::{DataItemIdx, DataSplitIdx, GetOrDelDataArgType};
use crate::general::network::proto_ext::{NewPartialFileDataArg, ProtoExtDataItem};
use crate::result::{WSError, WSResult, WsDataError};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::btree_set;
use std::ops::Range;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::broadcast;
use tracing;
use base64::{engine::general_purpose::STANDARD, Engine as _};

const DEFAULT_BLOCK_SIZE: usize = 4096;

/// 用于遍历数据项索引的迭代器
#[derive(Debug)]
pub(super) enum WantIdxIter<'a> {
    /// 遍历多个指定索引
    PartialMany {
        iter: btree_set::Iter<'a, DataItemIdx>,
    },
    /// 遍历单个索引
    PartialOne {
        idx: DataItemIdx,
        itercnt: u8,
    },
    /// 遍历所有或删除操作的索引
    Other {
        ty: GetOrDelDataArgType,
        itercnt: u8,
        len: u8,
    },
}

impl<'a> WantIdxIter<'a> {
    /// 创建新的索引迭代器
    /// 
    /// # 参数
    /// * `ty` - 迭代类型
    /// * `itemcnt` - 数据项总数
    #[must_use]
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

/// 共享内存区域的持有者
/// 负责管理共享内存的所有权和生命周期
#[derive(Debug, Clone)]
pub struct SharedMemHolder {
    /// 共享内存数据
    data: Arc<Vec<u8>>,
}

impl SharedMemHolder {
    pub fn len(&self) -> usize {
        self.data.len()
    }

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

    pub fn as_raw_bytes(&self) -> Option<&[u8]> {
        Some(self.data.as_ref())
    }
}

impl From<SharedMemHolder> for Vec<u8> {
    fn from(holder: SharedMemHolder) -> Self {
        holder.as_raw_bytes().expect("Failed to get raw bytes").to_vec()
    }
}

/// 共享内存区域的访问者
/// 提供对特定范围内存的安全访问
pub struct SharedMemOwnedAccess {
    /// 共享内存数据
    data: Arc<Vec<u8>>,
    /// 访问范围
    range: Range<usize>,
}

impl SharedMemOwnedAccess {
    /// 获取可变字节切片
    /// 
    /// # Safety
    /// 调用者必须确保:
    /// 1. 没有其他线程同时访问这块内存
    /// 2. 访问范围不超过内存边界
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

/// 创建新的共享内存和访问者
/// 
/// # 参数
/// * `splits` - 内存分片范围列表
#[must_use]
pub fn new_shared_mem(splits: &[Range<usize>]) -> (SharedMemHolder, Vec<SharedMemOwnedAccess>) {
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

/// 计算数据分片范围
/// 
/// # 参数
/// * `total_size` - 总大小
/// 
/// # 返回
/// * `Vec<Range<usize>>` - 分片范围列表
#[must_use]
pub fn calculate_splits(total_size: usize) -> Vec<Range<usize>> {
    let total_blocks = (total_size + DEFAULT_BLOCK_SIZE - 1) / DEFAULT_BLOCK_SIZE;
    let mut splits = Vec::with_capacity(total_blocks);
    for i in 0..total_blocks {
        let start = i * DEFAULT_BLOCK_SIZE;
        let end = (start + DEFAULT_BLOCK_SIZE).min(total_size);
        splits.push(start..end);
    }
    splits
}

/// 写入类型
/// 支持写入文件或内存两种模式
#[derive(Debug, Clone)]
pub enum WriteSplitDataType {
    Dir{
        /// 接受的压缩文件形式
        zip_file: PathBuf,
        /// 解压后的文件路径
        path: PathBuf,
    },
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

/// 写入分片任务的结果
#[derive(Debug)]
pub struct WriteSplitTaskResult {
    /// 写入的数据大小
    pub written_size: usize,
}

/// 写入分片任务组
/// 管理一组相关的写入任务
#[derive(Debug)]
pub enum WriteSplitDataTaskGroup {
    /// 文件写入模式
    ToFile {
        /// 任务唯一标识
        unique_id: UniqueId,
        /// 目标文件路径
        file_path: PathBuf,
        /// 任务列表
        tasks: Vec<tokio::task::JoinHandle<WriteSplitTaskResult>>,
        /// 接收新任务的通道
        rx: mpsc::Receiver<tokio::task::JoinHandle<WriteSplitTaskResult>>,
        /// 预期总大小
        expected_size: usize,
        /// 当前已写入大小
        current_size: usize,
        /// 广播通道发送端，用于通知任务完成
        broadcast_tx: Arc<broadcast::Sender<()>>,
    },
    /// 内存写入模式
    ToMem {
        /// 任务唯一标识
        unique_id: UniqueId,
        /// 共享内存区域
        shared_mem: SharedMemHolder,
        /// 任务列表
        tasks: Vec<tokio::task::JoinHandle<WriteSplitTaskResult>>,
        /// 接收新任务的通道
        rx: mpsc::Receiver<tokio::task::JoinHandle<WriteSplitTaskResult>>,
        /// 预期总大小
        expected_size: usize,
        /// 当前已写入大小
        current_size: usize,
        /// 广播通道发送端，用于通知任务完成
        broadcast_tx: Arc<broadcast::Sender<()>>,
    },
}

impl WriteSplitDataTaskGroup {
    /// 创建新的任务组
    pub async fn new(
        unique_id: UniqueId,
        total_size: usize,
        block_type: proto::BatchDataBlockType,
        version: u64,
    ) -> WSResult<(Self, WriteSplitDataTaskHandle)> {
        let (tx, rx) = mpsc::channel(32);
        let (broadcast_tx, _) = broadcast::channel::<()>(32);
        let broadcast_tx = Arc::new(broadcast_tx);

        match block_type {
            proto::BatchDataBlockType::File => {
                let file_path = PathBuf::from(format!("{}.data", 
                    STANDARD.encode(&unique_id)));
                
                let handle = WriteSplitDataTaskHandle {
                    tx,
                    write_type: WriteSplitDataType::File {
                        path: file_path.clone(),
                    },
                    version,
                    broadcast_tx: broadcast_tx.clone(),
                };
                
                let group = Self::ToFile {
                    unique_id,
                    file_path,
                    tasks: Vec::new(),
                    rx,
                    expected_size: total_size,
                    current_size: 0,
                    broadcast_tx: broadcast_tx.clone(),
                };
                
                Ok((group, handle))
            }
            proto::BatchDataBlockType::Memory => {
                let shared_mem = SharedMemHolder {
                    data: Arc::new(vec![0; total_size]),
                };
                
                let handle = WriteSplitDataTaskHandle {
                    tx,
                    write_type: WriteSplitDataType::Mem {
                        shared_mem: shared_mem.clone(),
                    },
                    version,
                    broadcast_tx: broadcast_tx.clone(),
                };
                
                let group = Self::ToMem {
                    unique_id,
                    shared_mem,
                    tasks: Vec::new(),
                    rx,
                    expected_size: total_size,
                    current_size: 0,
                    broadcast_tx: broadcast_tx.clone(),
                };
                
                Ok((group, handle))
            }
        }
    }

    /// 处理所有写入任务
    /// 
    /// # 返回
    /// * `Ok(item)` - 所有数据写入完成,返回数据项
    /// * `Err(e)` - 写入过程中出错
    pub async fn process_tasks(&mut self) -> WSResult<proto::DataItem> {
        let mut pending_tasks: FuturesUnordered<tokio::task::JoinHandle<WriteSplitTaskResult>> = FuturesUnordered::new();
        
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
                    match completed_result {
                        Ok(result) => {
                            match self {
                                Self::ToFile { current_size, .. } |
                                Self::ToMem { current_size, .. } => {
                                    *current_size += result.written_size;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Task failed: {}", e);
                            return Err(WSError::WsDataError(WsDataError::BatchTransferTaskFailed {
                                reason: format!("Task failed: {}", e)
                            }));
                        }
                    }
                }
            }
        }
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
                        request_id: proto::BatchRequestId {
                            node_id: 0,  // 这里需要传入正确的node_id
                            sequence: 0,
                        },
                        msg: format!("Written size {} exceeds expected size {} for unique_id {:?}", 
                            current_size, expected_size, unique_id)
                    }))
                } else if *current_size == *expected_size {
                    Ok(Some(proto::DataItem::new_file_data(file_path.clone(), false)))
                } else {
                    Ok(None)
                }
            }
            Self::ToMem { current_size, expected_size, shared_mem, unique_id, .. } => {
                if *current_size > *expected_size {
                    Err(WSError::WsDataError(WsDataError::BatchTransferError {
                        request_id: proto::BatchRequestId {
                            node_id: 0,  // 这里需要传入正确的node_id
                            sequence: 0,
                        },
                        msg: format!("Written size {} exceeds expected size {} for unique_id {:?}", 
                            current_size, expected_size, unique_id)
                    }))
                } else if *current_size == *expected_size {
                    Ok(Some(proto::DataItem::new_raw_bytes(shared_mem.clone())))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// 写入分片任务的句柄
/// 用于提交新的分片任务和等待任务完成
#[derive(Clone)]
pub struct WriteSplitDataTaskHandle {
    /// 发送任务的通道
    tx: mpsc::Sender<tokio::task::JoinHandle<WriteSplitTaskResult>>,
    /// 写入类型(文件或内存)
    write_type: WriteSplitDataType,
    /// 数据版本号
    /// 用于防止数据覆盖和保证数据一致性:
    /// 1. 防止旧版本数据覆盖新版本数据
    /// 2. 客户端可以通过比较版本号确认数据是否最新
    version: u64,
    /// 广播通道发送端，用于通知任务完成
    broadcast_tx: Arc<broadcast::Sender<()>>,
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
                let data = data.as_raw_bytes().unwrap_or(&[]).to_vec();
                let written_size = data.len();
                tokio::spawn(async move {
                    let result = tokio::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .open(&path)
                        .await;
                    
                    match result {
                        Ok(mut file) => {
                            use tokio::io::{AsyncSeekExt, AsyncWriteExt};
                            if let Err(e) = async move {
                                // 验证seek结果
                                let seek_pos = file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
                                if seek_pos != offset as u64 {
                                    return Err(std::io::Error::new(
                                        std::io::ErrorKind::Other,
                                        format!("Seek position mismatch: expected {}, got {}", offset, seek_pos)
                                    ));
                                }
                                // write_all保证写入所有数据或返回错误
                                file.write_all(&data).await?;
                                Ok::<_, std::io::Error>(())
                            }.await {
                                tracing::error!("Failed to write file data at offset {}: {}", offset, e);
                                panic!("Failed to write file: {}", e);
                            }
                            WriteSplitTaskResult { written_size }
                        }
                        Err(e) => {
                            tracing::error!("Failed to open file at offset {}: {}", offset, e);
                            panic!("Failed to open file: {}", e);
                        }
                    }
                })
            }
            WriteSplitDataType::Mem { shared_mem } => {
                let mem = shared_mem.clone();
                let offset = idx;
                let Some(data) = data.as_raw_bytes().map(|data| data.to_vec()) else {
                    return Err(WSError::WsDataError(WsDataError::BatchTransferFailed {
                        request_id: proto::BatchRequestId {
                            node_id: 0,
                            sequence: 0,
                        },
                        reason: format!("mem data expected"),
                    }));
                };
                let written_size = data.len();
                tracing::debug!("submit_split: Mem, len:{}, target len:{}", data.len(), shared_mem.len());

                tokio::spawn(async move {
                    unsafe {
                        let slice = std::slice::from_raw_parts_mut(
                            mem.data.as_ptr() as *mut u8,
                            mem.data.len()
                        );
                        slice[offset..offset + data.len()].copy_from_slice(&data);
                    }
                    WriteSplitTaskResult { written_size }
                })
            }
        };

        // 发送到通道
        let _ = self.broadcast_tx.send(());
        self.tx.send(task).await.map_err(|e| {
            tracing::error!("Failed to submit task: channel closed, idx: {:?}, error: {}", idx, e);
            WSError::WsDataError(WsDataError::DataSplitTaskError {
                msg: format!("Failed to submit task: channel closed, error: {}", e)
            })
        })
    }

    /// 等待所有已提交的写入任务完成
    /// 关闭发送端,不再接收新任务
    pub async fn wait_all_tasks(&self) -> WSResult<()> {
        // 等待广播通知
        let mut rx = self.broadcast_tx.subscribe();
        rx.recv().await.map_err(|e| {
            tracing::error!("Failed to wait for tasks: {}", e);
            WSError::WsDataError(WsDataError::BatchTransferTaskFailed {
                reason: format!("Failed to wait for tasks: {}", e)
            })
        })?;
        
        Ok(())
    }
}

#[derive(Debug)]
pub enum DataItemSource {
    Memory {
        data: Vec<u8>,
    },
    File {
        path: PathBuf,
    },
}

impl DataItemSource {
    pub fn to_debug_string(&self) -> String {
        match self {
            Self::Memory { data } => {
                //limit range vec
                format!("Memory({:?})", data[0..10.min(data.len())].to_vec())
            }
            Self::File { path } => format!("File({})", path.to_string_lossy()),
        }
    }

    pub fn new(data: proto::DataItem) -> Self {
        match &data.data_item_dispatch {
            Some(proto::data_item::DataItemDispatch::RawBytes(bytes)) => Self::Memory {
                data: bytes.clone(),
            },
            Some(proto::data_item::DataItemDispatch::File(file_data)) => Self::File {
                path: file_data.file_name_opt.clone().into(),
            },
            _ => Self::Memory {
                data: Vec::new(),
            },
        }
    }

    pub async fn size(&self) -> WSResult<usize> {
        match self {
            DataItemSource::Memory { data } => Ok(data.len()),
            DataItemSource::File { path } => {
                let metadata = tokio::fs::metadata(path).await.map_err(|e| 
                    WSError::WsDataError(WsDataError::BatchTransferFailed {
                        request_id: proto::BatchRequestId {
                            node_id: 0,  // 这里需要传入正确的node_id
                            sequence: 0,
                        },
                        reason: format!("Failed to get file size: {}", e),
                    })
                )?;
                Ok(metadata.len() as usize)
            }
        }
    }

    pub fn block_type(&self) -> proto::BatchDataBlockType {
        match self {
            DataItemSource::Memory { .. } => proto::BatchDataBlockType::Memory,
            DataItemSource::File { .. } => proto::BatchDataBlockType::File,
        }
    }

    pub async fn get_block(&self, block_idx: usize) -> WSResult<Vec<u8>> {
        match self {
            DataItemSource::Memory { data } => {
                if block_idx == 0 {
                    Ok(data.clone())
                } else {
                    Err(WSError::WsDataError(WsDataError::SizeMismatch {
                        expected: data.len(),
                        actual: 0,
                    }))
                }
            },
            DataItemSource::File { path } => {
                let content = tokio::fs::read(path).await.map_err(|_e| {
                    WSError::WsDataError(WsDataError::ReadDataFailed {
                        path: path.clone(),
                    })
                })?;
                if block_idx == 0 {
                    Ok(content)
                } else {
                    Err(WSError::WsDataError(WsDataError::SizeMismatch {
                        expected: content.len(),
                        actual: 0,
                    }))
                }
            },
        }
    }
}

use crate::general::network::proto_ext::DataItemExt;

impl DataItemExt for DataItemSource {
    fn decode_persist(data: Vec<u8>) -> WSResult<Self> {
        if data.is_empty() {
            return Err(WSError::WsDataError(WsDataError::DataDecodeError {
                reason: "Empty data".to_string(),
                data_type: "DataItemSource".to_string(),
            }));
        }
        match data[0] {
            0 => {
                let path_str = String::from_utf8(data[1..].to_vec()).map_err(|e| {
                    WSError::WsDataError(WsDataError::DataDecodeError {
                        reason: format!("Failed to decode path string: {}", e),
                        data_type: "DataItemSource::File".to_string(),
                    })
                })?;
                Ok(DataItemSource::File {
                    path: PathBuf::from(path_str),
                })
            },
            1 => Ok(DataItemSource::Memory {
                data: data[1..].to_owned(),
            }),
            _ => Err(WSError::WsDataError(WsDataError::DataDecodeError {
                reason: format!("Unknown data item type id: {}", data[0]),
                data_type: "DataItemSource".to_string(),
            }))
        }
    }

    fn encode_persist(&self) -> Vec<u8> {
        match self {
            DataItemSource::File { path } => {
                let mut ret = vec![0];
                ret.extend_from_slice(path.to_string_lossy().as_bytes());
                ret
            }
            DataItemSource::Memory { data } => {
                let mut ret = vec![1];
                ret.extend_from_slice(data);
                ret
            }
        }
    }
}

#[derive(Debug)]
enum DataItemZip {
    /// 未初始化状态
    Uninitialized,
    /// 不需要压缩（非目录）
    NoNeed,
    /// 已压缩的目录
    Directory {
        zipped_file: PathBuf,
    }
}

pub struct DataItemArgWrapper {
    pub dataitem: proto::DataItem,
    /// 目录压缩状态
    tmpzipfile: DataItemZip,
}

impl DataItemArgWrapper {
    pub fn from_file(filepath: PathBuf) -> Self {
        Self { 
            dataitem: proto::DataItem{
                data_item_dispatch: Some(proto::data_item::DataItemDispatch::File(proto::FileData{
                    is_dir_opt: filepath.is_dir(),
                    file_name_opt: filepath.to_str().unwrap().to_string(),
                    file_content: vec![],
                })),
            },
            tmpzipfile: DataItemZip::Uninitialized,
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { 
            dataitem: proto::DataItem::new_raw_bytes(bytes),
            tmpzipfile: DataItemZip::Uninitialized,
        }
    }

    pub async fn get_tmpzipfile(&mut self) -> WSResult<Option<&PathBuf>> {
        match &self.tmpzipfile {
            DataItemZip::Uninitialized => {
                self.init_tmpzipfile().await?;
            }
            _ => {}
        }

        match &self.tmpzipfile {
            DataItemZip::Directory { zipped_file } => Ok(Some(zipped_file)),
            DataItemZip::NoNeed => Ok(None),
            DataItemZip::Uninitialized => unreachable!(),
        }
    }

    async fn init_tmpzipfile(&mut self) -> WSResult<()> {
        // 确保只初始化一次
        if !matches!(self.tmpzipfile, DataItemZip::Uninitialized) {
            return Ok(());
        }

        let filedata = match self.dataitem.data_item_dispatch.as_ref().unwrap() {
            proto::data_item::DataItemDispatch::File(file_data) => file_data,
            proto::data_item::DataItemDispatch::RawBytes(_) => {
                self.tmpzipfile = DataItemZip::NoNeed;
                return Ok(());
            },
        };

        // 检查目录元数据
        let metadata = tokio::fs::metadata(&filedata.file_name_opt).await.map_err(|e| {
            WSError::WsDataError(WsDataError::FileMetadataErr {
                path: PathBuf::from(&filedata.file_name_opt),
                err: e,
            })
        })?;

        if metadata.is_dir() {
            let tmp_file = tempfile::NamedTempFile::new().map_err(|e| {
                WSError::WsDataError(WsDataError::FileMetadataErr {
                    path: PathBuf::from(&filedata.file_name_opt),
                    err: e,
                })
            })?;
            let tmp_path = tmp_file.path().to_path_buf();
            
            // 压缩目录到临时文件
            crate::util::zip::zip_dir_2_file(
                &filedata.file_name_opt,
                zip::CompressionMethod::Stored,
                tmp_file.into_file(),
            ).await?;

            self.tmpzipfile = DataItemZip::Directory {
                zipped_file: tmp_path,
            };
        } else {
            self.tmpzipfile = DataItemZip::NoNeed;
        }

        Ok(())
    }

    pub async fn transfer_size(&mut self) -> WSResult<usize> {
        match &self.dataitem.data_item_dispatch {
            Some(proto::data_item::DataItemDispatch::RawBytes(bytes)) => return Ok(bytes.len()),
            Some(proto::data_item::DataItemDispatch::File(_)) => {
                // handle in following
            }
            None => return Ok(0),
        }

        if let Some(tmp_path) = self.get_tmpzipfile().await? {
            let metadata = tokio::fs::metadata(tmp_path).await?;
            Ok(metadata.len() as usize)
        } else {
            let file_data=match &self.dataitem.data_item_dispatch {
                Some(proto::data_item::DataItemDispatch::File(file_data)) => {
                    // handle in following
                    file_data
                }
                Some(proto::data_item::DataItemDispatch::RawBytes(_)) | None=>{panic!("these case should be handled in previous match")}
            };
            let metadata = tokio::fs::metadata(&file_data.file_name_opt).await?;
            Ok(metadata.len() as usize)
        }
    }

    pub async fn clone_split_range(&mut self, range: Range<usize>) -> WSResult<proto::DataItem> {
        match &self.dataitem.data_item_dispatch {
            Some(proto::data_item::DataItemDispatch::RawBytes(bytes)) => {
                return Ok(proto::DataItem::new_partial_raw_bytes(bytes.to_owned(), range).map_err(|err|{
                    tracing::error!("Failed to clone split range: {}", err);
                    err
                })?)
            }
            Some(proto::data_item::DataItemDispatch::File(_)) => {
                
                // handle in following
            }
            None => panic!("proto dataitem must be Some"),
        }
        
        fn get_filedata(dataitem:&DataItemArgWrapper)->&proto::FileData{
            match &dataitem.dataitem.data_item_dispatch {
                Some(proto::data_item::DataItemDispatch::File(file_data)) => file_data,
                Some(proto::data_item::DataItemDispatch::RawBytes(_)) | None=>{panic!("these case should be handled in previous match")}
            }
        }

        // if zipped, use zipped file
        // else use file_data.file_name_opt
        if let Some(tmp_path) = self.get_tmpzipfile().await?.cloned() {
            let file_data=get_filedata(self);
            Ok(proto::DataItem::new_partial_file_data(NewPartialFileDataArg::FilePath { path: PathBuf::from_str(&file_data.file_name_opt).map_err(|err|{
                let err=WsDataError::FilePathParseErr {
                    path: file_data.file_name_opt.clone(),
                    err: err,
                };
                tracing::error!("Failed to clone split range: {:?}", err);
                err
            })? , zip_path: Some(tmp_path.clone()) }, range).await.map_err(|err|{
                tracing::error!("Failed to clone split range: {}", err);
                err
            })?)
        } else {
            let file_data=get_filedata(self);
            Ok(proto::DataItem::new_partial_file_data(NewPartialFileDataArg::FilePath { path: PathBuf::from_str(&file_data.file_name_opt).map_err(|err|{
                let err=WsDataError::FilePathParseErr {
                    path: file_data.file_name_opt.clone(),
                    err: err,
                };
                tracing::error!("Failed to clone split range: {:?}", err);
                err
            })? , zip_path: None }, range).await.map_err(|err|{
                tracing::error!("Failed to clone split range: {}", err);
                err
            })?)
        }
    }
}
