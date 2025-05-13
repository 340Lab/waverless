use crate::general::data::m_data_general::UniqueId;
use crate::general::data::m_data_general::{DataItemIdx, DataSplitIdx, GetOrDelDataArgType};
use crate::general::m_os::OperatingSystem;
use crate::general::network::proto;
use crate::general::network::proto_ext::{NewPartialFileDataArg, ProtoExtDataItem};
use crate::logical_module_view_impl;
use crate::result::{WSError, WSResult, WSResultExt, WsDataError};
use crate::util::zip;
use crate::LogicalModulesRef;
use parking_lot::Mutex;
//虞光勇修改，修改内容：增加use crate::LogicalModulesRef;来导入 LogicalModulesRef。
use ::zip::CompressionMethod; //虞光勇修改，因为编译器无法找到 zip 模块中的 CompressionMethod，需加入头文件（860续）
use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures::stream::{FuturesUnordered, StreamExt};
use std::cell::RefCell;
use std::collections::btree_set;
use std::io::Read;
use std::ops::Range;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::{broadcast, oneshot};
use tracing;

const DEFAULT_BLOCK_SIZE: usize = 4096;

// logical_module_view_impl!(DataItemView);
// logical_module_view_impl!(DataItemView, os, OperatingSystem);

/// 用于遍历数据项索引的迭代器
#[derive(Debug)]
pub(super) enum WantIdxIter<'a> {
    /// 遍历多个指定索引
    PartialMany {
        iter: btree_set::Iter<'a, DataItemIdx>,
    },
    /// 遍历单个索引
    PartialOne { idx: DataItemIdx, itercnt: u8 },
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
        tracing::debug!(
            "try_take_data, Arc::strong_count: {}, Arc::weak_count: {}",
            Arc::strong_count(&self.data),
            Arc::weak_count(&self.data)
        );

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
        holder
            .as_raw_bytes()
            .expect("Failed to get raw bytes")
            .to_vec()
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
    Dir {
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
        /// use option to drop before notify
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
        is_dir: bool,
        /// 任务唯一标识
        unique_id: UniqueId,
        /// 临时文件路径，用作传输
        tmp_file_path: PathBuf,
        /// 目标文件路径, 用作最终使用
        target_file_path: PathBuf,
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

        // /// 共享内存区域
        // shared_mem: RefCell<Option<<SharedMemHolder>>>,
        /// 费新文修改，修改内容：shared_mem: RefCell<Option<<SharedMemHolder>>>,
        /// 修改原因：shared_mem: RefCell<Option<<SharedMemHolder>>>, 需要修改为 RefCell<Option<SharedMemHolder>>,
        /// 修改后：shared_mem: RefCell<Option<SharedMemHolder>>,
        /// 共享内存区域
        ///
        // shared_mem: RefCell<Option<SharedMemHolder>>, 修改为RwLock<Option<SharedMemHolder>>, 曾俊
        shared_mem: RwLock<Option<SharedMemHolder>>,

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
        view: &DataGeneralView,
        unique_id: UniqueId,
        total_size: usize,
        block_type: proto::data_item::DataItemDispatch,
        version: u64,
        // file_name: Option<&str>,     函数体并没有用到这个参数    查看引用发现也没有使用到这个参数   这里直接删除     曾俊
    ) -> WSResult<(
        Self,
        WriteSplitDataTaskHandle,
        // oneshot::Receiver<Option<proto::DataItem>>, // send back the recevied data
    )> {
        // let (alldone_tx, alldone_rx) = oneshot::channel();
        let (tx, rx) = mpsc::channel(32);
        let (broadcast_tx, _) = broadcast::channel::<()>(32);
        let broadcast_tx = Arc::new(broadcast_tx);
        // let pathbase=DataItemView::new(try_get_modules_ref().todo_handle("Failed to get modules ref when create WriteSplitDataTaskGroup")?).os().file_path;
        //所有权发生变化   添加克隆方法    曾俊
        let pathbase = view.os().file_path.clone();

        match block_type {
            proto::data_item::DataItemDispatch::File(file_data) => {
                let tmp_file_path = pathbase
                    .join(DATA_TMP_DIR)
                    .join(format!("{}.data", STANDARD.encode(&unique_id)));

                let handle = WriteSplitDataTaskHandle {
                    tx,
                    write_type: WriteSplitDataType::File {
                        path: tmp_file_path.clone(),
                    },
                    version,
                    totalsize: total_size,
                    submited_size: Arc::new(AtomicUsize::new(0)),
                    // takeonce_alldone_tx: Arc::new(Mutex::new(Some(alldone_tx))),
                };

                let group = Self::ToFile {
                    is_dir: file_data.is_dir_opt,
                    unique_id,
                    tmp_file_path,
                    target_file_path: pathbase.join(file_data.file_name_opt.as_str()),
                    tasks: Vec::new(),
                    rx,
                    expected_size: total_size,
                    current_size: 0,
                    broadcast_tx: broadcast_tx.clone(),
                };

                Ok((group, handle))
            }
            proto::data_item::DataItemDispatch::RawBytes(_) => {
                let shared_mem = SharedMemHolder {
                    data: Arc::new(vec![0; total_size]),
                };

                let handle = WriteSplitDataTaskHandle {
                    tx,
                    write_type: WriteSplitDataType::Mem {
                        shared_mem: shared_mem.clone(),
                    },
                    version,
                    totalsize: total_size,
                    submited_size: Arc::new(AtomicUsize::new(0)),
                    // takeonce_alldone_tx: Arc::new(Mutex::new(Some(alldone_tx))),
                };

                let group = Self::ToMem {
                    unique_id,
                    // 原代码：shared_mem,     类型不匹配        曾俊
                    shared_mem: RwLock::new(Some(shared_mem)),
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
        let mut pending_tasks: FuturesUnordered<tokio::task::JoinHandle<WriteSplitTaskResult>> =
            FuturesUnordered::new();

        match self {
            Self::ToFile { tasks, .. } | Self::ToMem { tasks, .. } => {
                for task in tasks.drain(..) {
                    pending_tasks.push(task);
                }
            }
        }

        loop {
            // 1. 检查完成状态
            match self
                .try_complete()
                .await
                .todo_handle("Failed to complete write split data tasks")?
            {
                Some(item) => return Ok(item),
                None => {} // 继续等待
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
    async fn try_complete(&self) -> WSResult<Option<proto::DataItem>> {
        match self {
            Self::ToFile {
                current_size,
                expected_size,
                tmp_file_path,
                target_file_path,
                unique_id,
                is_dir,
                ..
            } => {
                if *current_size > *expected_size {
                    Err(WSError::WsDataError(WsDataError::BatchTransferError {
                        request_id: proto::BatchRequestId {
                            node_id: 0, // 这里需要传入正确的node_id
                            sequence: 0,
                        },
                        msg: format!(
                            "Written size {} exceeds expected size {} for unique_id {:?}",
                            current_size, expected_size, unique_id
                        ),
                    }))
                } else if *current_size == *expected_size {
                    if *is_dir {
                        // unzip to file_path
                        // - open received file with std api
                        let file = std::fs::File::open(tmp_file_path).map_err(|e| {
                            tracing::error!("Failed to open file: {}", e);
                            WSError::from(WsDataError::FileOpenErr {
                                path: tmp_file_path.clone(),
                                err: e,
                            })
                        })?;
                        // let tmp_file_path = tmp_file_path.clone();
                        let target_file_path = target_file_path.clone();
                        tokio::task::spawn_blocking(move || {
                            zip_extract::extract(file, target_file_path.as_path(), false).map_err(
                                |e| {
                                    // open and read data
                                    // let mut file = std::fs::File::open(tmp_file_path.clone())
                                    //     .map_err(|e| {
                                    //         tracing::error!("Failed to open file: {}", e);
                                    //         WSError::from(WsDataError::FileOpenErr {
                                    //             path: tmp_file_path.clone(),
                                    //             err: e,
                                    //         })
                                    //     })
                                    //     .unwrap();
                                    // let mut data = Vec::new();
                                    // let read_size = file
                                    //     .read_to_end(&mut data)
                                    //     .map_err(|e| {
                                    //         tracing::error!("Failed to read file: {}", e);
                                    //         WSError::from(WsDataError::FileReadErr {
                                    //             path: tmp_file_path.clone(),
                                    //             err: e,
                                    //         })
                                    //     })
                                    //     .unwrap();
                                    // tracing::debug!(
                                    //     "zip file data size: {:?} {}",
                                    //     data.len(),
                                    //     read_size
                                    // );
                                    // deb
                                    WSError::from(WsDataError::UnzipErr {
                                        path: target_file_path,
                                        err: e,
                                    })
                                },
                            )
                        })
                        .await
                        .unwrap()
                        .todo_handle("Failed to unzip file")?;
                        // .map_err(|err| {

                        // })?;
                    } else {
                        // rename tmp_file_path to target_file_path
                        let target_dir = target_file_path.parent().unwrap();
                        std::fs::create_dir_all(target_dir).map_err(|e| {
                            tracing::error!("Failed to create target directory: {}", e);
                            WSError::from(WsDataError::FileCreateErr {
                                path: target_dir.to_path_buf(),
                                err: e,
                            })
                        })?;
                        std::fs::rename(tmp_file_path, target_file_path).map_err(|e| {
                            tracing::error!(
                                "Failed to rename file from {:?} to {:?}, error: {}",
                                tmp_file_path,
                                target_file_path,
                                e
                            );
                            WSError::from(WsDataError::FileRenameErr {
                                from: tmp_file_path.clone(),
                                to: target_file_path.clone(),
                                err: e,
                            })
                        })?;
                    }
                    Ok(Some(proto::DataItem {
                        data_item_dispatch: Some(proto::data_item::DataItemDispatch::File(
                            proto::FileData {
                                file_name_opt: target_file_path.to_string_lossy().to_string(),
                                is_dir_opt: *is_dir,
                                file_content: vec![],
                            },
                        )),
                    }))
                } else {
                    Ok(None)
                }
            }
            Self::ToMem {
                current_size,
                expected_size,
                shared_mem,
                unique_id,
                ..
            } => {
                if *current_size > *expected_size {
                    Err(WSError::WsDataError(WsDataError::BatchTransferError {
                        request_id: proto::BatchRequestId {
                            node_id: 0, // 这里需要传入正确的node_id
                            sequence: 0,
                        },
                        msg: format!(
                            "Written size {} exceeds expected size {} for unique_id {:?}",
                            current_size, expected_size, unique_id
                        ),
                    }))
                } else if *current_size == *expected_size {
                    tracing::debug!("size reached {}, taking mem data", current_size);
                    Ok(Some(proto::DataItem {
                        //曾俊  随RwLock数据类型改动
                        // data_item_dispatch: Some(proto::data_item::DataItemDispatch::RawBytes(shared_mem.borrow_mut().take().unwrap().try_take_data().expect("only group can take data once"))),
                        data_item_dispatch: Some(proto::data_item::DataItemDispatch::RawBytes(
                            shared_mem
                                .write()
                                .expect("Failed to lock RwLock for writing")
                                .take()
                                .unwrap()
                                .try_take_data()
                                .expect("only group can take data once"),
                        )),
                    }))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// 简化的任务完成等待器
pub struct WriteSplitDataWaiter {
    rx: broadcast::Receiver<()>,
}

impl WriteSplitDataWaiter {
    /// 等待所有任务完成
    pub async fn wait(mut self) -> WSResult<()> {
        // 持续接收直到通道关闭
        while let Ok(_) = self.rx.recv().await {
            // 不需要处理具体消息内容，只需要知道有消息到达
        }

        // 通道关闭表示所有发送端都已释放
        Ok(())
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
    // broadcast_tx: Arc<broadcast::Sender<()>>,
    // takeonce_alldone_tx: Arc<Mutex<Option<oneshot::Sender<Option<proto::DataItem>>>>>,

    /// total size
    totalsize: usize,

    /// submited size
    submited_size: Arc<AtomicUsize>,
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
    /// * `Ok(true)` - 还未接受完全
    /// * `Ok(false)` - 已接受完全
    /// * `Err(e)` - 任务提交失败,可能是通道已关闭
    pub async fn submit_split(&self, idx: DataSplitIdx, data: Vec<u8>) -> WSResult<bool> {
        let (task, submited_size) = match &self.write_type {
            // WriteSplitDataType::File { path } | WriteSplitDataType::Dir { path } => {   原WriteSplitDataType::Dir忽视了zip_file字段   发现没有用到修改为直接忽视   曾俊
            WriteSplitDataType::File { path } | WriteSplitDataType::Dir { path, .. } => {
                let path = path.clone();
                let offset = idx;
                let written_size = data.len();
                let submited_size = self
                    .submited_size
                    .fetch_add(written_size, std::sync::atomic::Ordering::Relaxed)
                    + written_size;
                tracing::debug!(
                    "submit_split: after submit len:{}, target len:{}",
                    submited_size,
                    self.totalsize,
                );
                let task = tokio::spawn(async move {
                    let result = tokio::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .open(&path)
                        .await;

                    match result {
                        Ok(mut file) => {
                            tracing::debug!(
                                "write_split len:{} offset:{} path:{:?}",
                                data.len(),
                                offset,
                                path,
                            );
                            use tokio::io::{AsyncSeekExt, AsyncWriteExt};
                            if let Err(e) = async move {
                                // 验证seek结果
                                let seek_pos =
                                    file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
                                if seek_pos != offset as u64 {
                                    return Err(std::io::Error::new(
                                        std::io::ErrorKind::Other,
                                        format!(
                                            "Seek position mismatch: expected {}, got {}",
                                            offset, seek_pos
                                        ),
                                    ));
                                }
                                // write_all保证写入所有数据或返回错误
                                file.write_all(&data).await?;
                                // file.sync_data().await?;
                                file.flush().await?;
                                // check file size
                                #[cfg(test)]
                                {
                                    let metadata = tokio::fs::metadata(&path).await?;
                                    assert!(
                                        metadata.len() as usize >= offset + data.len(),
                                        "file size mismatch, expected {}, got {}",
                                        offset + data.len(),
                                        metadata.len()
                                    );

                                    tracing::debug!(
                                        "write file data at offset {} success with size {}",
                                        offset,
                                        metadata.len()
                                    );
                                }

                                Ok::<_, std::io::Error>(())
                            }
                            .await
                            {
                                tracing::error!(
                                    "Failed to write file data at offset {}: {}",
                                    offset,
                                    e
                                );
                                panic!("Failed to write file: {}", e);
                            }
                            WriteSplitTaskResult { written_size }
                        }
                        Err(e) => {
                            tracing::error!("Failed to open file at offset {}: {}", offset, e);
                            panic!("Failed to open file: {}", e);
                        }
                    }
                });
                (task, submited_size)
            }
            WriteSplitDataType::Mem { shared_mem } => {
                let mem = shared_mem.clone();
                let offset = idx;
                let written_size = data.len();
                let submited_size = self
                    .submited_size
                    .fetch_add(written_size, std::sync::atomic::Ordering::Relaxed)
                    + written_size;

                tracing::debug!(
                    "submit_split: Mem, len:{}, target len:{}",
                    data.len(),
                    shared_mem.len()
                );

                (
                    tokio::spawn(async move {
                        unsafe {
                            let slice = std::slice::from_raw_parts_mut(
                                mem.data.as_ptr() as *mut u8,
                                mem.data.len(),
                            );
                            slice[offset..offset + data.len()].copy_from_slice(&data);
                        }
                        WriteSplitTaskResult { written_size }
                    }),
                    submited_size,
                )
            }
        };

        // 发送到通道
        // let _ = self.send(());
        let _ = self.tx.send(task).await.map_err(|e| {
            tracing::error!(
                "Failed to submit task: channel closed, idx: {:?}, error: {}",
                idx,
                e
            );
            WSError::WsDataError(WsDataError::DataSplitTaskError {
                msg: format!("Failed to submit task: channel closed, error: {}", e),
            })
        })?;
        if submited_size == self.totalsize {
            Ok(false)
        } else if submited_size > self.totalsize {
            Err(WSError::WsDataError(WsDataError::DataSplitTaskError {
                msg: format!(
                    "submited_size: {} > totalsize: {}",
                    submited_size, self.totalsize
                ),
            }))
        } else {
            Ok(true)
        }
    }

    // / 等待所有已提交的写入任务完成
    // / 关闭发送端,不再接收新任务
    // pub async fn wait_all_tasks(&self) -> WSResult<()> {
    //     // 等待广播通知
    //     let mut rx = self.broadcast_tx.subscribe();
    //     rx.recv().await.map_err(|e| {
    //         tracing::error!("Failed to wait for tasks: {}", e);
    //         WSError::WsDataError(WsDataError::BatchTransferTaskFailed {
    //             reason: format!("Failed to wait for tasks: {}", e),
    //         })
    //     })?;

    //     Ok(())
    // }
}

#[derive(Debug)]
pub enum DataItemSource {
    Memory { data: Vec<u8> },
    File { path: PathBuf },
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
            _ => Self::Memory { data: Vec::new() },
        }
    }

    //添加一个DataItemSource转换到DataItem的函数      曾俊
    pub fn to_data_item(&self) -> proto::DataItem {
        match self {
            DataItemSource::Memory { data } => proto::DataItem {
                data_item_dispatch: Some(proto::data_item::DataItemDispatch::RawBytes(
                    data.clone(),
                )),
            },
            DataItemSource::File { path } => proto::DataItem {
                data_item_dispatch: Some(proto::data_item::DataItemDispatch::File(
                    proto::FileData {
                        file_name_opt: path
                            .to_str()
                            .map_or_else(|| String::from(""), |s| s.to_string()), // 这里需要根据实际情况调整类型转换
                        ..Default::default() // 假设 FileData 有其他字段，这里使用默认值
                    },
                )),
            },
        }
    }

    pub async fn size(&self) -> WSResult<usize> {
        match self {
            DataItemSource::Memory { data } => Ok(data.len()),
            DataItemSource::File { path } => {
                let metadata = tokio::fs::metadata(path).await.map_err(|e| {
                    WSError::WsDataError(WsDataError::BatchTransferFailed {
                        request_id: proto::BatchRequestId {
                            node_id: 0, // 这里需要传入正确的node_id
                            sequence: 0,
                        },
                        reason: format!("Failed to get file size: {}", e),
                    })
                })?;
                Ok(metadata.len() as usize)
            }
        }
    }

    // pub fn block_type(&self) -> proto::BatchDataBlockType {
    //     match self {
    //         DataItemSource::Memory { .. } => proto::BatchDataBlockType::Memory,
    //         DataItemSource::File { .. } => proto::BatchDataBlockType::File,
    //     }
    // }

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
            }
            DataItemSource::File { path } => {
                let content = tokio::fs::read(path).await.map_err(|_e| {
                    WSError::WsDataError(WsDataError::ReadDataFailed { path: path.clone() })
                })?;
                if block_idx == 0 {
                    Ok(content)
                } else {
                    Err(WSError::WsDataError(WsDataError::SizeMismatch {
                        expected: content.len(),
                        actual: 0,
                    }))
                }
            }
        }
    }
}

use crate::general::network::proto_ext::DataItemExt;

use super::{DataGeneralView, DATA_TMP_DIR};

impl DataItemExt for DataItemSource {
    fn inmem_size(&self) -> usize {
        todo!()
    }
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
            }
            1 => Ok(DataItemSource::Memory {
                data: data[1..].to_owned(),
            }),
            _ => Err(WSError::WsDataError(WsDataError::DataDecodeError {
                reason: format!("Unknown data item type id: {}", data[0]),
                data_type: "DataItemSource".to_string(),
            })),
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

    fn get_data_type(&self) -> proto::data_item::DataItemDispatch {
        todo!()
    }

    fn into_data_bytes(self) -> Vec<u8> {
        todo!()
    }
}

#[derive(Debug, Clone)]
enum DataItemZip {
    /// 未初始化状态
    Uninitialized,
    /// 不需要压缩（非目录）
    NoNeed,
    /// 已压缩的目录
    Directory { zipped_file: PathBuf },
}

//派生显示特征  曾俊
#[derive(Debug, Clone)]
pub struct DataItemArgWrapper {
    pub dataitem: proto::DataItem,
    /// 目录压缩状态
    tmpzipfile: DataItemZip,
}

impl DataItemArgWrapper {
    pub fn filepath(&self) -> Option<String> {
        match &self.dataitem.data_item_dispatch {
            Some(proto::data_item::DataItemDispatch::File(file_data)) => {
                Some(file_data.file_name_opt.clone())
            }
            _ => None,
        }
    }
    // 根据传入的DataItem类型新建一个DataItemArgWrapper实例， tmpzipfile默认为Uninitialized。      曾俊
    pub fn new(dataitem: proto::DataItem) -> Self {
        DataItemArgWrapper {
            dataitem: dataitem,
            tmpzipfile: DataItemZip::Uninitialized,
        }
    }
    pub fn from_file(mref: LogicalModulesRef, filepath: PathBuf) -> WSResult<Self> {
        let view = DataGeneralView::new(mref);

        //let abs_filepath=view.os().abs_file_path(filepath);
        //虞光勇修改 添加.clone()
        let abs_filepath = view.os().abs_file_path(filepath.clone());

        Ok(Self {
            dataitem: proto::DataItem {
                data_item_dispatch: Some(proto::data_item::DataItemDispatch::File(
                    proto::FileData {
                        is_dir_opt: abs_filepath.is_dir(),
                        file_name_opt: filepath.to_str().unwrap().to_string(),
                        file_content: vec![],
                    },
                )),
            },
            tmpzipfile: DataItemZip::Uninitialized,
        })
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            dataitem: proto::DataItem {
                data_item_dispatch: Some(proto::data_item::DataItemDispatch::RawBytes(bytes)),
            },
            tmpzipfile: DataItemZip::Uninitialized,
        }
    }

    pub async fn get_tmpzipfile(&mut self, filepath: &PathBuf) -> WSResult<Option<&PathBuf>> {
        match &self.tmpzipfile {
            DataItemZip::Uninitialized => {
                self.init_tmpzipfile(filepath).await.map_err(|err| {
                    tracing::warn!(
                        "Failed to init tmpzipfile {} with error {}",
                        self.dataitem
                            .as_file_data()
                            .expect("only filedata need to be initialized")
                            .file_name_opt,
                        err
                    );
                    err
                })?;
            }
            _ => {}
        }

        match &self.tmpzipfile {
            DataItemZip::Directory { zipped_file } => Ok(Some(zipped_file)),
            DataItemZip::NoNeed => Ok(None),
            DataItemZip::Uninitialized => unreachable!(),
        }
    }

    // call this only when this dataitem is a directory
    async fn init_tmpzipfile(&mut self, filepath: &PathBuf) -> WSResult<()> {
        // 确保只初始化一次
        if !matches!(self.tmpzipfile, DataItemZip::Uninitialized) {
            return Ok(());
        }

        let filedata = match self.dataitem.data_item_dispatch.as_ref().unwrap() {
            proto::data_item::DataItemDispatch::File(file_data) => file_data,
            proto::data_item::DataItemDispatch::RawBytes(_) => {
                self.tmpzipfile = DataItemZip::NoNeed;
                return Ok(());
            }
        };

        let filepath = filepath.join(&filedata.file_name_opt);
        // 检查目录元数据
        let metadata = tokio::fs::metadata(&filepath).await.map_err(|e| {
            WSError::WsDataError(WsDataError::FileMetadataErr {
                path: filepath.clone(),
                err: e,
            })
        })?;

        if metadata.is_dir() {
            let tmp_file = tempfile::NamedTempFile::new().map_err(|e| {
                WSError::WsDataError(WsDataError::TransferDirCreateTmpFileFailed {
                    path: filepath.clone(),
                    err: e,
                    context: "init_tmpzipfile".to_string(),
                })
            })?;
            // let tmp_path = tmp_file.path().to_path_buf();
            let (mut tmp_file, tmp_path) = tmp_file.keep().map_err(|err| {
                tracing::error!("Failed to keep tmp_file: {}", err);
                WSError::WsDataError(WsDataError::TransferDirPersistTmpFileKeepFailed {
                    path: filepath.clone(),
                    err: err,
                    context: "init_tmpzipfile".to_string(),
                })
            })?;

            // 压缩目录到临时文件
            crate::util::zip::zip_dir_2_file(
                &filepath,
                //zip::CompressionMethod::Stored,
                CompressionMethod::Stored, //（续）虞光勇修改，修改内容删除zip::
                &mut tmp_file,
            )
            .await?;

            // debug zip file size
            let metadata = tokio::fs::metadata(&tmp_path).await?;
            tracing::info!("zip file size: {}", metadata.len());

            self.tmpzipfile = DataItemZip::Directory {
                zipped_file: tmp_path,
            };
        } else {
            self.tmpzipfile = DataItemZip::NoNeed;
        }

        Ok(())
    }

    /// get file sized
    /// files supposed to be all compressed if return no error
    pub async fn get_data_size(&mut self, filepath: &PathBuf) -> WSResult<usize> {
        match &self.dataitem.data_item_dispatch {
            Some(proto::data_item::DataItemDispatch::RawBytes(bytes)) => return Ok(bytes.len()),
            Some(proto::data_item::DataItemDispatch::File(_)) => {
                // handle in following
            }
            None => return Ok(0),
        }

        if let Some(tmp_path) = self.get_tmpzipfile(filepath).await? {
            // tracing::info!("tmp_path: {:?}, please debug file in 30 seconds", tmp_path);
            // tokio::time::sleep(Duration::from_secs(30)).await;
            let metadata = tokio::fs::metadata(tmp_path).await?;
            Ok(metadata.len() as usize)
        } else {
            let file_data = match &self.dataitem.data_item_dispatch {
                Some(proto::data_item::DataItemDispatch::File(file_data)) => {
                    // handle in following
                    file_data
                }
                Some(proto::data_item::DataItemDispatch::RawBytes(_)) | None => {
                    panic!("these case should be handled in previous match")
                }
            };
            let metadata = tokio::fs::metadata(&file_data.file_name_opt).await?;
            Ok(metadata.len() as usize)
        }
    }

    pub async fn clone_split_range(
        &mut self,
        filepath: &PathBuf,
        range: Range<usize>,
    ) -> WSResult<proto::DataItem> {
        match &self.dataitem.data_item_dispatch {
            Some(proto::data_item::DataItemDispatch::RawBytes(bytes)) => {
                return Ok(
                    proto::DataItem::new_partial_raw_bytes(bytes.to_owned(), range).map_err(
                        |err| {
                            tracing::error!("Failed to clone split range: {}", err);
                            err
                        },
                    )?,
                )
            }
            Some(proto::data_item::DataItemDispatch::File(_)) => {

                // handle in following
            }
            None => panic!("proto dataitem must be Some"),
        }

        fn get_filedata(dataitem: &DataItemArgWrapper) -> &proto::FileData {
            match &dataitem.dataitem.data_item_dispatch {
                Some(proto::data_item::DataItemDispatch::File(file_data)) => file_data,
                Some(proto::data_item::DataItemDispatch::RawBytes(_)) | None => {
                    panic!("these case should be handled in previous match")
                }
            }
        }

        // if zipped, use zipped file
        // else use file_data.file_name_opt
        if let Some(tmp_path) = self.get_tmpzipfile(filepath).await?.cloned() {
            let file_data = get_filedata(self);
            Ok(proto::DataItem::new_partial_file_data(
                NewPartialFileDataArg::FilePath {
                    basepath: filepath.clone(),
                    path: PathBuf::from(file_data.file_name_opt.clone()),
                    zip_path: Some(tmp_path.clone()),
                },
                range,
            )
            .await
            .map_err(|err| {
                tracing::error!("Failed to clone split range: {}", err);
                err
            })?)
        } else {
            let file_data = get_filedata(self);
            // let path = ;
            // if !path.exists() {
            //     return Err(WSError::WsDataError(WsDataError::FileNotFound {
            //         path: path.clone(),
            //     }));
            // }
            Ok(proto::DataItem::new_partial_file_data(
                NewPartialFileDataArg::FilePath {
                    basepath: filepath.clone(),
                    path: PathBuf::from(file_data.file_name_opt.clone()),
                    zip_path: None,
                },
                range,
            )
            .await
            .map_err(|err| {
                tracing::error!("Failed to clone split range: {}", err);
                err
            })?)
        }
    }
}
