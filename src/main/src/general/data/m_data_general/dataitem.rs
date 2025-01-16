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
        file_path: PathBuf,
        tasks: Vec<tokio::task::JoinHandle<WSResult<()>>>,
    },
    ToMem {
        shared_mem: SharedMemHolder,
        tasks: Vec<tokio::task::JoinHandle<WSResult<()>>>,
    },
}

impl WriteSplitDataTaskGroup {
    pub async fn new(
        unique_id: Vec<u8>,
        splits: Vec<Range<usize>>,
        mut rx: tokio::sync::mpsc::Receiver<WSResult<(DataSplitIdx, proto::DataItem)>>,
        cachemode: CacheModeVisitor,
    ) -> WSResult<Self> {
        tracing::debug!(
            "new merge task group for uid({:?}), cachemode({})",
            unique_id,
            cachemode.0
        );
        if cachemode.is_map_file() {
            tracing::debug!("cachemode is map_file");
            // base64
            // let file_path = PathBuf::from(format!("{:?}.data", unique_id));
            let file_path = PathBuf::from(format!(
                "{}.data",
                base64::engine::general_purpose::STANDARD.encode(&unique_id)
            ));

            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&file_path)?;
            let file = std::sync::Arc::new(file);

            let mut tasks = vec![];
            for _ in 0..splits.len() {
                let parital_data = rx.recv().await.unwrap();
                match parital_data {
                    Err(e) => {
                        return Err(e);
                    }
                    Ok((splitidx, split_data_item)) => {
                        let file = file.clone();
                        let unique_id = unique_id.clone();
                        let split_range = splits[splitidx as usize].clone();

                        let task = tokio::task::spawn_blocking(move || {
                            let Some(split_data_bytes) = split_data_item.as_raw_bytes() else {
                                return Err(WsDataError::SplitDataItemNotRawBytes {
                                    unique_id: unique_id.clone(),
                                    splitidx,
                                }
                                .into());
                            };

                            if split_range.len() != split_data_bytes.len() {
                                return Err(WsDataError::SplitLenMismatch {
                                    unique_id,
                                    splitidx,
                                    expect: split_range.len(),
                                    actual: split_data_bytes.len(),
                                }
                                .into());
                            }
                            // SAFETY: Each task writes to a different non-overlapping portion of the file
                            use std::os::unix::fs::FileExt;
                            if let Err(e) =
                                file.write_at(split_data_bytes, split_range.start as u64)
                            {
                                return Err(WSError::WsIoErr(WsIoErr::Io(e)));
                            }
                            Ok(())
                        });
                        tasks.push(task);
                    }
                }
            }
            Ok(Self::ToFile { file_path, tasks })
        } else if cachemode.is_map_common_kv() {
            tracing::debug!("cachemode is map_common_kv");
            let (shared_mem, owned_accesses) = new_shared_mem(&splits);
            let mut owned_accesses = owned_accesses
                .into_iter()
                .map(|access| Some(access))
                .collect::<Vec<_>>();
            let mut tasks = vec![];
            for _ in 0..splits.len() {
                let parital_data = rx.recv().await.unwrap();
                match parital_data {
                    Err(e) => {
                        return Err(e);
                    }
                    Ok((splitidx, split_data_item)) => {
                        let owned_access = owned_accesses[splitidx].take().unwrap();
                        let unique_id = unique_id.clone();
                        let task = tokio::spawn(async move {
                            // write to shared memory
                            let access = unsafe { owned_access.as_bytes_mut() };
                            let Some(split_data_item) = split_data_item.as_raw_bytes() else {
                                return Err(WsDataError::SplitDataItemNotRawBytes {
                                    unique_id: unique_id.clone(),
                                    splitidx,
                                }
                                .into());
                            };
                            if access.len() != split_data_item.len() {
                                return Err(WsDataError::SplitLenMismatch {
                                    unique_id: unique_id.clone(),
                                    splitidx,
                                    expect: access.len(),
                                    actual: split_data_item.len(),
                                }
                                .into());
                            }
                            access.copy_from_slice(split_data_item);
                            Ok(())
                        });
                        tasks.push(task);
                    }
                }
            }
            Ok(Self::ToMem { shared_mem, tasks })
        } else {
            panic!("cachemode should be map_file or map_mem");
        }
    }

    pub async fn join(self) -> WSResult<proto::DataItem> {
        match self {
            WriteSplitDataTaskGroup::ToFile { file_path, tasks } => {
                let taskress = join_all(tasks).await;
                for res in taskress {
                    if res.is_err() {
                        return Err(WSError::from(WsRuntimeErr::TokioJoin {
                            err: res.unwrap_err(),
                            context: "write split data to file".to_owned(),
                        }));
                    }
                    if res.as_ref().unwrap().is_err() {
                        return Err(res.unwrap().unwrap_err());
                    }
                }
                Ok(proto::DataItem::new_file_data(file_path, false))
            }
            WriteSplitDataTaskGroup::ToMem {
                shared_mem: shared_mems,
                tasks,
            } => {
                let taskress = join_all(tasks).await;
                for res in taskress {
                    if res.is_err() {
                        return Err(WSError::from(WsRuntimeErr::TokioJoin {
                            err: res.unwrap_err(),
                            context: "write split data to file".to_owned(),
                        }));
                    }
                    if res.as_ref().unwrap().is_err() {
                        return Err(res.unwrap().unwrap_err());
                    }
                }
                // convert to dataitem
                Ok(proto::DataItem::new_raw_bytes(
                    shared_mems
                        .try_take_data()
                        .expect("shared_mems should be take when all partial task stoped"),
                ))
            }
        }
    }
}

// pub async fn read_splitdata_from_nodes_to_file<'a>(
//     ty: &GetOrDelDataArgType,
//     unique_id: &[u8],
//     view: &DataGeneralView,
//     meta: &DataSetMetaV2,
//     each_node_data: HashMap<NodeID, proto::GetOneDataRequest>,
// ) ->ReadSplitDataTask{
//     // prepare file with meta size
//     let file_path = format!("{}.data", unique_id);
//     let file = File::create(file_path)?;

//     // parallel read and write to position of file with pwrite
//     let mut tasks = vec![];
//     // get idxs, one idx one file

//     for (node_id, req) in each_node_data {
//         let view = view.clone();
//         let task = tokio::spawn(async move {
//             let res = view
//                 .data_general()
//                 .rpc_call_get_data
//                 .call(view.p2p(), node_id, req, Some(Duration::from_secs(30)))
//                 .await;
//             match res {
//                 Err(err) => {
//                     tracing::warn!("get/delete data failed {}", err);
//                     vec![]
//                 }
//                 Ok(res) => {
//                     res.
//                     // get offset and size by meta with got

//                     vec![]
//                 },
//             }
//         });
//         tasks.push(task);
//     }
//     Ok(HashMap::new())
// }

// pub async fn read_splitdata_from_nodes_to_mem<'a>(
//     ty: &GetOrDelDataArgType,
//     unique_id: &[u8],
//     view: &DataGeneralView,
//     meta: &DataSetMetaV2,
//     each_node_data: HashMap<NodeID, proto::GetOneDataRequest>,
// ) -> ReadSplitDataTask {
//     // read to mem
//     let mut tasks = vec![];
//     for (node_id, req) in each_node_data {
//         let view = view.clone();
//         let task = tokio::spawn(async move {
//             let req_idxs = req.idxs.clone();
//             tracing::debug!("rpc_call_get_data start, remote({})", node_id);
//             let res = view
//                 .data_general()
//                 .rpc_call_get_data
//                 .call(view.p2p(), node_id, req, Some(Duration::from_secs(30)))
//                 .await;
//             tracing::debug!("rpc_call_get_data returned, remote({})", node_id);
//             let res: WSResult<Vec<(u32, proto::DataItem)>> = res.map(|response| {
//                 if !response.success {
//                     tracing::warn!("get/delete data failed {}", response.message);
//                     vec![]
//                 } else {
//                     req_idxs.into_iter().zip(response.data).collect()
//                 }
//             });
//             (node_id, res)
//         });
//         tasks.push(task);
//     }

//     let mut node_partialdatas: HashMap<(NodeID, DataItemIdx), proto::DataItem> = HashMap::new();
//     for tasks in tasks {
//         let (node_id, partdata) = tasks.await.map_err(|err| {
//             WSError::from(WsRuntimeErr::TokioJoin {
//                 err,
//                 context: "get_or_del_data - get_or_del ing remote data".to_owned(),
//             })
//         })?;

//         match partdata {
//             Err(err) => {
//                 return Err(err);
//             }
//             Ok(partdata) => {
//                 for (idx, data_item) in partdata {
//                     let _ = node_partialdatas.insert((node_id, idx as u8), data_item);
//                 }
//             }
//         }
//     }

//     let mut idx_2_data_item: HashMap<DataItemIdx, proto::DataItem> = HashMap::new();
//     for idx in WantIdxIter::new(&ty) {
//         let data_split = &meta.datas_splits[idx as usize];
//         let data_item = data_split.recorver_data(unique_id, idx, &mut node_partialdatas)?;

//         idx_2_data_item
//             .insert(idx, proto::DataItem::new_raw_bytes(data_item))
//             .expect("dataitem should be unique with idx");
//     }

//     Ok(idx_2_data_item)
// }
