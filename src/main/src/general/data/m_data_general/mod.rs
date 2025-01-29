mod dataitem;

use crate::general::data::m_data_general::dataitem::WantIdxIter;
use crate::general::data::m_data_general::dataitem::WriteSplitDataTaskGroup;
use crate::general::{
    data::m_kv_store_engine::{
        KeyTypeDataSetItem, KeyTypeDataSetMeta, KvAdditionalConf, KvStoreEngine, KvVersion,
    },
    m_os::OperatingSystem,
    network::{
        m_p2p::{P2PModule, RPCCaller, RPCHandler, RPCResponsor},
        proto::{
            self, DataMeta, DataMetaGetRequest, DataVersionScheduleRequest, WriteOneDataRequest,
            WriteOneDataResponse,
        },
        proto_ext::ProtoExtDataItem,
    },
};
use crate::{
    general::{
        data::m_kv_store_engine::{KeyLockGuard, KeyType},
        network::{msg_pack::MsgPack, proto_ext::DataItemExt},
    },
    logical_module_view_impl,
    result::{WSError, WSResult, WSResultExt, WsRuntimeErr, WsSerialErr, WsNetworkLogicErr},
    sys::{LogicalModule, LogicalModuleNewArgs, NodeID},
    util::JoinHandleWrapper,
};
use crate::{result::WsDataError, sys::LogicalModulesRef};
use async_trait::async_trait;
use camelpaste::paste;
use core::str;
use enum_as_inner::EnumAsInner;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::Duration,
    sync::atomic::{AtomicU32, Ordering},
};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::task::JoinError;
use ws_derive::LogicalModule;
use std::future::Future;

// use super::m_appmeta_manager::AppMeta;

logical_module_view_impl!(DataGeneralView);
logical_module_view_impl!(DataGeneralView, p2p, P2PModule);
logical_module_view_impl!(DataGeneralView, data_general, DataGeneral);
logical_module_view_impl!(DataGeneralView, kv_store_engine, KvStoreEngine);
logical_module_view_impl!(DataGeneralView, os, OperatingSystem);

pub type DataVersion = u64;
pub type DataItemIdx = u8;

pub const DATA_UID_PREFIX_APP_META: &str = "app";
pub const DATA_UID_PREFIX_FN_KV: &str = "fkv";

pub const CACHE_MODE_TIME_MASK: u16 = 0xf000;
pub const CACHE_MODE_TIME_FOREVER_MASK: u16 = 0x0fff;
pub const CACHE_MODE_TIME_AUTO_MASK: u16 = 0x1fff;

pub const CACHE_MODE_POS_MASK: u16 = 0x0f00;
pub const CACHE_MODE_POS_ALLNODE_MASK: u16 = 0xf0ff;
pub const CACHE_MODE_POS_SPECNODE_MASK: u16 = 0xf1ff;
pub const CACHE_MODE_POS_AUTO_MASK: u16 = 0xf2ff;

pub const CACHE_MODE_MAP_MASK: u16 = 0x00f0;
pub const CACHE_MODE_MAP_COMMON_KV_MASK: u16 = 0xff0f;
pub const CACHE_MODE_MAP_FILE_MASK: u16 = 0xff1f;
// const DATA_UID_PREFIX_OBJ: &str = "obj";

pub fn new_data_unique_id_app(app_name: &str) -> String {
    format!("{}{}", DATA_UID_PREFIX_APP_META, app_name)
}

pub fn new_data_unique_id_fn_kv(key: &[u8]) -> Vec<u8> {
    let mut temp = DATA_UID_PREFIX_FN_KV.as_bytes().to_owned();
    temp.extend(key);
    temp
    // let key_str = str::from_utf8(key).unwrap();
    // format!("{}{}", DATA_UID_PREFIX_FN_KV, key_str)
}

#[derive(LogicalModule)]
pub struct DataGeneral {
    view: DataGeneralView,
    pub rpc_call_data_version_schedule: RPCCaller<proto::DataVersionScheduleRequest>,
    rpc_call_write_once_data: RPCCaller<proto::WriteOneDataRequest>,
    rpc_call_batch_data: RPCCaller<proto::sche::BatchDataRequest>,
    rpc_call_get_data_meta: RPCCaller<proto::DataMetaGetRequest>,
    rpc_call_get_data: RPCCaller<proto::GetOneDataRequest>,

    rpc_handler_write_once_data: RPCHandler<proto::WriteOneDataRequest>,
    rpc_handler_batch_data: RPCHandler<proto::sche::BatchDataRequest>,
    rpc_handler_data_meta_update: RPCHandler<proto::DataMetaUpdateRequest>,
    rpc_handler_get_data_meta: RPCHandler<proto::DataMetaGetRequest>,
    rpc_handler_get_data: RPCHandler<proto::GetOneDataRequest>,

    // 用于跟踪批量传输的状态
    batch_transfers: DashMap<String, (u64, Vec<u8>)>, // 修改类型为 (unique_id -> (version, data))
}

impl DataGeneral {
    fn next_batch_id(&self) -> u32 {
        static NEXT_BATCH_ID: AtomicU32 = AtomicU32::new(1);  // 从1开始,保留0作为特殊值
        NEXT_BATCH_ID.fetch_add(1, Ordering::Relaxed)
    }

    async fn write_data_batch(
        &self,
        unique_id: &[u8],
        version: u64,
        data: proto::DataItem,
        data_item_idx: usize,
        node_id: NodeID,
        batch_size: usize,
    ) -> WSResult<()> {
        let total_size = data.data_sz_bytes();
        let total_batches = (total_size + batch_size - 1) / batch_size;
        
        // 克隆整个 view
        let view = self.view.clone();
        
        // Initialize batch transfer
        let init_req = proto::sche::BatchDataRequest {
            unique_id: unique_id.to_vec(),
            version,
            batch_id: 0,  // 使用 0 作为初始化标记
            total_batches: total_batches as u32,
            data: vec![],
            data_item_idx: data_item_idx as u32,
            is_complete: false,
        };

        let init_resp = self
            .rpc_call_batch_data
            .call(
                view.p2p(),
                node_id,
                init_req,
                Some(Duration::from_secs(60)),
            )
            .await?;

        if !init_resp.success {
            return Err(WsDataError::BatchTransferFailed {
                node: node_id,
                batch: 0,
                reason: init_resp.error,
            }
            .into());
        }

        let batch_id = init_resp.batch_id;

        // Send data in batches
        for batch_idx in 0..total_batches {
            let start = batch_idx * batch_size;
            let end = (start + batch_size).min(total_size);
            let is_last = batch_idx == total_batches - 1;

            let batch_data = data.clone_split_range(start..end);
            let batch_req = proto::sche::BatchDataRequest {
                    unique_id: unique_id.to_vec(),
                    version,
                batch_id,
                    total_batches: total_batches as u32,
                data: batch_data.encode_persist(),
                    data_item_idx: data_item_idx as u32,
                is_complete: is_last,
            };

            let batch_resp = self
                .rpc_call_batch_data
                .call(
                    view.p2p(),
                    node_id,
                    batch_req,
                    Some(Duration::from_secs(60)),
                )
                .await?;

            if !batch_resp.success {
                return Err(WsDataError::BatchTransferFailed {
                    node: node_id,
                    batch: batch_idx as u32,
                    reason: batch_resp.error,
                }
                .into());
            }
        }

        Ok(())
    }

    pub async fn get_or_del_datameta_from_master(
        &self,
        unique_id: &[u8],
        delete: bool,
    ) -> WSResult<DataSetMetaV2> {
        let p2p = self.view.p2p();
        // get meta from master
        let meta = self
            .rpc_call_get_data_meta
            .call(
                p2p,
                p2p.nodes_config.get_master_node(),
                proto::DataMetaGetRequest {
                    unique_id: unique_id.to_vec(),
                    delete,
                },
                Some(Duration::from_secs(60)),
            )
            .await?;

        if meta.serialized_meta.is_empty() {
            return Err(WsDataError::DataSetNotFound {
                uniqueid: unique_id.to_vec(),
            }
            .into());
        }

        bincode::deserialize(&meta.serialized_meta).map_err(|err| {
            WsSerialErr::BincodeErr {
                err,
                context: "get_or_del_datameta_from_master".to_owned(),
            }
            .into()
        })
    }

    pub async fn get_or_del_data(
        &self,
        GetOrDelDataArg {
            meta,
            unique_id,
            ty,
        }: GetOrDelDataArg,
    ) -> WSResult<(DataSetMetaV2, HashMap<DataItemIdx, proto::DataItem>)> {
        let mut data_map = HashMap::new();

        // get meta from master
        let meta = if let Some(meta) = meta {
            meta
        } else {
            self.get_or_del_datameta_from_master(&unique_id, false)
                .await?
        };

        tracing::debug!("get_or_del_data uid: {:?},meta: {:?}", unique_id, meta);

        // basical verify
        for idx in 0..meta.data_item_cnt() {
            let idx = idx as DataItemIdx;
            let check_cache_map = |meta: &DataSetMetaV2| -> WSResult<()> {
                if !meta.cache_mode_visitor(idx).is_map_common_kv()
                    && !meta.cache_mode_visitor(idx).is_map_file()
                {
                    return Err(WsDataError::UnknownCacheMapMode {
                        mode: meta.cache_mode_visitor(idx).0,
                    }
                    .into());
                }
                Ok(())
            };
            check_cache_map(&meta)?;
        }

        // get data
        let p2p = self.view.p2p();

        match ty {
            GetOrDelDataArgType::All => {
                for idx in 0..meta.data_item_cnt() {
                    let idx = idx as DataItemIdx;
                    let resp = self
                        .rpc_call_get_data
                        .call(
                            p2p,
                            meta.get_data_node(idx),
                            proto::GetOneDataRequest {
                                unique_id: unique_id.to_vec(),
                                idxs: vec![idx as u32],
                                delete: false,
                                return_data: true,
                            },
                            Some(Duration::from_secs(60)),
                        )
                        .await?;

                    if !resp.success {
                        return Err(WsDataError::GetDataFailed {
                            unique_id: unique_id.to_vec(),
                            msg: resp.message,
                    }
                    .into());
                }

                    data_map.insert(idx, resp.data[0].clone());
                }
            }
            GetOrDelDataArgType::Delete => {
                for idx in 0..meta.data_item_cnt() {
                    let idx = idx as DataItemIdx;
                    let resp = self
                        .rpc_call_get_data
                        .call(
                            p2p,
                            meta.get_data_node(idx),
                            proto::GetOneDataRequest {
                                unique_id: unique_id.to_vec(),
                                idxs: vec![idx as u32],
                                delete: true,
                                return_data: true,
                            },
                            Some(Duration::from_secs(60)),
                        )
                        .await?;

                    if !resp.success {
                        return Err(WsDataError::GetDataFailed {
                            unique_id: unique_id.to_vec(),
                            msg: resp.message,
                    }
                    .into());
                }

                    data_map.insert(idx, resp.data[0].clone());
                }
            }
            GetOrDelDataArgType::PartialOne { idx } => {
                let resp = self
                    .rpc_call_get_data
                    .call(
                        p2p,
                        meta.get_data_node(idx),
                        proto::GetOneDataRequest {
                            unique_id: unique_id.to_vec(),
                            idxs: vec![idx as u32],
                            delete: false,
                            return_data: true,
                        },
                        Some(Duration::from_secs(60)),
                    )
                    .await?;

                if !resp.success {
                    return Err(WsDataError::GetDataFailed {
                        unique_id: unique_id.to_vec(),
                        msg: resp.message,
                    }
                    .into());
                }

                data_map.insert(idx, resp.data[0].clone());
            }
            GetOrDelDataArgType::PartialMany { idxs } => {
                for idx in idxs {
                    let resp = self
                        .rpc_call_get_data
                        .call(
                            p2p,
                            meta.get_data_node(idx),
                            proto::GetOneDataRequest {
                                unique_id: unique_id.to_vec(),
                                idxs: vec![idx as u32],
                                delete: false,
                                return_data: true,
                            },
                            Some(Duration::from_secs(60)),
                        )
                        .await?;

                    if !resp.success {
                        return Err(WsDataError::GetDataFailed {
                            unique_id: unique_id.to_vec(),
                            msg: resp.message,
                    }
                    .into());
                }

                    data_map.insert(idx, resp.data[0].clone());
                }
            }
        }

        Ok((meta, data_map))
    }

    pub async fn write_data(
        &self,
        unique_id: impl Into<Vec<u8>>,
        datas: Vec<proto::DataItem>,
        context_openode_opetype_operole: Option<(
            NodeID,
            proto::DataOpeType,
            proto::data_schedule_context::OpeRole,
        )>,
    ) -> WSResult<()> {
        let unique_id = unique_id.into();
        let log_tag = format!("[write_data({})]", String::from_utf8_lossy(&unique_id));
        tracing::debug!("{} start write data", log_tag);

        // 获取数据调度计划
        let version_schedule_resp = self
            .rpc_call_data_version_schedule
            .call(
                self.view.p2p(),
                self.view.p2p().nodes_config.get_master_node(),
                proto::DataVersionScheduleRequest {
                    unique_id: unique_id.clone(),
                    context: context_openode_opetype_operole.map(|(node, ope, role)| {
                        proto::DataScheduleContext {
                            each_data_sz_bytes: datas
                                .iter()
                                .map(|d| d.data_sz_bytes() as u32)
                                .collect(),
                            ope_node: node as i64,
                            ope_type: ope as i32,
                            ope_role: Some(role),
                        }
                    }),
                    version: 0,
                },
                Some(Duration::from_secs(60)),
            )
            .await?;

        // Clone the response to extend its lifetime
        let version = version_schedule_resp.version;
        let splits = version_schedule_resp.split.clone();

        // 处理每个数据项
        for (data_item_idx, (data_item, split)) in datas
            .iter()
            .zip(splits.iter())
            .enumerate()
        {
            let mut tasks = Vec::new();
            tracing::debug!(
                "{} processing data item {}/{}",
                log_tag,
                data_item_idx + 1,
                datas.len()
            );

            // 1. 并行写入所有主数据分片
            for (split_idx, split_info) in split.splits.iter().enumerate() {
                tracing::debug!(
                    "{} creating split write task {}/{} for node {}, offset={}, size={}",
                    log_tag,
                    split_idx + 1,
                    split.splits.len(),
                    split_info.node_id,
                    split_info.data_offset,
                    split_info.data_size
                );

                // 克隆必要的数据
                let split_info = split_info.clone();  // 必须克隆，来自临时变量
                let unique_id = unique_id.clone();    // 必须克隆，多个任务需要
                let data_item = data_item.clone_split_range(  // 克隆必要的数据范围
                    split_info.data_offset as usize
                        ..(split_info.data_offset + split_info.data_size) as usize,
                );
                let view = self.view.clone();  // 克隆 view，包含所有模块引用
                let version = version;    // 复制值类型

                let task = tokio::spawn(async move {
                    let resp = view.data_general()
                        .rpc_call_write_once_data
                        .call(
                            view.p2p(),
                            split_info.node_id,
                            proto::WriteOneDataRequest {
                                unique_id,
                                version,
                                data: vec![proto::DataItemWithIdx {
                                    idx: data_item_idx as u32,
                                    data: Some(data_item),
                                }],
                            },
                            Some(Duration::from_secs(60)),
                        )
                        .await?;
                    Ok::<proto::WriteOneDataResponse, WSError>(resp)
                });
                tasks.push(task);
            }

            // 2. 并行写入缓存数据（完整数据）
            let visitor = CacheModeVisitor(version_schedule_resp.cache_mode[data_item_idx] as u16);
            let need_cache = visitor.is_map_common_kv() || visitor.is_map_file();
            
            let cache_nodes: Vec<NodeID> = if need_cache {
                split.splits.iter().map(|s| s.node_id).collect()
            } else {
                vec![]
            };

            if !cache_nodes.is_empty() {
                tracing::debug!(
                    "{} found {} cache nodes: {:?}",
                    log_tag,
                    cache_nodes.len(),
                    cache_nodes
                );

                // 使用信号量限制并发的批量传输数量
                const MAX_CONCURRENT_TRANSFERS: usize = 3;
                let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_TRANSFERS));

                for (cache_idx, &node_id) in cache_nodes.iter().enumerate() {
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    tracing::debug!(
                        "{} creating cache write task {}/{} for node {}",
                        log_tag,
                        cache_idx + 1,
                        cache_nodes.len(),
                        node_id
                    );

                    // 创建批量传输任务
                    let unique_id = unique_id.clone();
                    let data_item = data_item.clone();
                    let view = self.view.clone();

                    let task = tokio::spawn(async move {
                        let _permit = permit;
                        view.data_general()
                            .write_data_batch(
                                &unique_id,
                                version,
                                data_item.clone(),
                                data_item_idx,
                                node_id,
                                1024 * 1024, // 1MB batch size
                            )
                            .await?;
                        Ok::<proto::WriteOneDataResponse, WSError>(proto::WriteOneDataResponse {
                            remote_version: version,
                            success: true,
                            message: String::new(),
                        })
                    });
                    tasks.push(task);
                }
            }

            // 等待所有写入任务完成
            for task in tasks {
                task.await??;
            }
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct DataMetaSys {
    pub cache: i32,
    pub distribute: i32,
}
impl From<DataMeta> for DataMetaSys {
    fn from(d: DataMeta) -> Self {
        Self {
            cache: d.cache,
            distribute: d.distribute,
        }
    }
}
impl Into<DataMeta> for DataMetaSys {
    fn into(self) -> DataMeta {
        DataMeta {
            cache: self.cache,
            distribute: self.distribute,
        }
    }
}

/// depracated, latest is v2
/// the data's all in one meta
/// https://fvd360f8oos.feishu.cn/docx/XoFudWhAgox84MxKC3ccP1TcnUh#share-Tqqkdxubpokwi5xREincb1sFnLc
#[derive(Serialize, Deserialize)]
pub struct DataSetMetaV1 {
    // unique_id: Vec<u8>,
    pub version: u64,
    pub data_metas: Vec<DataMetaSys>,
    pub synced_nodes: HashSet<NodeID>,
}

pub type CacheMode = u16;

/// the data's all in one meta
///
/// attention: new from `DataSetMetaBuilder`
///
/// https://fvd360f8oos.feishu.cn/docx/XoFudWhAgox84MxKC3ccP1TcnUh#share-Tqqkdxubpokwi5xREincb1sFnLc
#[derive(Serialize, Deserialize, Debug)]
pub struct DataSetMetaV2 {
    // unique_id: Vec<u8>,
    api_version: u8,
    pub version: u64,
    pub cache_mode: Vec<CacheMode>,
    /// the data splits for each data item, the index is the data item index
    pub datas_splits: Vec<DataSplit>,
}

impl DataSetMetaV2 {
    pub fn cache_mode_visitor(&self, idx: DataItemIdx) -> CacheModeVisitor {
        CacheModeVisitor(self.cache_mode[idx as usize])
    }
    
    pub fn data_item_cnt(&self) -> usize {
        self.datas_splits.len()
    }

    pub fn get_data_node(&self, idx: DataItemIdx) -> NodeID {
        // 获取指定数据项的主节点
        self.datas_splits[idx as usize].splits[0].node_id
    }
}

pub type DataSetMeta = DataSetMetaV2;

// message EachNodeSplit{
//     uint32 node_id=1;
//     uint32 data_offset=2;
//     uint32 data_size=3;
//   }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EachNodeSplit {
    pub node_id: NodeID,
    pub data_offset: u32,
    pub data_size: u32,
    pub cache_mode: u32,  // 添加 cache_mode 字段
}

impl EachNodeSplit {
    pub fn cache_mode_visitor(&self) -> CacheModeVisitor {
        CacheModeVisitor(self.cache_mode as u16)
    }
}

/// the split of one dataitem
/// we need to know the split size for one data
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataSplit {
    pub splits: Vec<EachNodeSplit>,
}

pub type DataSplitIdx = usize;

// impl DataSplit {
//     /// node_2_datas will be consumed partially
//     pub fn recorver_data(
//         &self,
//         unique_id: &[u8],
//         idx: DataItemIdx,
//         node_2_datas: &mut HashMap<(NodeID, DataItemIdx), proto::DataItem>,
//     ) -> WSResult<Vec<u8>> {
//         let nodes = node_2_datas
//             .iter()
//             .filter(|v| v.0 .1 == idx)
//             .map(|v| v.0 .0)
//             .collect::<Vec<_>>();

//         let mut each_node_splits: HashMap<NodeID, (proto::DataItem, Option<EachNodeSplit>)> =
//             HashMap::new();

//         for node in nodes {
//             let data = node_2_datas.remove(&(node, idx)).unwrap();
//             let _ = each_node_splits.insert(node, (data, None));
//         }

//         let mut max_size = 0;
//         let mut missing = vec![];

//         // zip with split info
//         //  by the way, check if the split is missing
//         for split in &self.splits {
//             let Some(find) = each_node_splits.get_mut(&split.node_id) else {
//                 missing.push((*split).clone());
//                 continue;
//             };
//             find.1 = Some(split.clone());
//             if split.data_offset + split.data_size > max_size {
//                 max_size = split.data_offset + split.data_size;
//             }
//         }

//         if missing.len() > 0 {
//             return Err(WsDataError::SplitRecoverMissing {
//                 unique_id: unique_id.to_owned(),
//                 idx,
//                 missing,
//             }
//             .into());
//         }

//         let mut recover = vec![0; max_size.try_into().unwrap()];

//         for (_node, (data, splitmeta)) in each_node_splits {
//             let splitmeta = splitmeta.unwrap();
//             let begin = splitmeta.data_offset as usize;
//             let end = begin + splitmeta.data_size as usize;
//             recover[begin..end].copy_from_slice(data.as_ref());
//         }

//         Ok(recover)
//     }
// }

impl Into<proto::EachNodeSplit> for EachNodeSplit {
    fn into(self) -> proto::EachNodeSplit {
        proto::EachNodeSplit {
            node_id: self.node_id,
            data_offset: self.data_offset,
            data_size: self.data_size,
        }
    }
}

impl Into<proto::DataSplit> for DataSplit {
    fn into(self) -> proto::DataSplit {
        proto::DataSplit {
            splits: self.splits.into_iter().map(|s| s.into()).collect(),
        }
    }
}
//     uint32 split_size = 1;
//   repeated uint32 node_ids = 2;

#[derive(Debug, Clone, Copy)]
pub struct CacheModeVisitor(pub u16);

macro_rules! generate_cache_mode_methods {
    // The macro takes a list of pairs of the form [time, mask] and generates methods.
    ($(($group:ident, $mode:ident)),*) => {
        paste!{
            impl CacheModeVisitor {
                $(
                    pub fn [<is _ $group _ $mode>](&self) -> bool {
                        (self.0 & [<CACHE_MODE_ $group:upper _MASK>]) ==
                            ([<CACHE_MODE_ $group:upper _ $mode:upper _MASK>] & [<CACHE_MODE_ $group:upper _MASK>])
                    }
                )*
            }
            impl DataSetMetaBuilder {
                $(
                    pub fn [<cache_mode_ $group _ $mode>](&mut self, idx: DataItemIdx) -> &mut Self {
                        self.assert_cache_mode_len();
                        self.building.as_mut().unwrap().cache_mode[idx as usize] =
                            (self.building.as_mut().unwrap().cache_mode[idx as usize] & ![<CACHE_MODE_ $group:upper _MASK>]) |
                            ([<CACHE_MODE_ $group:upper _ $mode:upper _MASK>] & [<CACHE_MODE_ $group:upper _MASK>]);
                        self
                    }
                )*
            }
        }
    };
}
generate_cache_mode_methods!(
    (time, forever),
    (time, auto),
    (pos, allnode),
    (pos, specnode),
    (pos, auto),
    (map, common_kv),
    (map, file)
);

#[test]
fn test_cache_mode_visitor() {
    let cache_mode_visitor = CacheModeVisitor(CACHE_MODE_TIME_FOREVER_MASK);
    assert!(cache_mode_visitor.is_time_forever());
    assert!(!cache_mode_visitor.is_time_auto());

    let cache_mode_visitor = CacheModeVisitor(CACHE_MODE_POS_ALLNODE_MASK);
    assert!(cache_mode_visitor.is_pos_allnode());
    assert!(!cache_mode_visitor.is_pos_specnode());
    assert!(!cache_mode_visitor.is_pos_auto());

    let cache_mode_visitor = CacheModeVisitor(CACHE_MODE_MAP_FILE_MASK);
    assert!(cache_mode_visitor.is_map_file());
    assert!(!cache_mode_visitor.is_map_common_kv());

    // test builder

    let meta = DataSetMetaBuilder::new()
        .set_data_splits(vec![DataSplit { splits: vec![] }])
        .cache_mode_map_file(0)
        .cache_mode_time_forever(0)
        .build();
    assert!(meta.cache_mode_visitor(0).is_map_file());
    assert!(!meta.cache_mode_visitor(0).is_map_common_kv());
    assert!(meta.cache_mode_visitor(0).is_time_forever());
    assert!(!meta.cache_mode_visitor(0).is_time_auto());
    let meta = DataSetMetaBuilder::new()
        .set_data_splits(vec![DataSplit { splits: vec![] }])
        .cache_mode_map_common_kv(0)
        .cache_mode_time_forever(0)
        .build();
    assert!(meta.cache_mode_visitor(0).is_map_common_kv());
    assert!(!meta.cache_mode_visitor(0).is_map_file());
    assert!(meta.cache_mode_visitor(0).is_time_forever());
    assert!(!meta.cache_mode_visitor(0).is_time_auto());
}

pub struct DataSetMetaBuilder {
    building: Option<DataSetMetaV2>,
}
impl From<DataSetMetaV2> for DataSetMetaBuilder {
    fn from(d: DataSetMetaV2) -> Self {
        Self { building: Some(d) }
    }
}
impl DataSetMetaBuilder {
    pub fn new() -> Self {
        Self {
            building: Some(DataSetMetaV2 {
                version: 0,
                cache_mode: vec![],
                api_version: 2,
                datas_splits: vec![],
            }),
        }
    }
    fn assert_cache_mode_len(&self) {
        if self.building.as_ref().unwrap().cache_mode.len() == 0 {
            panic!("please set_data_splits before set_cache_mode");
        }
    }

    pub fn version(&mut self, version: u64) -> &mut Self {
        self.building.as_mut().unwrap().version = version;
        self
    }

    #[must_use]
    pub fn set_data_splits(&mut self, splits: Vec<DataSplit>) -> &mut Self {
        let building = self.building.as_mut().unwrap();
        building.datas_splits = splits;
        building.cache_mode = vec![0; building.datas_splits.len()];
        self
    }

    pub fn set_cache_mode(&mut self, idx: DataItemIdx, mode: u16) -> &mut Self {
        self.building.as_mut().unwrap().cache_mode[idx as usize] = mode;
        self
    }

    pub fn set_cache_mode_for_all(&mut self, mode: Vec<u16>) -> &mut Self {
        self.building.as_mut().unwrap().cache_mode = mode;
        assert_eq!(
            self.building.as_mut().unwrap().cache_mode.len(),
            self.building.as_mut().unwrap().datas_splits.len(),
            "cache mode len must be equal to data splits len"
        );
        self
    }

    pub fn build(&mut self) -> DataSetMetaV2 {
        self.building.take().unwrap()
    }
}

// impl From<DataSetMetaV1> for DataSetMetaV2 {
//     fn from(
//         DataSetMetaV1 {
//             version,
//             data_metas: _,
//             synced_nodes: _,
//         }: DataSetMetaV1,
//     ) -> Self {
//         DataSetMetaBuilder::new()
//             .version(version)
//             .cache_mode_pos_allnode()
//             .build()
//         // DataSetMetaV2 {
//         //     version,
//         //     data_metas,
//         //     synced_nodes,
//         // }
//     }
// }

mod test {
    #[test]
    fn test_option_and_vec_serialization_size() {
        // 定义一个具体的值
        let value: i32 = 42;

        // 创建 Option 类型的变量
        let some_value: Option<i32> = Some(value);
        let none_value: Option<i32> = None;

        // 创建 Vec 类型的变量
        let empty_vec: Vec<i32> = Vec::new();
        let single_element_vec: Vec<i32> = vec![value];

        let some_empty_vec: Option<Vec<i32>> = Some(vec![]);
        let some_one_vec: Option<Vec<i32>> = Some(vec![value]);

        // 序列化
        let serialized_some = bincode::serialize(&some_value).unwrap();
        let serialized_none = bincode::serialize(&none_value).unwrap();
        let serialized_empty_vec = bincode::serialize(&empty_vec).unwrap();
        let serialized_single_element_vec = bincode::serialize(&single_element_vec).unwrap();
        let serialized_some_empty_vec = bincode::serialize(&some_empty_vec).unwrap();
        let serialized_some_one_vec = bincode::serialize(&some_one_vec).unwrap();

        // 获取序列化后的字节大小
        let size_some = serialized_some.len();
        let size_none = serialized_none.len();
        let size_empty_vec = serialized_empty_vec.len();
        let size_single_element_vec = serialized_single_element_vec.len();
        let size_some_empty_vec = serialized_some_empty_vec.len();
        let size_some_one_vec = serialized_some_one_vec.len();

        // 打印结果
        println!("Size of serialized Some(42): {}", size_some);
        println!("Size of serialized None: {}", size_none);
        println!("Size of serialized empty Vec: {}", size_empty_vec);
        println!(
            "Size of serialized Vec with one element (42): {}",
            size_single_element_vec
        );
        println!(
            "Size of serialized Some(empty Vec): {}",
            size_some_empty_vec
        );
        println!(
            "Size of serialized Some(one element Vec): {}",
            size_some_one_vec
        );

        // 比较大小
        assert!(
            size_some > size_none,
            "Expected serialized Some to be larger than serialized None"
        );
        assert!(
            size_single_element_vec > size_empty_vec,
            "Expected serialized Vec with one element to be larger than serialized empty Vec"
        );
    }
}

pub struct GetOrDelDataArg {
    pub meta: Option<DataSetMetaV2>,
    pub unique_id: Vec<u8>,
    pub ty: GetOrDelDataArgType,
}

#[derive(Clone)]
pub enum GetOrDelDataArgType {
    All,
    Delete,
    PartialOne { idx: DataItemIdx },
    PartialMany { idxs: BTreeSet<DataItemIdx> },
}

impl DataGeneralView {
    fn inner_new(args: LogicalModuleNewArgs) -> Self {
        Self {
            inner: args.logical_modules_ref,
        }
    }
}

// 为 proto::EachNodeSplit 实现 cache_mode_visitor
// impl proto::EachNodeSplit {
//     pub fn cache_mode_visitor(&self) -> CacheModeVisitor {
//         CacheModeVisitor(self.cache_mode as u16)
//     }
// }

// 实现 From trait 处理错误转换
impl From<JoinError> for WSError {
    fn from(err: JoinError) -> Self {
        WsNetworkLogicErr::TaskJoinError { err }.into()
    }
}

#[async_trait]
impl LogicalModule for DataGeneral {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: DataGeneralView::new(args.logical_modules_ref.clone()),
            rpc_call_data_version_schedule: RPCCaller::new(),
            rpc_call_write_once_data: RPCCaller::new(),
            rpc_call_batch_data: RPCCaller::new(),
            rpc_call_get_data_meta: RPCCaller::new(),
            rpc_call_get_data: RPCCaller::new(),
            
            rpc_handler_write_once_data: RPCHandler::new(),
            rpc_handler_batch_data: RPCHandler::new(),
            rpc_handler_data_meta_update: RPCHandler::new(),
            rpc_handler_get_data_meta: RPCHandler::new(),
            rpc_handler_get_data: RPCHandler::new(),
            
            batch_transfers: DashMap::new(),
        }
    }

    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        Ok(vec![])
    }
}

fn flush_the_data(
    log_tag: &str,
    unique_id: &[u8],
    version: u64,
    split_size: usize,
    view: &DataGeneralView,
    one_data_item: &proto::DataItem,
    nodeid: NodeID,
    offset: usize,
    dataitem_idx: usize,
    write_source_data_tasks: &mut Vec<JoinHandle<WSResult<WriteOneDataResponse>>>,
) {
    let log_tag = log_tag.to_owned();
    let unique_id = unique_id.to_owned();
    let view = view.clone();
    let one_data_item_split = one_data_item.clone_split_range(offset..offset + split_size);
    let t = tokio::spawn(async move {
        let req = WriteOneDataRequest {
            unique_id,
            version,
            data: vec![proto::DataItemWithIdx {
                idx: dataitem_idx as u32,
                data: Some(one_data_item_split),
            }],
        };
        tracing::debug!(
            "[{}] write_data flushing, target node: {}, `WriteOneDataRequest` msg_id: {}",
            log_tag,
            nodeid,
            req.msg_id()
        );
        view.data_general()
            .rpc_call_write_once_data
            .call(view.p2p(), nodeid, req, Some(Duration::from_secs(60)))
            .await
    });
    write_source_data_tasks.push(t);
}
