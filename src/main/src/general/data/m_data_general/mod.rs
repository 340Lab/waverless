mod dataitem;
mod batch;
mod batch_handler;

use crate::general::data::m_data_general::dataitem::{WantIdxIter, WriteSplitDataTaskGroup};
use crate::general::data::m_data_general::batch_handler::{BatchReceiveState, SharedWithBatchHandler};

use crate::general::{
    data::m_kv_store_engine::{
        KeyTypeDataSetItem, KeyTypeDataSetMeta, KvAdditionalConf, KvStoreEngine, KvVersion,
    },
    m_os::OperatingSystem,
    network::{
        m_p2p::{P2PModule, RPCCaller, RPCHandler, RPCResponsor},
        proto::{
            self, BatchDataBlockType, DataMeta, DataMetaGetRequest, DataVersionScheduleRequest, 
            WriteOneDataRequest, WriteOneDataResponse,
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
use tokio::sync::mpsc;

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

/// 默认数据块大小 (4MB)
pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024 * 1024;

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
    rpc_call_batch_data: RPCCaller<proto::BatchDataRequest>,
    rpc_call_get_data_meta: RPCCaller<proto::DataMetaGetRequest>,
    rpc_call_get_data: RPCCaller<proto::GetOneDataRequest>,

    rpc_handler_write_once_data: RPCHandler<proto::WriteOneDataRequest>,
    rpc_handler_batch_data: RPCHandler<proto::BatchDataRequest>,
    rpc_handler_data_meta_update: RPCHandler<proto::DataMetaUpdateRequest>,
    rpc_handler_get_data_meta: RPCHandler<proto::DataMetaGetRequest>,
    rpc_handler_get_data: RPCHandler<proto::GetOneDataRequest>,
    
    // 批量数据接收状态管理
    batch_receive_states: DashMap<proto::BatchRequestId, super::batch_handler::BatchReceiveState>,
}

impl DataGeneral {
    #[allow(dead_code)]
    fn next_batch_id(&self) -> u32 {
        static NEXT_BATCH_ID: AtomicU32 = AtomicU32::new(1);  // 从1开始,保留0作为特殊值
        NEXT_BATCH_ID.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn write_data_batch(
        &self,
        unique_id: &[u8],
        version: u64,
        data: proto::DataItem,
        data_item_idx: usize,
        node_id: NodeID,
    ) -> WSResult<()> {
        // 调用 batch_transfer 函数处理数据传输
        batch_transfer(
            unique_id.to_vec(),
            version,
            node_id,
            Arc::new(DataItemSource::new(data)),
            self.view.clone(),
        ).await
    }

    async fn batch_transfer(
        unique_id: Vec<u8>,
        version: u64,
        target_node: NodeID,
        data: Arc<DataItemSource>,
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
                    return Err(WsDataError::BatchTransferFailed {
                        request_id: proto::BatchRequestId {
                            node_id: target_node.into(),
                            sequence: block_idx.into(),
                        },
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

                    let _ = data_map.insert(idx, resp.data[0].clone());
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

                    let _ = data_map.insert(idx, resp.data[0].clone());
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

                let _ = data_map.insert(idx, resp.data[0].clone());
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

                    let _ = data_map.insert(idx, resp.data[0].clone());
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
        let mut iter = WantIdxIter::new(&GetOrDelDataArgType::All, datas.len() as u8);
        while let Some(data_item_idx) = iter.next() {
            let data_item = &datas[data_item_idx as usize];
            let split = &splits[data_item_idx as usize];
            let mut primary_tasks = Vec::new();
            
            // 1. 并行写入所有主数据分片
            let mut split_iter = WantIdxIter::new(&GetOrDelDataArgType::All, split.splits.len() as u8);
            while let Some(split_idx) = split_iter.next() {
                let split_info = &split.splits[split_idx as usize];
                tracing::debug!("{} creating split write task {}/{} for node {}, offset={}, size={}",
                    log_tag, split_idx + 1, split.splits.len(), split_info.node_id, split_info.data_offset, split_info.data_size);
                let split_info = split_info.clone();
                let unique_id_clone = unique_id.clone();
                let data_item_primary = data_item.clone_split_range(
                    split_info.data_offset as usize
                        ..(split_info.data_offset + split_info.data_size) as usize
                );
                let view = self.view.clone();
                let version_copy = version;
                let task = tokio::spawn(async move {
                                        view.data_general()
                        .rpc_call_write_once_data
                        .call(
                            view.p2p(),
                            split_info.node_id,
                            proto::WriteOneDataRequest {
                                unique_id: unique_id_clone,
                                version: version_copy,
                                data: vec![proto::DataItemWithIdx {
                                    idx: data_item_idx as u32,
                                    data: Some(data_item_primary),
                                }],
                            },
                            Some(Duration::from_secs(60)),
                        )
                        .await
                });
                primary_tasks.push(task);
            }

            // 2. 并行写入缓存数据（完整数据）
            let visitor = CacheModeVisitor(version_schedule_resp.cache_mode[data_item_idx as usize] as u16);
            let need_cache = visitor.is_map_common_kv() || visitor.is_map_file();
            let cache_nodes: Vec<NodeID> = if need_cache {
                split.splits.iter().map(|s| s.node_id).collect()
            } else {
                vec![]
            };

            let mut cache_tasks = Vec::new();
            if !cache_nodes.is_empty() {
                tracing::debug!("{} found {} cache nodes: {:?}", log_tag, cache_nodes.len(), cache_nodes);
                const MAX_CONCURRENT_TRANSFERS: usize = 3;
                let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_TRANSFERS));
                
                let mut cache_iter = WantIdxIter::new(&GetOrDelDataArgType::All, cache_nodes.len() as u8);
                while let Some(cache_idx) = cache_iter.next() {
                    let node_id = cache_nodes[cache_idx as usize];
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    tracing::debug!("{} creating cache write task {}/{} for node {}", log_tag, cache_idx + 1, cache_nodes.len(), node_id);
                    let unique_id_clone = unique_id.clone();
                    let data_item_cache = data_item.clone();
                    let view = self.view.clone();
                    let task = tokio::spawn(async move {
                        let _permit = permit; // 持有permit直到任务完成
                        view.data_general()
                            .write_data_batch(&unique_id_clone, version, data_item_cache, data_item_idx as usize, node_id)
                            .await?;
                        Ok::<proto::WriteOneDataResponse, WSError>(proto::WriteOneDataResponse {
                            remote_version: version,
                            success: true,
                            message: String::new(),
                        })
                    });
                    cache_tasks.push(task);
                }
            }

            let primary_results = futures::future::join_all(primary_tasks).await;
            let cache_results = futures::future::join_all(cache_tasks).await;

            if primary_results.iter().any(|res| res.is_err()) || cache_results.iter().any(|res| res.is_err()) {
                let error_msg = format!("主节点或缓存节点数据写入失败");
                tracing::error!("{}", error_msg);
                return Err(WSError::WsDataError(WsDataError::WriteDataFailed {
                    unique_id: unique_id.clone(),
                    message: error_msg,
                }));
            }
        }

        Ok(())
    }

    async fn rpc_handle_write_one_data(
        &self,
        responsor: RPCResponsor<WriteOneDataRequest>,
        req: WriteOneDataRequest,
    ) {
        tracing::debug!("verify data meta bf write data");
        let kv_store_engine = self.view.kv_store_engine();

        // Step1: verify version
        // take old meta
        #[allow(unused_assignments)]
        let mut required_meta: Option<(usize, DataSetMetaV2)> = None;
        {
            let keybytes: Vec<u8> = KeyTypeDataSetMeta(&req.unique_id).make_key();
            let fail_by_overwrite = || async {
                let message = "New data version overwrite".to_owned();
                tracing::warn!("{}", message);
                responsor
                    .send_resp(WriteOneDataResponse {
                        remote_version: 0,
                        success: false,
                        message,
                    })
                    .await
                    .todo_handle();
            };
            let fail_with_msg = |message: String| async {
                tracing::warn!("{}", message);
                responsor
                    .send_resp(WriteOneDataResponse {
                        remote_version: 0,
                        success: false,
                        message,
                    })
                    .await
                    .todo_handle();
            };

            loop {
                // tracing::debug!("verify version loop");
                let lock =
                    kv_store_engine.with_rwlock(&KeyTypeDataSetMeta(&req.unique_id).make_key());
                let guard = KeyLockGuard::Read(lock.read());
                required_meta = kv_store_engine.get(
                    &KeyTypeDataSetMeta(&req.unique_id),
                    true,
                    KvAdditionalConf {},
                ); //tofix, master send maybe not synced
                let old_dataset_version = if required_meta.is_none() {
                    0
                } else {
                    required_meta.as_ref().unwrap().1.version
                };
                // need to wait for new version
                if required_meta.is_none()
                    || required_meta.as_ref().unwrap().1.version < req.version
                {
                    if required_meta.is_none() {
                        tracing::debug!("no data version, waiting for notify");
                    } else {
                        tracing::debug!(
                            "data version is old({}) at node({}), waiting for new notify({})",
                            required_meta.as_ref().unwrap().1.version,
                            self.view.p2p().nodes_config.this_node(),
                            req.version
                        );
                    }

                    let (kv_version, new_value) = kv_store_engine
                        .register_waiter_for_new(&keybytes, guard)
                        .await
                        .unwrap_or_else(|err| {
                            panic!("fail to wait for new data version: {:?}", err);
                        });

                    let Some(new_value) = new_value.as_raw_data() else {
                        fail_with_msg(format!(
                            "fatal error, kv value supposed to be DataSetMeta, rathe than {:?}",
                            new_value
                        ))
                        .await;
                        return;
                    };

                    // deserialize
                    let new_value = bincode::deserialize::<DataSetMeta>(&new_value);
                    if let Err(err) = new_value {
                        fail_with_msg(format!(
                            "fatal error, kv value deserialization failed: {}",
                            err
                        ))
                        .await;
                        return;
                    }
                    let new_value = new_value.unwrap();

                    // version check
                    if new_value.version > req.version {
                        fail_by_overwrite().await;
                        return;
                    } else if new_value.version < req.version {
                        tracing::debug!("recv data version({}) is old than required({}), waiting for new notify",new_value.version, req.version);
                        // still need to wait for new version
                        continue;
                    } else {
                        required_meta = Some((kv_version, new_value));
                        break;
                    }
                } else if old_dataset_version > req.version {
                    drop(guard);
                    fail_by_overwrite().await;
                    return;
                } else {
                    tracing::debug!(
                        "data version is matched cur({}) require({}) // 0 should be invalid",
                        old_dataset_version,
                        req.version
                    );
                    break;
                }
            }
        }

        // Step3: write data
        tracing::debug!("start to write data");
        let lock = kv_store_engine.with_rwlock(&KeyTypeDataSetMeta(&req.unique_id).make_key());
        let guard = KeyLockGuard::Write(lock.write());
        let check_meta = kv_store_engine.get(
            &KeyTypeDataSetMeta(&req.unique_id),
            true,
            KvAdditionalConf {},
        ); //tofix, master send maybe not synced
        if check_meta.is_none()
            || check_meta.as_ref().unwrap().0 != required_meta.as_ref().unwrap().0
        {
            drop(guard);
            responsor
                .send_resp(WriteOneDataResponse {
                    remote_version: if check_meta.is_none() {
                        0
                    } else {
                        check_meta.as_ref().unwrap().1.version
                    },
                    success: false,
                    message: "meta is updated again, cancel write".to_owned(),
                })
                .await
                .todo_handle();
            return;
        }

        for data_with_idx in req.data.into_iter() {
            let proto::DataItemWithIdx { idx, data } = data_with_idx;
            let data = data.unwrap();
            let serialize = data.encode_persist();
            tracing::debug!(
                "writing data part uid({:?}) idx({}) item({})",
                req.unique_id,
                idx,
                data.to_string()
            );
            if let Err(err) = kv_store_engine.set(
                KeyTypeDataSetItem {
                    uid: req.unique_id.as_ref(),
                    idx: idx as u8,
                },
                &serialize,
                true,
            ) {
                tracing::warn!("flush error: {}", err)
            }
        }
        kv_store_engine.flush();
        drop(guard);
        tracing::debug!("data is written");
        responsor
            .send_resp(WriteOneDataResponse {
                remote_version: req.version,
                success: true,
                message: "".to_owned(),
            })
            .await
            .todo_handle();
    }

    async fn rpc_handle_data_meta_update(
        &self,
        responsor: RPCResponsor<proto::DataMetaUpdateRequest>,
        mut req: proto::DataMetaUpdateRequest,
    ) {
        struct Defer {
            node: NodeID,
        }
        impl Drop for Defer {
            fn drop(&mut self) {
                tracing::debug!("rpc_handle_data_meta_update return at node({})", self.node);
            }
        }
        let _defer = Defer {
            node: self.view.p2p().nodes_config.this_node(),
        };

        let key = KeyTypeDataSetMeta(&req.unique_id);
        let keybytes = key.make_key();

        tracing::debug!("rpc_handle_data_meta_update {:?}", req);
        let kv_lock = self.view.kv_store_engine().with_rwlock(&keybytes);
        let _kv_write_lock_guard = kv_lock.write();

        if let Some((_old_version, mut old_meta)) =
            self.view.kv_store_engine().get(&key, true, KvAdditionalConf {})
        {
            if old_meta.version > req.version {
                drop(_kv_write_lock_guard);
                let err_msg = "New data version is smaller, failed update";
                tracing::warn!("{}", err_msg);
                responsor
                    .send_resp(proto::DataMetaUpdateResponse {
                        version: old_meta.version,
                        message: err_msg.to_owned(),
                    })
                    .await
                    .todo_handle();
                return;
            }
            old_meta.version = req.version;
            if req.serialized_meta.len() > 0 {
                self.view.kv_store_engine()
                    .set_raw(&keybytes, std::mem::take(&mut req.serialized_meta), true)
                    .todo_handle();
            } else {
                self.view.kv_store_engine()
                    .set(key, &old_meta, true)
                    .todo_handle();
            }
        } else {
            if req.serialized_meta.len() > 0 {
                tracing::debug!(
                    "set new meta data, {:?}",
                    bincode::deserialize::<DataSetMeta>(&req.serialized_meta)
                );
                self.view.kv_store_engine()
                    .set_raw(&keybytes, std::mem::take(&mut req.serialized_meta), true)
                    .todo_handle();
            } else {
                drop(_kv_write_lock_guard);
                let err_msg = "Old meta data not found and missing new meta";
                tracing::warn!("{}", err_msg);
                responsor
                    .send_resp(proto::DataMetaUpdateResponse {
                        version: 0,
                        message: err_msg.to_owned(),
                    })
                    .await
                    .todo_handle();
                return;
            }
        }
        drop(_kv_write_lock_guard);
        tracing::debug!("rpc_handle_data_meta_update success");
        responsor
            .send_resp(proto::DataMetaUpdateResponse {
                version: req.version,
                message: "Update success".to_owned(),
            })
            .await
            .todo_handle();
    }

    async fn rpc_handle_get_data_meta(
        &self,
        req: proto::DataMetaGetRequest,
        responsor: RPCResponsor<proto::DataMetaGetRequest>,
    ) -> WSResult<()> {
        tracing::debug!("rpc_handle_get_data_meta with req({:?})", req);
        let meta = self.view.get_metadata(&req.unique_id, req.delete).await?;
        tracing::debug!("rpc_handle_get_data_meta data meta found");
        
        let serialized_meta = bincode::serialize(&meta).map_err(|err| {
            WsSerialErr::BincodeErr {
                err,
                context: "rpc_handle_get_data_meta".to_owned(),
            }
        })?;

        responsor
            .send_resp(proto::DataMetaGetResponse { serialized_meta })
            .await?;

        Ok(())
    }

    async fn rpc_handle_get_one_data(
        &self,
        responsor: RPCResponsor<proto::GetOneDataRequest>,
        req: proto::GetOneDataRequest,
    ) -> WSResult<()> {
        tracing::debug!("starting rpc_handle_get_one_data {:?}", req);

        let kv_store_engine = self.view.kv_store_engine();
        let _ = self.view
            .get_metadata(&req.unique_id, req.delete)
            .await
            .map_err(|err| {
                tracing::warn!("rpc_handle_get_one_data get_metadata failed: {:?}", err);
                err
            })?;

        let mut got_or_deleted = vec![];
        let mut kv_ope_err = vec![];

        for idx in req.idxs {
            let value = if req.delete {
                match kv_store_engine.del(
                    KeyTypeDataSetItem {
                        uid: req.unique_id.as_ref(),
                        idx: idx as u8,
                    },
                    false,
                ) {
                    Ok(value) => value,
                    Err(e) => {
                        kv_ope_err.push(e);
                        None
                    }
                }
            } else {
                kv_store_engine.get(
                    &KeyTypeDataSetItem {
                        uid: req.unique_id.as_ref(),
                        idx: idx as u8,
                    },
                    false,
                    KvAdditionalConf {},
                )
            };
            got_or_deleted.push(value);
        }

        let (success, message): (bool, String) = if kv_ope_err.len() > 0 {
            (false, {
                let mut msg = String::from("KvEngine operation failed: ");
                for e in kv_ope_err.iter() {
                    msg.push_str(&format!("{:?}", e));
                }
                msg
            })
        } else if got_or_deleted.iter().all(|v| v.is_some()) {
            (true, "success".to_owned())
        } else {
            tracing::warn!("some data not found");
            (false, "some data not found".to_owned())
        };

        let mut got_or_deleted_checked: Vec<proto::DataItem> = vec![];
        if success {
            for v in got_or_deleted {
                let decode_res = proto::DataItem::decode_persist(v.unwrap().1);
                tracing::debug!("decode_res type: {:?}", decode_res.to_string());
                got_or_deleted_checked.push(decode_res);
            }
        }

        responsor
            .send_resp(proto::GetOneDataResponse {
                success,
                data: got_or_deleted_checked,
                message,
            })
            .await?;

        Ok(())
    }

    async fn rpc_handle_batch_data(
        &self,
        responsor: RPCResponsor<proto::BatchDataRequest>,
        req: proto::BatchDataRequest,
    ) -> WSResult<()> {
        // 1. 查找或创建状态
        let (state, is_new_state) = self.batch_receive_states
            .entry(req.request_id.clone())
            .or_insert_with(|| {
                // 通过 WriteSplitDataTaskGroup::new 创建任务组和句柄
                let (group, handle) = super::dataitem::WriteSplitDataTaskGroup::new(
                    req.request_id.clone(),
                    Vec::new(), // TODO: 根据实际需求设置分片范围
                    req.block_type,
                    0, // TODO: 根据实际需求设置版本号
                ).await;
                
                (super::batch_handler::BatchReceiveState::new(handle, group), true)
            });

        // 2. 提交分片数据
        state.handle.submit_split(
            req.block_idx * DEFAULT_BLOCK_SIZE,
            req.data
        ).await?;

        // 3. 更新响应器
        state.shared.update_responsor(responsor).await;

        // 4. 只在首次创建状态时启动完成监控任务
        if is_new_state {
            let state_clone = state.clone();
            let request_id = req.request_id.clone();
            let batch_receive_states = self.batch_receive_states.clone();
            
            tokio::spawn(async move {
                // 等待所有任务完成
                if let Err(e) = state_clone.handle.wait_all_tasks().await {
                    tracing::error!("Failed to wait for tasks: {}", e);
                    return;
                }

                // 发送最终响应
                if let Some(final_responsor) = state_clone.shared.get_final_responsor().await {
                    if let Err(e) = final_responsor.response(Ok(())).await {
                        tracing::error!("Failed to send final response: {}", e);
                    }
                }

                // 清理状态
                batch_receive_states.remove(&request_id);
            });
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


/// DataItem 数据源
pub enum DataItemSource {
    Memory {
        data: Arc<Vec<u8>>,
    },
    File {
        path: String,
    },
}

impl DataItemSource {
    pub fn new(data: proto::DataItem) -> Self {
        match &data.data_item_dispatch {
            Some(proto::data_item::DataItemDispatch::RawBytes(bytes)) => Self::Memory {
                data: Arc::new(bytes.clone()),
            },
            Some(proto::data_item::DataItemDispatch::File(file_data)) => Self::File {
                path: file_data.file_path.clone(),
            },
            _ => Self::Memory {
                data: Arc::new(Vec::new()),
            },
        }
    }
}

impl DataItemSource {
    async fn size(&self) -> WSResult<usize> {
        match self {
            Self::Memory { data } => Ok(data.len()),
            Self::File { path } => {
                let metadata = tokio::fs::metadata(path).await?;
                Ok(metadata.len() as usize)
            }
        }
    }
    
    async fn read_chunk(&self, offset: usize, size: usize) -> WSResult<Vec<u8>> {
        match self {
            Self::Memory { data } => {
                let end = (offset + size).min(data.len());
                Ok(data[offset..end].to_vec())
            }
            Self::File { path } => {
                let mut file = tokio::fs::File::open(path).await?;
                file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
                let mut buffer = vec![0u8; size];
                let n = file.read(&mut buffer).await?;
                buffer.truncate(n);
                Ok(buffer)
            }
        }
    }
    
    fn block_type(&self) -> proto::BatchDataBlockType {
        match self {
            Self::Memory { .. } => proto::BatchDataBlockType::Memory,
            Self::File { .. } => proto::BatchDataBlockType::File,
        }
    }
}

/// 数据集元信息
#[derive(Serialize, Deserialize)]
pub struct DataSetMetaV1 {
    // unique_id: Vec<u8>,
    pub version: u64,
    pub data_metas: Vec<DataMetaSys>,
    pub synced_nodes: HashSet<NodeID>,
}

pub type CacheMode = u16;

/// 数据集元信息
///
/// 注意：新建元信息请使用 `DataSetMetaBuilder`
///
/// https://fvd360f8oos.feishu.cn/docx/XoFudWhAgox84MxKC3ccP1TcnUh#share-Tqqkdxubpokwi5xREincb1sFnLc
#[derive(Serialize, Deserialize, Debug,Clone)]
pub struct DataSetMetaV2 {
    // unique_id: Vec<u8>,
    api_version: u8,
    pub version: u64,
    pub cache_mode: Vec<CacheMode>,
    /// 每个数据项的分片信息，索引为数据项索引
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

/// 数据项的分片信息
/// 我们需要知道每个数据项的分片大小
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
    fn get_data_meta(
        &self,
        unique_id: &[u8],
        delete: bool,
    ) -> WSResult<Option<(KvVersion, DataSetMetaV2)>> {
        let ope_name = if delete { "delete" } else { "get" };
        tracing::debug!("{} data meta for uid({:?})", ope_name, unique_id);

        let kv_store_engine = self.kv_store_engine();
        let key = KeyTypeDataSetMeta(&unique_id);
        let keybytes = key.make_key();

        let write_lock = kv_store_engine.with_rwlock(&keybytes);
        let _guard = write_lock.write();

        let meta_opt = if delete {
            kv_store_engine.del(key, true)?
        } else {
            kv_store_engine.get(&key, true, KvAdditionalConf {})
        };
        Ok(meta_opt)
    }

    pub async fn get_metadata(
        &self,
        unique_id: &[u8],
        delete: bool,
    ) -> WSResult<DataSetMetaV2> {
        // 先尝试从本地获取
        if let Some((_version, meta)) = self.get_data_meta(unique_id, delete)? {
            return Ok(meta);
        }

        // 本地不存在，从 master 获取
        self.data_general().get_or_del_datameta_from_master(unique_id, delete).await
    }
}

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
            
            // 批量数据接收状态管理
            batch_receive_states: DashMap::new(),
        }
    }

    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start as master");

        let p2p = self.view.p2p();
        // register rpc callers
        {
            self.rpc_call_data_version_schedule.regist(p2p);
            self.rpc_call_write_once_data.regist(p2p);
            self.rpc_call_batch_data.regist(p2p);
            self.rpc_call_get_data_meta.regist(p2p);
            self.rpc_call_get_data.regist(p2p);
        }

        // register rpc handlers
        {
            let view = self.view.clone();
            self.rpc_handler_write_once_data
                .regist(p2p, move |responsor, req| {
                    let view = view.clone();
                    let _ = tokio::spawn(async move {
                        view.data_general().rpc_handle_write_one_data(responsor, req).await;
                    });
                    Ok(())
                });

            let view = self.view.clone();
            self.rpc_handler_batch_data.regist(
                p2p,
                move |responsor: RPCResponsor<proto::BatchDataRequest>,
                      req: proto::BatchDataRequest| {
                    let view = view.clone();
                    let _ = tokio::spawn(async move {
                        let _ = view.data_general().rpc_handle_batch_data(responsor, req).await;
                    });
                    Ok(())
                },
            );

            let view = self.view.clone();
            self.rpc_handler_data_meta_update.regist(
                p2p,
                move |responsor: RPCResponsor<proto::DataMetaUpdateRequest>,
                      req: proto::DataMetaUpdateRequest| {
                    let view = view.clone();
                    let _ = tokio::spawn(async move {
                        view.data_general().rpc_handle_data_meta_update(responsor, req).await
                    });
                    Ok(())
                },
            );

            let view = self.view.clone();
            self.rpc_handler_get_data_meta
                .regist(p2p, move |responsor, req| {
                    let view = view.clone();
                    let _ = tokio::spawn(async move {
                        view.data_general().rpc_handle_get_data_meta(req, responsor)
                            .await
                            .todo_handle();
                    });
                    Ok(())
                });

            let view = self.view.clone();
            self.rpc_handler_get_data.regist(
                p2p,
                move |responsor: RPCResponsor<proto::GetOneDataRequest>,
                      req: proto::GetOneDataRequest| {
                    let view = view.clone();
                    let _ = tokio::spawn(async move { 
                        view.data_general().rpc_handle_get_one_data(responsor, req).await 
                    });
                    Ok(())
                },
            );
        }

        Ok(vec![])
    }
}
