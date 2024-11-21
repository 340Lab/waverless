use super::{
    m_kv_store_engine::{
        KeyTypeDataSetItem, KeyTypeDataSetMeta, KvAdditionalConf, KvStoreEngine, KvVersion,
    },
    m_os::OperatingSystem,
    network::{
        m_p2p::{P2PModule, RPCCaller, RPCHandler, RPCResponsor},
        proto::{
            self, DataMeta, DataMetaGetRequest, DataVersionScheduleRequest,
            WriteOneDataRequest, WriteOneDataResponse,
        },
        proto_ext::ProtoExtDataItem,
    },
};
use crate::{
    general::m_kv_store_engine::KeyType,
    logical_module_view_impl,
    result::{WSError, WSResult, WsRuntimeErr, WsSerialErr},
    sys::{LogicalModule, LogicalModuleNewArgs, NodeID},
    util::JoinHandleWrapper,
};
use crate::{result::WsDataError, sys::LogicalModulesRef};
use async_trait::async_trait;
use camelpaste::paste;
use core::str;

use prost::Message;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinHandle;
use ws_derive::LogicalModule;

// use super::m_appmeta_manager::AppMeta;

logical_module_view_impl!(DataGeneralView);
logical_module_view_impl!(DataGeneralView, p2p, P2PModule);
logical_module_view_impl!(DataGeneralView, data_general, DataGeneral);
logical_module_view_impl!(DataGeneralView, kv_store_engine, KvStoreEngine);
logical_module_view_impl!(DataGeneralView, os, OperatingSystem);

pub type DataVersion = u64;

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

    pub rpc_call_data_version_schedule: RPCCaller<DataVersionScheduleRequest>,
    rpc_call_write_once_data: RPCCaller<WriteOneDataRequest>,
    rpc_call_get_data_meta: RPCCaller<DataMetaGetRequest>,
    rpc_call_get_data: RPCCaller<proto::GetOneDataRequest>,

    rpc_handler_write_once_data: RPCHandler<WriteOneDataRequest>,
    rpc_handler_data_meta_update: RPCHandler<proto::DataMetaUpdateRequest>,
    rpc_handler_get_data_meta: RPCHandler<DataMetaGetRequest>,
    rpc_handler_get_data: RPCHandler<proto::GetOneDataRequest>,
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
            rpc_call_get_data_meta: RPCCaller::new(),
            rpc_call_get_data: RPCCaller::new(),

            rpc_handler_write_once_data: RPCHandler::new(),
            rpc_handler_data_meta_update: RPCHandler::new(),
            rpc_handler_get_data_meta: RPCHandler::new(),
            rpc_handler_get_data: RPCHandler::new(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start as master");

        let p2p = self.view.p2p();
        // register rpc callers
        {
            self.rpc_call_data_version_schedule.regist(p2p);
            self.rpc_call_write_once_data.regist(p2p);
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
                        view.rpc_handle_write_one_data(responsor, req).await;
                    });
                    Ok(())
                });
            let view = self.view.clone();
            self.rpc_handler_data_meta_update.regist(
                p2p,
                move |responsor: RPCResponsor<proto::DataMetaUpdateRequest>,
                      req: proto::DataMetaUpdateRequest| {
                    let view = view.clone();
                    let _ =
                        tokio::spawn(
                            async move { view.rpc_handle_data_meta_update(responsor, req) },
                        );
                    Ok(())
                },
            );
            let view = self.view.clone();
            self.rpc_handler_get_data_meta
                .regist(p2p, move |responsor, req| {
                    let view = view.clone();
                    let _ = tokio::spawn(async move {
                        view.rpc_handle_get_data_meta(req, responsor).await;
                    });
                    Ok(())
                });
            let view = self.view.clone();
            self.rpc_handler_get_data.regist(
                p2p,
                move |responsor: RPCResponsor<proto::GetOneDataRequest>,
                      req: proto::GetOneDataRequest| {
                    let view = view.clone();
                    let _ =
                        tokio::spawn(async move { view.rpc_handle_get_one_data(responsor, req) });
                    Ok(())
                },
            );
        }

        Ok(vec![])
    }
}

impl DataGeneralView {
    async fn rpc_handle_data_meta_update(
        self,
        responsor: RPCResponsor<proto::DataMetaUpdateRequest>,
        mut req: proto::DataMetaUpdateRequest,
    ) {
        let key = KeyTypeDataSetMeta(&req.unique_id);
        let keybytes = key.make_key();

        let write_lock = self.kv_store_engine().with_rwlock(&keybytes);
        write_lock.write();

        if let Some((_old_version, mut old_meta)) =
            self.kv_store_engine().get(&key, true, KvAdditionalConf {})
        {
            if old_meta.version > req.version {
                responsor.send_resp(proto::DataMetaUpdateResponse {
                    version: old_meta.version,
                    message: "New data version overwrite".to_owned(),
                });
                return;
            }
            old_meta.version = req.version;
            if req.serialized_meta.len() > 0 {
                self.kv_store_engine().set_raw(
                    &keybytes,
                    std::mem::take(&mut req.serialized_meta),
                    true,
                );
            } else {
                self.kv_store_engine().set(key, &old_meta, true);
            }
        } else {
            if req.serialized_meta.len() > 0 {
                self.kv_store_engine().set_raw(
                    &keybytes,
                    std::mem::take(&mut req.serialized_meta),
                    true,
                );
            } else {
                responsor.send_resp(proto::DataMetaUpdateResponse {
                    version: 0,
                    message: "Old meta data not found and missing new meta".to_owned(),
                });
                return;
            }
        }
        responsor.send_resp(proto::DataMetaUpdateResponse {
            version: req.version,
            message: "Update success".to_owned(),
        });
    }

    async fn rpc_handle_get_one_data(
        self,
        responsor: RPCResponsor<proto::GetOneDataRequest>,
        req: proto::GetOneDataRequest,
    ) -> WSResult<()> {
        // req.unique_id
        let kv_store_engine = self.kv_store_engine();
        let _ = self.get_data_meta(&req.unique_id, true)?;
        // let meta = bincode::deserialize::<DataSetMetaV2>(&req.serialized_meta).map_err(|err| {
        //     WsSerialErr::BincodeErr {
        //         err,
        //         context: "rpc_handle_get_one_data".to_owned(),
        //     }
        // })?;
        let mut deleted = vec![];

        let mut kv_ope_err = vec![];

        for idx in req.idxs {
            let value = if req.delete {
                match kv_store_engine.del(
                    KeyTypeDataSetItem {
                        uid: req.unique_id.as_ref(), //req.unique_id.clone(),
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
                        uid: req.unique_id.as_ref(), //req.unique_id.clone(),
                        idx: idx as u8,
                    },
                    false,
                    KvAdditionalConf {},
                )
            };
            deleted.push(value);
        }

        tracing::warn!("temporaly no data response");

        let (success, message): (bool, String) = if kv_ope_err.len() > 0 {
            (false, {
                let mut msg = String::from("KvEngine operation failed: ");
                for e in kv_ope_err.iter() {
                    msg.push_str(&format!("{:?}", e));
                }
                msg
            })
        } else if deleted.iter().all(|v| v.is_some()) {
            (true, "success".to_owned())
        } else {
            (false, "some data not found".to_owned())
        };

        responsor
            .send_resp(proto::GetOneDataResponse {
                success,
                data: vec![],
                message,
            })
            .await?;

        Ok(())
    }
    async fn rpc_handle_write_one_data(
        self,
        responsor: RPCResponsor<WriteOneDataRequest>,
        req: WriteOneDataRequest,
    ) {
        tracing::debug!("verify data meta bf write data");
        let kv_store_engine = self.kv_store_engine();

        // Step 0: pre-check
        {
            if req.data.is_empty() {
                responsor.send_resp(WriteOneDataResponse {
                    remote_version: 0,
                    success: false,
                    message: "Request data is empty".to_owned(),
                });
                return;
            }
            if req.data[0].data_item_dispatch.is_none() {
                responsor.send_resp(WriteOneDataResponse {
                    remote_version: 0,
                    success: false,
                    message: "Request data enum is none".to_owned(),
                });
                return;
            }
        }

        // Step1: verify version
        // take old meta
        {
            let keybytes = KeyTypeDataSetMeta(&req.unique_id).make_key();
            let fail_by_overwrite = || {
                let message = "New data version overwrite".to_owned();
                tracing::warn!("{}", message);
                responsor.send_resp(WriteOneDataResponse {
                    remote_version: 0,
                    success: false,
                    message,
                });
            };
            let fail_with_msg = |message: String| {
                tracing::warn!("{}", message);
                responsor.send_resp(WriteOneDataResponse {
                    remote_version: 0,
                    success: false,
                    message,
                });
            };
            loop {
                let res = kv_store_engine.get(
                    &KeyTypeDataSetMeta(&req.unique_id),
                    false,
                    KvAdditionalConf {},
                ); //tofix, master send maybe not synced
                let old_dataset_version = if res.is_none() {
                    0
                } else {
                    res.as_ref().unwrap().1.version
                };
                // need to wait for new version
                if res.is_none() || res.as_ref().unwrap().1.version < req.version {
                    let (_, new_value) = kv_store_engine.wait_for_new(&keybytes).await;
                    let Some(new_value) = new_value.as_data_set_meta() else {
                        fail_with_msg(format!(
                            "fatal error, kv value supposed to be DataSetMeta, rathe than {:?}",
                            new_value
                        ));
                        return;
                    };

                    if new_value.version > req.version {
                        fail_by_overwrite();
                        return;
                    } else if new_value.version < req.version {
                        // still need to wait for new version
                        continue;
                    } else {
                        break;
                    }
                } else if old_dataset_version > req.version {
                    fail_by_overwrite();
                    return;
                }
            }
        }

        // Step3: write data
        tracing::debug!("start to write data");
        for (idx, data) in req.data.into_iter().enumerate() {
            match data.data_item_dispatch.unwrap() {
                proto::data_item::DataItemDispatch::File(f) => {
                    // just store in kv
                    kv_store_engine.set(
                        KeyTypeDataSetItem {
                            uid: req.unique_id.as_ref(), //req.unique_id.clone(),
                            idx: idx as u8,
                        },
                        &f.encode_to_vec(),
                        false,
                    );
                }
                proto::data_item::DataItemDispatch::RawBytes(bytes) => {
                    tracing::debug!("writing data part{} bytes", idx);
                    kv_store_engine.set(
                        KeyTypeDataSetItem {
                            uid: &req.unique_id,
                            idx: idx as u8,
                        },
                        &bytes,
                        false,
                    );
                }
            }
        }
        kv_store_engine.flush();
        tracing::debug!("data is written");
        responsor
            .send_resp(WriteOneDataResponse {
                remote_version: req.version,
                success: true,
                message: "".to_owned(),
            })
            .await;
        // ## response
    }

    async fn rpc_handle_get_data_meta(
        self,
        req: proto::DataMetaGetRequest,
        responsor: RPCResponsor<proto::DataMetaGetRequest>,
    ) -> WSResult<()> {
        let meta = self.get_data_meta(&req.unique_id, req.delete)?;

        let serialized_meta = meta.map_or(vec![], |(_kvversion, meta)| {
            bincode::serialize(&meta).unwrap()
        });

        responsor
            .send_resp(proto::DataMetaGetResponse { serialized_meta })
            .await?;

        Ok(())
    }
    // pub async fn

    fn get_data_meta(
        &self,
        unique_id: &[u8],
        delete: bool,
    ) -> WSResult<Option<(KvVersion, DataSetMetaV2)>> {
        let kv_store_engine = self.kv_store_engine();
        let key = KeyTypeDataSetMeta(&unique_id);
        let keybytes = key.make_key();

        let write_lock = kv_store_engine.with_rwlock(&keybytes);
        let _guard = write_lock.write();

        let meta_opt = if delete {
            kv_store_engine.get(&key, true, KvAdditionalConf {})
        } else {
            kv_store_engine.del(key, true)?
        };
        Ok(meta_opt)
    }
}

// pub enum DataWrapper {
//     Bytes(Vec<u8>),
//     File(PathBuf),
// }

impl DataGeneral {
    async fn get_datameta_from_master(&self, unique_id: &[u8]) -> WSResult<DataSetMetaV2> {
        let p2p = self.view.p2p();
        let data_general = self.view.data_general();
        // get meta from master
        let meta = data_general
            .rpc_call_get_data_meta
            .call(
                p2p,
                p2p.nodes_config.get_master_node(),
                DataMetaGetRequest {
                    unique_id: unique_id.to_owned(),
                    delete: true,
                },
                Some(Duration::from_secs(30)),
            )
            .await?;
        bincode::deserialize::<DataSetMetaV2>(&meta.serialized_meta).map_err(|e| {
            WSError::from(WsSerialErr::BincodeErr {
                err: e,
                context: "delete data meta at master wrong meta serialized".to_owned(),
            })
        })
    }

    async fn get_data_by_meta(
        &self,
        unique_id: &[u8],
        meta: DataSetMetaV2,
        delete: bool,
    ) -> WSResult<(DataSetMetaV2, HashMap<(NodeID, usize), proto::DataItem>)> {
        let view = &self.view;
        // Step2: delete data on each node
        let mut each_node_data: HashMap<NodeID, proto::GetOneDataRequest> = HashMap::new();
        for (idx, data_splits) in meta.datas_splits.iter().enumerate() {
            for split in &data_splits.splits {
                let _ = each_node_data
                    .entry(split.node_id)
                    .and_modify(|old| {
                        old.idxs.push(idx as u32);
                    })
                    .or_insert(proto::GetOneDataRequest {
                        unique_id: unique_id.to_owned(),
                        idxs: vec![idx as u32],
                        delete,
                        return_data: true,
                    });
            }
        }

        let mut tasks = vec![];
        for (node_id, req) in each_node_data {
            let view = view.clone();
            let task = tokio::spawn(async move {
                let req_idxs = req.idxs.clone();
                let res = view
                    .data_general()
                    .rpc_call_get_data
                    .call(view.p2p(), node_id, req, Some(Duration::from_secs(30)))
                    .await;
                let res: WSResult<Vec<(u32, proto::DataItem)>> = res.map(|response| {
                    if !response.success {
                        tracing::warn!("get/delete data failed {}", response.message);
                        vec![]
                    } else {
                        req_idxs.into_iter().zip(response.data).collect()
                    }
                });
                (node_id, res)
            });
            tasks.push(task);
        }

        let mut node_2_datas: HashMap<(NodeID, usize), proto::DataItem> = HashMap::new();
        for tasks in tasks {
            let (node_id, data) = tasks.await.map_err(|err| {
                WSError::from(WsRuntimeErr::TokioJoin {
                    err,
                    context: "delete_data - deleting remote data".to_owned(),
                })
            })?;
            for (idx, data_item) in data? {
                let _ = node_2_datas.insert((node_id, idx as usize), data_item);
            }
        }

        Ok((meta, node_2_datas))
    }

    pub async fn get_data(
        &self,
        unique_id: impl Into<Vec<u8>>,
    ) -> WSResult<(DataSetMetaV2, HashMap<(NodeID, usize), proto::DataItem>)> {
        let unique_id: Vec<u8> = unique_id.into();
        // Step1: get meta
        let meta: DataSetMetaV2 = self.get_datameta_from_master(&unique_id).await?;
        self.get_data_by_meta(&unique_id, meta, false).await
    }

    /// return (meta, data_map)
    /// data_map: (node_id, idx) -> data_items
    pub async fn delete_data(
        &self,
        unique_id: impl Into<Vec<u8>>,
    ) -> WSResult<(DataSetMetaV2, HashMap<(NodeID, usize), proto::DataItem>)> {
        let unique_id: Vec<u8> = unique_id.into();

        // Step1: get meta
        let meta: DataSetMetaV2 = self.get_datameta_from_master(&unique_id).await?;

        self.get_data_by_meta(&unique_id, meta, true).await
        //
    }

    /// - check the uid from DATA_UID_PREFIX_XXX
    pub async fn get_data_item(&self, unique_id: &[u8], idx: u8) -> Option<proto::DataItem> {
        let Some((_, itembytes)) = self.view.kv_store_engine().get(
            &KeyTypeDataSetItem {
                uid: unique_id,
                idx: idx as u8,
            },
            false,
            KvAdditionalConf {},
        ) else {
            return None;
        };
        Some(proto::DataItem {
            data_item_dispatch: Some(proto::data_item::DataItemDispatch::RawBytes(itembytes)),
        })
    }

    ///  The user's data write entry
    ///
    ///  - check the design here
    ///
    ///  - check the uid from DATA_UID_PREFIX_XXX
    ///
    ///  - https://fvd360f8oos.feishu.cn/docx/XoFudWhAgox84MxKC3ccP1TcnUh#share-Rtxod8uDqoIcRwxOM1rccuXxnQg
    pub async fn write_data(
        &self,
        unique_id: impl Into<Vec<u8>>,
        // data_metas: Vec<DataMeta>,
        datas: Vec<proto::DataItem>,
        context_openode_opetype_operole: Option<(
            NodeID,
            proto::DataOpeType,
            proto::data_schedule_context::OpeRole,
        )>,
    ) -> WSResult<()> {
        let p2p = self.view.p2p();
        let unique_id: Vec<u8> = unique_id.into();
        let log_tag = Arc::new(format!(
            "write_data,uid:{:?},operole:{:?}",
            str::from_utf8(&unique_id),
            context_openode_opetype_operole.as_ref().map(|v| &v.2)
        ));

        // Step 1: need the master to do the decision
        // - require for the latest version for write permission
        // - require for the distribution and cache mode
        let version_schedule_resp = {
            let resp = self
                .rpc_call_data_version_schedule
                .call(
                    self.view.p2p(),
                    p2p.nodes_config.get_master_node(),
                    DataVersionScheduleRequest {
                        unique_id: unique_id.clone(),
                        version: 0,
                        context: context_openode_opetype_operole.map(
                            |(ope_node, ope_type, ope_role)| proto::DataScheduleContext {
                                ope_node: ope_node as i64,
                                ope_type: ope_type as i32,
                                each_data_sz_bytes: datas
                                    .iter()
                                    .map(|data_item| data_item.data_sz_bytes() as u32)
                                    .collect::<Vec<_>>(),
                                ope_role: Some(ope_role),
                            },
                        ),
                    },
                    Some(Duration::from_secs(60)),
                )
                .await;

            let resp = match resp {
                Err(inner_e) => {
                    let e = WsDataError::WriteDataRequireVersionErr {
                        unique_id,
                        err: Box::new(inner_e),
                    };
                    tracing::warn!("{:?}", e);
                    return Err(e.into());

                    // tracing::warn!("write_data require version error: {:?}", e);
                    // return e;
                }
                Ok(ok) => ok,
            };
            resp
        };

        // Step2: dispatch the data source and caches
        {
            // resp.split is decision for each data, so the length should be verified
            if version_schedule_resp.split.len() != datas.len() {
                let e = WsDataError::WriteDataSplitLenNotMatch {
                    unique_id,
                    expect: datas.len(),
                    actual: version_schedule_resp.split.len(),
                };
                tracing::warn!("{:?}", e);
                return Err(e.into());
            }

            let mut write_source_data_tasks = vec![];

            // write the data split to kv
            for (one_data_splits, one_data_item) in
                version_schedule_resp.split.into_iter().zip(datas)
            {
                // let mut last_node_begin: Option<(NodeID, usize)> = None;
                fn flush_the_data(
                    log_tag: &str,
                    unique_id: &[u8],
                    version: u64,
                    split_size: usize,
                    view: &DataGeneralView,
                    one_data_item: &proto::DataItem,
                    nodeid: NodeID,
                    offset: usize,
                    write_source_data_tasks: &mut Vec<JoinHandle<WSResult<WriteOneDataResponse>>>,
                ) {
                    let log_tag = log_tag.to_owned();
                    let unique_id = unique_id.to_owned();
                    let view = view.clone();
                    // let version = version_schedule_resp.version;
                    // let split_size = one_data_splits.split_size as usize;
                    let one_data_item_split =
                        one_data_item.clone_split_range(offset..offset + split_size);
                    let t = tokio::spawn(async move {
                        tracing::debug!("write_data flushing {}", log_tag);
                        view.data_general()
                            .rpc_call_write_once_data
                            .call(
                                view.p2p(),
                                nodeid,
                                WriteOneDataRequest {
                                    unique_id,
                                    version,
                                    data: vec![one_data_item_split],
                                },
                                Some(Duration::from_secs(60)),
                            )
                            .await
                    });
                    write_source_data_tasks.push(t);
                }
                for split in one_data_splits.splits.iter() {
                    flush_the_data(
                        &log_tag,
                        &unique_id,
                        version_schedule_resp.version,
                        split.data_size as usize,
                        &self.view,
                        &one_data_item,
                        split.node_id,
                        split.data_offset as usize,
                        &mut write_source_data_tasks,
                    );
                }
            }

            // count and hanlde failed
            let mut failed = false;
            for t in write_source_data_tasks {
                let res = t.await;
                match res {
                    Ok(res) => match res {
                        Ok(_) => {}
                        Err(e) => {
                            failed = true;
                            tracing::warn!("write source data failed: {}", e);
                        }
                    },

                    Err(e) => {
                        failed = true;
                        tracing::warn!("write_source_data_tasks failed: {}", e);
                    }
                }
            }
            if failed {
                tracing::warn!("TODO: need to rollback");
            }
            // let res = join_all(write_source_data_tasks).await;
            // // check if there's error
            // if let Some(err)=res.iter().filter(|res|{res.is_err()}).next(){
            //     tracing::warn!("failed to write data {}")
            //     panic!("failed to write data");
            // }
        }

        Ok(())
        // if DataModeDistribute::BroadcastRough as i32 == data_metas[0].distribute {
        //     self.write_data_broadcast_rough(unique_id, data_metas, datas)
        //         .await;
        // }
    }

    // async fn write_data_broadcast_rough(
    //     &self,
    //     unique_id: String,
    //     data_metas: Vec<DataMeta>,
    //     datas: Vec<DataItem>,
    // ) {
    //     let p2p = self.view.p2p();

    //     tracing::debug!("start broadcast data with version");
    //     let version = resp.version;
    //     // use the got version to send to global paralell
    //     let mut tasks = vec![];

    //     for (_idx, node) in p2p.nodes_config.all_nodes_iter().enumerate() {
    //         let n = *node.0;
    //         let view = self.view.clone();
    //         let datas = datas.clone();
    //         let unique_id = unique_id.clone();
    //         // let datas = unsafe { util::SendNonNull(util::non_null(&datas)) };

    //         let t = tokio::spawn(async move {
    //             view.data_general()
    //                 .rpc_call_write_once_data
    //                 .call(
    //                     view.p2p(),
    //                     n,
    //                     WriteOneDataRequest {
    //                         unique_id,
    //                         version,
    //                         data: datas,
    //                     },
    //                     Some(Duration::from_secs(60)),
    //                 )
    //                 .await
    //         });

    //         tasks.push(t);
    //     }
    //     for t in tasks {
    //         let res = t.await.unwrap();
    //         match res {
    //             Err(e) => {
    //                 tracing::warn!("write_data_broadcast_rough broadcast error: {:?}", e);
    //             }
    //             Ok(ok) => {
    //                 if !ok.success {
    //                     tracing::warn!(
    //                         "write_data_broadcast_rough broadcast error: {:?}",
    //                         ok.message
    //                     );
    //                 }
    //             }
    //         }
    //     }
    // }
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
    pub cache_mode: u16,
    pub datas_splits: Vec<DataSplit>,
}

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
}

/// we need to know the split size for one data
#[derive(Serialize, Deserialize, Debug)]
pub struct DataSplit {
    pub splits: Vec<EachNodeSplit>,
}

impl DataSplit {
    /// node_2_datas will be consumed partially
    pub fn recorver_data(
        &self,
        unique_id: &[u8],
        idx: usize,
        node_2_datas: &mut HashMap<(NodeID, usize), proto::DataItem>,
    ) -> WSResult<Vec<u8>> {
        let nodes = node_2_datas
            .iter()
            .filter(|v| v.0 .1 == idx)
            .map(|v| v.0 .0)
            .collect::<Vec<_>>();

        let mut each_node_splits: HashMap<NodeID, (proto::DataItem, Option<EachNodeSplit>)> =
            HashMap::new();

        for node in nodes {
            let data = node_2_datas.remove(&(node, idx)).unwrap();
            let _ = each_node_splits.insert(node, (data, None));
        }

        let mut max_size = 0;
        let mut missing = vec![];

        // zip with split info
        //  by the way, check if the split is missing
        for split in &self.splits {
            let Some(find) = each_node_splits.get_mut(&split.node_id) else {
                missing.push((*split).clone());
                continue;
            };
            find.1 = Some(split.clone());
            if split.data_offset + split.data_size > max_size {
                max_size = split.data_offset + split.data_size;
            }
        }

        if missing.len() > 0 {
            return Err(WsDataError::SplitRecoverMissing {
                unique_id: unique_id.to_owned(),
                idx,
                missing,
            }
            .into());
        }

        let mut recover = vec![0; max_size.try_into().unwrap()];

        for (_node, (data, splitmeta)) in each_node_splits {
            let splitmeta = splitmeta.unwrap();
            let begin = splitmeta.data_offset as usize;
            let end = begin + splitmeta.data_size as usize;
            recover[begin..end].copy_from_slice(data.as_ref());
        }

        Ok(recover)
    }
}

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
                        self.0 & [<CACHE_MODE_ $group:upper _MASK>]
                            == self.0 & [<CACHE_MODE_ $group:upper _MASK>] & [<CACHE_MODE_ $group:upper _ $mode:upper _MASK>]
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
                cache_mode: 0,
                api_version: 2,
                datas_splits: vec![],
            }),
        }
    }
    pub fn cache_mode_time_forever(&mut self) -> &mut Self {
        self.building.as_mut().unwrap().cache_mode &= CACHE_MODE_TIME_FOREVER_MASK;
        self
    }

    pub fn cache_mode_time_auto(&mut self) -> &mut Self {
        self.building.as_mut().unwrap().cache_mode &= CACHE_MODE_TIME_AUTO_MASK;
        self
    }

    pub fn cache_mode_pos_allnode(&mut self) -> &mut Self {
        self.building.as_mut().unwrap().cache_mode &= CACHE_MODE_POS_ALLNODE_MASK;
        self
    }

    pub fn cache_mode_pos_specnode(&mut self) -> &mut Self {
        self.building.as_mut().unwrap().cache_mode &= CACHE_MODE_POS_SPECNODE_MASK;
        self
    }

    pub fn cache_mode_pos_auto(&mut self) -> &mut Self {
        self.building.as_mut().unwrap().cache_mode &= CACHE_MODE_POS_AUTO_MASK;
        self
    }

    pub fn cache_mode_map_common_kv(&mut self) -> &mut Self {
        self.building.as_mut().unwrap().cache_mode &= CACHE_MODE_MAP_COMMON_KV_MASK;
        self
    }

    pub fn cache_mode_map_file(&mut self) -> &mut Self {
        self.building.as_mut().unwrap().cache_mode &= CACHE_MODE_MAP_FILE_MASK;
        self
    }

    pub fn version(&mut self, version: u64) -> &mut Self {
        self.building.as_mut().unwrap().version = version;
        self
    }

    #[must_use]
    pub fn set_data_splits(&mut self, splits: Vec<DataSplit>) -> &mut Self {
        self.building.as_mut().unwrap().datas_splits = splits;
        self
    }

    pub fn build(&mut self) -> DataSetMetaV2 {
        self.building.take().unwrap()
    }
}

impl From<DataSetMetaV1> for DataSetMetaV2 {
    fn from(
        DataSetMetaV1 {
            version,
            data_metas: _,
            synced_nodes: _,
        }: DataSetMetaV1,
    ) -> Self {
        DataSetMetaBuilder::new()
            .version(version)
            .cache_mode_pos_allnode()
            .build()
        // DataSetMetaV2 {
        //     version,
        //     data_metas,
        //     synced_nodes,
        // }
    }
}

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
