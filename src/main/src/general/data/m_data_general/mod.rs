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
    result::{WSError, WSResult, WSResultExt, WsRuntimeErr, WsSerialErr},
    sys::{LogicalModule, LogicalModuleNewArgs, NodeID},
    util::JoinHandleWrapper,
};
use crate::{result::WsDataError, sys::LogicalModulesRef};
use async_trait::async_trait;
use camelpaste::paste;
use core::str;
use enum_as_inner::EnumAsInner;

use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
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

    // // unique_id,idx -> file_path
    // auto_cache: moka::sync::Cache<(String, u8), (DataVersion, proto::DataItem)>,

    // // unique_id,idx -> serialized value
    // forever_cache: dashmap::DashMap<(String, u8), (DataVersion, proto::DataItem)>,
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

            // auto_cache: moka::sync::Cache::new(100),
            // forever_cache: dashmap::DashMap::new(),
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
                    let _ = tokio::spawn(async move {
                        view.rpc_handle_data_meta_update(responsor, req).await
                    });
                    Ok(())
                },
            );
            let view = self.view.clone();
            self.rpc_handler_get_data_meta
                .regist(p2p, move |responsor, req| {
                    let view = view.clone();
                    let _ = tokio::spawn(async move {
                        view.rpc_handle_get_data_meta(req, responsor)
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
                    let _ =
                        tokio::spawn(
                            async move { view.rpc_handle_get_one_data(responsor, req).await },
                        );
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
        struct Defer {
            node: NodeID,
        }
        impl Drop for Defer {
            fn drop(&mut self) {
                tracing::debug!("rpc_handle_data_meta_update return at node({})", self.node);
            }
        }
        let _defer = Defer {
            node: self.p2p().nodes_config.this_node(),
        };

        let key = KeyTypeDataSetMeta(&req.unique_id);
        let keybytes = key.make_key();

        tracing::debug!("rpc_handle_data_meta_update {:?}", req);
        let kv_lock = self.kv_store_engine().with_rwlock(&keybytes);
        let _kv_write_lock_guard = kv_lock.write();

        if let Some((_old_version, mut old_meta)) =
            self.kv_store_engine().get(&key, true, KvAdditionalConf {})
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
                self.kv_store_engine()
                    .set_raw(&keybytes, std::mem::take(&mut req.serialized_meta), true)
                    .todo_handle();
            } else {
                self.kv_store_engine()
                    .set(key, &old_meta, true)
                    .todo_handle();
            }
        } else {
            if req.serialized_meta.len() > 0 {
                tracing::debug!(
                    "set new meta data, {:?}",
                    bincode::deserialize::<DataSetMeta>(&req.serialized_meta)
                );
                self.kv_store_engine()
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

    async fn rpc_handle_get_one_data(
        self,
        responsor: RPCResponsor<proto::GetOneDataRequest>,
        req: proto::GetOneDataRequest,
    ) -> WSResult<()> {
        tracing::debug!("starting rpc_handle_get_one_data {:?}", req);

        // req.unique_id
        let kv_store_engine = self.kv_store_engine();
        let _ = self
            .get_data_meta(&req.unique_id, req.delete)
            .map_err(|err| {
                tracing::warn!("rpc_handle_get_one_data get_data_meta failed: {:?}", err);
                err
            })?;
        // let meta = bincode::deserialize::<DataSetMetaV2>(&req.serialized_meta).map_err(|err| {
        //     WsSerialErr::BincodeErr {
        //         err,
        //         context: "rpc_handle_get_one_data".to_owned(),
        //     }
        // })?;
        let mut got_or_deleted = vec![];

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
            got_or_deleted.push(value);
        }

        // tracing::warn!("temporaly no data response");

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
                // if let Ok(v) = decode_res {
                got_or_deleted_checked.push(decode_res);
                // } else {
                //     success = false;
                //     got_or_deleted_checked = vec![];
                //     message = format!("decode data item failed {:?}", decode_res.unwrap_err());
                //     tracing::warn!("{}", message);
                //     break;
                // }
            }
        }

        //  = got_or_deleted
        //     .into_iter()
        //     .map(|one| proto::FileData::decode(bytes::Bytes::from(one.unwrap().1)))
        //     .all(|one|one.is_ok())
        //     .collect::<Vec<_>>();
        responsor
            .send_resp(proto::GetOneDataResponse {
                success,
                data: got_or_deleted_checked,
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
                            self.p2p().nodes_config.this_node(),
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
        // let old_dataset_version = if res.is_none() {
        //     0
        // } else {
        //     res.as_ref().unwrap().1.version
        // };

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
                    uid: req.unique_id.as_ref(), //req.unique_id.clone(),
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
        // ## response
    }

    async fn rpc_handle_get_data_meta(
        self,
        req: proto::DataMetaGetRequest,
        responsor: RPCResponsor<proto::DataMetaGetRequest>,
    ) -> WSResult<()> {
        tracing::debug!("rpc_handle_get_data_meta with req({:?})", req);
        let meta = self.get_data_meta(&req.unique_id, req.delete)?;
        if meta.is_none() {
            tracing::debug!("rpc_handle_get_data_meta data meta not found");
        } else {
            tracing::debug!("rpc_handle_get_data_meta data meta found");
        }
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
}

// pub enum DataWrapper {
//     Bytes(Vec<u8>),
//     File(PathBuf),
// }

pub enum DataUidMeta {
    Meta {
        unique_id: Vec<u8>,
        meta: DataSetMetaV2,
    },
    UniqueId(Vec<u8>),
}

#[derive(EnumAsInner, Clone)]
pub enum GetOrDelDataArgType {
    Delete,
    All,
    PartialOne {
        // partial can't be deleted
        idx: DataItemIdx,
    },
    PartialMany {
        idxs: BTreeSet<DataItemIdx>,
    },
}

pub struct GetOrDelDataArg {
    pub meta: Option<DataSetMetaV2>,
    pub unique_id: Vec<u8>,
    pub ty: GetOrDelDataArgType,
}

impl DataGeneral {
    pub async fn get_or_del_datameta_from_master(
        &self,
        unique_id: &[u8],
        delete: bool,
    ) -> WSResult<DataSetMetaV2> {
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
                    delete,
                },
                Some(Duration::from_secs(30)),
            )
            .await?;
        if meta.serialized_meta.is_empty() {
            return Err(WsDataError::DataSetNotFound {
                uniqueid: unique_id.to_owned(),
            }
            .into());
        }
        bincode::deserialize::<DataSetMetaV2>(&meta.serialized_meta).map_err(|e| {
            WSError::from(WsSerialErr::BincodeErr {
                err: e,
                context: format!(
                    "get_datameta_from_master failed, meta:{:?}",
                    meta.serialized_meta
                ),
            })
        })
    }

    // should return real dataitem, rather than split dataitem
    pub async fn get_or_del_data(
        &self,
        GetOrDelDataArg {
            meta,
            unique_id,
            ty,
        }: GetOrDelDataArg,
    ) -> WSResult<(DataSetMetaV2, HashMap<DataItemIdx, proto::DataItem>)> {
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
            // not proper desig, skip
            //   https://fvd360f8oos.feishu.cn/wiki/DYAHw4oPLiZ5NYkTG56cFtJdnKg#share-Div9dUq11oGFOBxJO9ic3RtnnSf
            //   fn check_cache_pos(meta: &DataSetMetaV2) -> WSResult<()> {
            //       if !meta.cache_mode_visitor().is_pos_allnode()
            //           && !meta.cache_mode_visitor().is_pos_auto()
            //           && !meta.cache_mode_visitor().is_pos_specnode()
            //       {
            //           return Err(WsDataError::UnknownCachePosMode {
            //               mode: meta.cache_mode_visitor().0,
            //           }
            //           .into());
            //       }
            //       if meta.cache_mode_visitor().is_pos_specnode() {
            //           // check this node is in the spec node list
            //           panic!("TODO: check this node is in the spec node list");
            //       }
            //       Ok(())
            //   }
            let check_cache_time = |meta: &DataSetMetaV2| -> WSResult<()> {
                if !meta.cache_mode_visitor(idx).is_time_auto()
                    && !meta.cache_mode_visitor(idx).is_time_forever()
                {
                    return Err(WsDataError::UnknownCacheTimeMode {
                        mode: meta.cache_mode_visitor(idx).0,
                    }
                    .into());
                }
                Ok(())
            };
            check_cache_map(&meta)?;
            // not proper desig, skip
            //   check_cache_pos(&meta)?;
            check_cache_time(&meta)?;
        }

        // verify idx range & get whether to delete
        let delete = match &ty {
            GetOrDelDataArgType::Delete => true,
            GetOrDelDataArgType::All => false,
            GetOrDelDataArgType::PartialOne { idx } => {
                if *idx as usize >= meta.data_item_cnt() {
                    return Err(WsDataError::ItemIdxOutOfRange {
                        wanted: *idx,
                        len: meta.data_item_cnt() as u8,
                    }
                    .into());
                }
                false
            }
            GetOrDelDataArgType::PartialMany { idxs } => {
                let Some(biggest_idx) = idxs.iter().rev().next() else {
                    return Err(WsDataError::ItemIdxEmpty.into());
                };
                if *biggest_idx >= meta.data_item_cnt() as u8 {
                    return Err(WsDataError::ItemIdxOutOfRange {
                        wanted: *biggest_idx,
                        len: meta.data_item_cnt() as u8,
                    }
                    .into());
                }
                false
            }
        };

        // // TODO 读取数据的时候先看看缓存有没有，如果没有再读数据源，如果有从缓存里面拿，需要校验 version
        // if !delete {
        //     let mut cached_items = HashMap::new();
        //     for idx in WantIdxIter::new(&ty) {
        //         let cache_list = if meta.cache_mode_visitor(idx).is_time_auto() {
        //             self.auto_cache.clone()
        //         } else if meta.cache_mode_visitor(idx).is_time_forever() {
        //             self.forever_cache.clone()
        //         } else {
        //             None
        //         };
        //         if cache_list.is_none() {
        //             continue;
        //         }
        //         // 从缓存中获取数据
        //         let cache_key = (unique_id.clone(), idx);
        //         let cached_value = cache_list.get(&cache_key);
        //         // 如果找到缓存且版本匹配
        //         if let Some((cached_version, cached_item)) = cached_value {
        //             if cached_version == meta.version {
        //                 cached_items.insert(idx, cached_item.clone());
        //                 tracing::debug!("Cache hit for idx: {}, version: {}", idx, cached_version);
        //             } else {
        //                 // 如果缓存版本不匹配，从缓存中删除掉
        //                 cache_list.remove(&cache_key);
        //                 tracing::debug!(
        //                     "Cache version mismatch for idx: {}, cached: {}, current: {}",
        //                     idx,
        //                     cached_version,
        //                     meta.version
        //                 );
        //             }
        //         }
        //     }
        //     // 如果所有请求的数据都在缓存中找到，直接返回
        //     if matches!(ty, GetOrDelDataArgType::All)
        //         && cached_items.len() == meta.datas_splits.len()
        //         || matches!(ty, GetOrDelDataArgType::PartialOne { .. }) && cached_items.len() == 1
        //         || matches!(ty, GetOrDelDataArgType::PartialMany { idxs })
        //             && cached_items.len() == idxs.len()
        //     {
        //         tracing::debug!("All requested data found in cache, returning early");
        //         return Ok((meta, cached_items));
        //     }
        // }

        // 如果缓存里没有，则需要从数据源读取
        let mut cache: Vec<bool> = Vec::new();
        for _ in 0..meta.data_item_cnt() {
            match &ty {
                GetOrDelDataArgType::Delete => {
                    cache.push(false);
                }
                GetOrDelDataArgType::All
                | GetOrDelDataArgType::PartialOne { .. }
                | GetOrDelDataArgType::PartialMany { .. } => {
                    cache.push(true);
                }
            }
        }

        // Step2: get/delete data on each node
        // nodeid -> (getdata_req, splitidx)
        let mut each_node_getdata: HashMap<NodeID, (proto::GetOneDataRequest, Vec<usize>)> =
            HashMap::new();
        let mut each_item_idx_receive_worker_tx_rx_splits: HashMap<
            u8,
            (
                tokio::sync::mpsc::Sender<WSResult<(DataSplitIdx, proto::DataItem)>>,
                tokio::sync::mpsc::Receiver<WSResult<(DataSplitIdx, proto::DataItem)>>,
                Vec<Range<usize>>, // split ranges
            ),
        > = HashMap::new();

        for idx in WantIdxIter::new(&ty, meta.data_item_cnt() as DataItemIdx) {
            tracing::debug!("prepare get data slices request with idx:{}", idx);
            let data_splits = &meta.datas_splits[idx as usize];
            for (splitidx, split) in data_splits.splits.iter().enumerate() {
                let _ = each_node_getdata
                    .entry(split.node_id)
                    .and_modify(|(req, splitidxs)| {
                        req.idxs.push(idx as u32);
                        splitidxs.push(splitidx);
                    })
                    .or_insert((
                        proto::GetOneDataRequest {
                            unique_id: unique_id.to_owned(),
                            idxs: vec![idx as u32],
                            delete,
                            return_data: true,
                        },
                        vec![splitidx],
                    ));
            }
            let (tx, rx) =
                tokio::sync::mpsc::channel::<WSResult<(DataSplitIdx, proto::DataItem)>>(3);
            let _ = each_item_idx_receive_worker_tx_rx_splits.insert(
                idx,
                (
                    tx,
                    rx,
                    data_splits
                        .splits
                        .iter()
                        .map(|split| {
                            split.data_offset as usize
                                ..split.data_offset as usize + split.data_size as usize
                        })
                        .collect::<Vec<_>>(),
                ),
            );
        }

        // this part is a little complex
        // 1. all the splits will be read parallelly
        // 2. for one dataitem (unique by idx), we want one worker to wait for ready dataitem(split)

        // 1. read tasks

        for (node_id, (req, splitidxs)) in each_node_getdata {
            let view = self.view.clone();
            // let req_idxs = req.idxs.clone();
            // let idx_2_sender_to_recv_worker = each_item_idx_receive_worker_tx_rx_splitcnt.clone();
            let idx_of_idx_and_sender_to_recv_worker = req
                .idxs
                .iter()
                .enumerate()
                .map(|(idx_of_idx, reqidx)| {
                    let tx_rx_splits = each_item_idx_receive_worker_tx_rx_splits
                        .get(&(*reqidx as DataItemIdx))
                        .unwrap();
                    (idx_of_idx, tx_rx_splits.0.clone())
                })
                .collect::<Vec<_>>();
            let unique_id = unique_id.clone();
            let _task = tokio::spawn(async move {
                tracing::debug!("rpc_call_get_data start, remote({})", node_id);
                let mut res = view
                    .data_general()
                    .rpc_call_get_data
                    .call(view.p2p(), node_id, req, Some(Duration::from_secs(30)))
                    .await;
                tracing::debug!("rpc_call_get_data returned, remote({})", node_id);

                // result will contain multiple splits of dataitems
                // so we need to send the result to the corresponding tx

                if res.is_err() {
                    let e = Arc::new(res.err().unwrap());
                    for (_idx_of_idx, tx) in idx_of_idx_and_sender_to_recv_worker {
                        tracing::warn!("send to data merge tasks failed: {:?}", e);
                        tx.send(Err(WSError::ArcWrapper(e.clone())))
                            .await
                            .expect("send to data merge tasks failed");
                    }
                } else {
                    for (idx_of_idx, tx) in idx_of_idx_and_sender_to_recv_worker {
                        let res = res.as_mut().unwrap();
                        if !res.success {
                            tx.send(Err(WsDataError::GetDataFailed {
                                unique_id: unique_id.clone(),
                                msg: std::mem::take(&mut res.message),
                            }
                            .into()))
                                .await
                                .expect("send to data merge tasks failed");
                        } else {
                            let _ = tx
                                .send(Ok((
                                    splitidxs[idx_of_idx],
                                    std::mem::take(&mut res.data[idx_of_idx]),
                                )))
                                .await
                                .expect("send to data merge tasks failed");
                        }
                    }
                }
            });
        }

        // 2. data merge tasks
        let mut merge_task_group_tasks = vec![];
        for idx in WantIdxIter::new(&ty, meta.data_item_cnt() as DataItemIdx) {
            let (_, rx, splits) = each_item_idx_receive_worker_tx_rx_splits
                .remove(&idx)
                .unwrap();
            let unique_id = unique_id.clone();
            let cache_mode = meta.cache_mode_visitor(idx);
            let task = tokio::spawn(async move {
                WriteSplitDataTaskGroup::new(unique_id.clone(), splits, rx, cache_mode)
            });
            merge_task_group_tasks.push((idx, task));
        }

        // 3. wait for results
        let mut idx_2_data_item = HashMap::new();
        for (idx, task) in merge_task_group_tasks {
            let merge_group = task.await;
            match merge_group {
                Err(e) => {
                    return Err(WsRuntimeErr::TokioJoin {
                        err: e,
                        context: format!("get data split failed, idx:{}", idx),
                    }
                    .into());
                }
                Ok(merge_group) => match merge_group.await {
                    Err(e) => {
                        return Err(e);
                    }
                    Ok(res) => {
                        let res = res.join().await;
                        match res {
                            Err(e) => {
                                return Err(e);
                            }
                            Ok(res) => {
                                let _ = idx_2_data_item.insert(idx, res);
                            }
                        }
                    }
                },
            }
        }

        // // TODO: 将这里获取到的数据写入到缓存中
        // for (idx, data_item) in idx_2_data_item.iter() {
        //     // 只缓存需要缓存的数据，前面拿到过
        //     if !cache[*idx as usize] {
        //         continue;
        //     }
        //     let cache_mode = meta.cache_mode_visitor(*idx);
        //     let cache_key = (unique_id.clone(), *idx);
        //     let cache_value = (meta.version, data_item.clone());
        //     if cache_mode.is_time_forever() {
        //         self.forever_cache.insert(cache_key, cache_value);
        //     } else if cache_mode.is_time_auto() {
        //         self.auto_cache.insert(cache_key, cache_value);
        //     }
        // }

        Ok((meta, idx_2_data_item))
    }

    // pub async fn get_data(
    //     &self,
    //     unique_id: impl Into<Vec<u8>>,
    // ) -> WSResult<(DataSetMetaV2, HashMap<(NodeID, usize), proto::DataItem>)> {
    //     let unique_id: Vec<u8> = unique_id.into();
    //     tracing::debug!("get_or_del_datameta_from_master start");
    //     // Step1: get meta
    //     let meta: DataSetMetaV2 = self
    //         .get_or_del_datameta_from_master(&unique_id, false)
    //         .await
    //         .map_err(|err| {
    //             if let WSError::WsDataError(WsDataError::DataSetNotFound { uniqueid }) = err {
    //                 tracing::debug!("data not found, uniqueid:{:?}", uniqueid);
    //                 return WSError::WsDataError(WsDataError::DataSetNotFound { uniqueid });
    //             }
    //             tracing::warn!("`get_data` failed, err:{}", err);
    //             err
    //         })?;
    //     tracing::debug!("get_or_del_datameta_from_master end\n  get_data_by_meta start");
    //     let res = self.get_data_by_meta(GetDataArg::All{

    //     }).await;
    //     tracing::debug!("get_data_by_meta end");
    //     res
    // }

    // /// return (meta, data_map)
    // /// data_map: (node_id, idx) -> data_items
    // pub async fn delete_data(
    //     &self,
    //     unique_id: impl Into<Vec<u8>>,
    // ) -> WSResult<(DataSetMetaV2, HashMap<(NodeID, usize), proto::DataItem>)> {
    //     let unique_id: Vec<u8> = unique_id.into();

    //     // Step1: get meta
    //     let meta: DataSetMetaV2 = self
    //         .get_or_del_datameta_from_master(&unique_id, true)
    //         .await
    //         .map_err(|err| {
    //             if let WSError::WsDataError(WsDataError::DataSetNotFound { uniqueid }) = err {
    //                 tracing::debug!("data not found, uniqueid:{:?}", uniqueid);
    //                 return WSError::WsDataError(WsDataError::DataSetNotFound { uniqueid });
    //             }
    //             tracing::warn!("`get_data` failed, err:{}", err);
    //             err
    //         })?;
    //     // .default_log_err("`delete_data`")?;

    //     return self.get_data_by_meta(GetDataArg::Delete{
    //         unique_id,
    //     }&, meta, true).await
    //     //
    // }

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
        tracing::debug!("write_data {:?} start", unique_id.clone());

        let log_tag = Arc::new(format!(
            "write_data,uid:{:?},operole:{:?}",
            str::from_utf8(&unique_id),
            context_openode_opetype_operole.as_ref().map(|v| &v.2)
        ));

        // Step 1: need the master to do the decision
        // - require for the latest version for write permission
        // - require for the distribution and cache mode
        let version_schedule_req = DataVersionScheduleRequest {
            unique_id: unique_id.clone(),
            version: 0,
            context: context_openode_opetype_operole.map(|(ope_node, ope_type, ope_role)| {
                proto::DataScheduleContext {
                    ope_node: ope_node as i64,
                    ope_type: ope_type as i32,
                    each_data_sz_bytes: datas
                        .iter()
                        .map(|data_item| data_item.data_sz_bytes() as u32)
                        .collect::<Vec<_>>(),
                    ope_role: Some(ope_role),
                }
            }),
        };
        tracing::debug!(
            "{} data version schedule requesting {:?}",
            log_tag,
            version_schedule_req
        );
        let version_schedule_resp = {
            let resp = self
                .rpc_call_data_version_schedule
                .call(
                    self.view.p2p(),
                    p2p.nodes_config.get_master_node(),
                    version_schedule_req,
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
        tracing::debug!(
            "{} data version scheduled, resp: {:?}",
            log_tag,
            version_schedule_resp
        );

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
            for (dataitem_idx, (one_data_splits, one_data_item)) in version_schedule_resp
                .split
                .into_iter()
                .zip(datas)
                .enumerate()
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
                    dataitem_idx: usize,
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
                        dataitem_idx,
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
}

/// the split of one dataitem
/// we need to know the split size for one data
#[derive(Serialize, Deserialize, Debug)]
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
