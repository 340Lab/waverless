use super::{
    m_kv_store_engine::{KeyTypeDataSetItem, KeyTypeDataSetMeta, KvStoreEngine},
    m_os::OperatingSystem,
    network::{
        m_p2p::{P2PModule, RPCCaller, RPCHandler, RPCResponsor},
        proto::{
            write_one_data_request::{data_item::Data, DataItem},
            DataMeta, DataModeDistribute, DataVersionRequest, WriteOneDataRequest,
            WriteOneDataResponse,
        },
    },
};
use crate::{
    general::network::proto::write_one_data_request,
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, NodeID},
    util::JoinHandleWrapper,
};
use crate::{
    result::{WSError, WsDataError},
    sys::LogicalModulesRef,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, time::Duration};
use ws_derive::LogicalModule;

// use super::m_appmeta_manager::AppMeta;

logical_module_view_impl!(DataGeneralView);
logical_module_view_impl!(DataGeneralView, p2p, P2PModule);
logical_module_view_impl!(DataGeneralView, data_general, DataGeneral);
logical_module_view_impl!(DataGeneralView, kv_store_engine, KvStoreEngine);
logical_module_view_impl!(DataGeneralView, os, OperatingSystem);

pub type DataVersion = u64;

#[derive(LogicalModule)]
pub struct DataGeneral {
    view: DataGeneralView,
    pub rpc_call_data_version: RPCCaller<DataVersionRequest>,

    rpc_call_write_once_data: RPCCaller<WriteOneDataRequest>,
    rpc_handler_write_once_data: RPCHandler<WriteOneDataRequest>,
}

#[async_trait]
impl LogicalModule for DataGeneral {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: DataGeneralView::new(args.logical_modules_ref.clone()),
            rpc_call_data_version: RPCCaller::new(),
            rpc_call_write_once_data: RPCCaller::new(),
            rpc_handler_write_once_data: RPCHandler::new(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start as master");
        let p2p = self.view.p2p();
        self.rpc_call_data_version.regist(p2p);
        self.rpc_call_write_once_data.regist(p2p);
        let view = self.view.clone();
        self.rpc_handler_write_once_data
            .regist(p2p, move |responsor, req| {
                let view = view.clone();
                let _ = tokio::spawn(async move {
                    view.data_general().write_one_data(responsor, req).await;
                });
                Ok(())
            });
        Ok(vec![])
    }
}

// pub enum DataWrapper {
//     Bytes(Vec<u8>),
//     File(PathBuf),
// }

impl DataGeneral {
    async fn write_one_data(
        &self,
        responsor: RPCResponsor<WriteOneDataRequest>,
        req: WriteOneDataRequest,
    ) {
        // ## verify data meta
        tracing::debug!("verify data meta bf write data");
        let Some(res) = self
            .view
            .kv_store_engine()
            .get(KeyTypeDataSetMeta(req.unique_id.as_bytes()))
        else {
            responsor.send_resp(WriteOneDataResponse {
                remote_version: 0,
                success: false,
                message: "Data meta not found".to_owned(),
            });
            return;
        };
        if res.version != req.version {
            responsor.send_resp(WriteOneDataResponse {
                remote_version: res.version,
                success: false,
                message: "Data meta version not match".to_owned(),
            });
            return;
        }
        if req.data.is_empty() {
            responsor.send_resp(WriteOneDataResponse {
                remote_version: res.version,
                success: false,
                message: "Data is empty".to_owned(),
            });
            return;
        }
        if req.data[0].data.is_none() {
            responsor.send_resp(WriteOneDataResponse {
                remote_version: res.version,
                success: false,
                message: "Data enum is none".to_owned(),
            });
            return;
        }
        for (_idx, data) in req.data.iter().enumerate() {
            match data.data.as_ref().unwrap() {
                write_one_data_request::data_item::Data::File(f) => {
                    if f.file_name.starts_with("/") {
                        responsor.send_resp(WriteOneDataResponse {
                            remote_version: res.version,
                            success: false,
                            message: format!(
                                "File name {} starts with / is forbidden",
                                f.file_name
                            ),
                        });
                        return;
                    }
                }
                _ => {}
            }
        }
        // ## write data
        tracing::debug!("start to write data");
        for (idx, data) in req.data.into_iter().enumerate() {
            match data.data.unwrap() {
                write_one_data_request::data_item::Data::File(f) => {
                    tracing::debug!("writing data part{} file {}", idx, f.file_name);
                    let p: std::path::PathBuf = self.view.os().file_path.join(f.file_name);
                    let view = self.view.clone();

                    let p2 = p.clone();
                    let res = if f.is_dir {
                        tokio::task::spawn_blocking(move || {
                            view.os().unzip_data_2_path(p2, f.file_content);
                        })
                    } else {
                        // flush to p
                        tokio::task::spawn_blocking(move || {
                            view.os().cover_data_2_path(p2, f.file_content);
                        })
                    };
                    let res = res.await;
                    if let Err(e) = res {
                        responsor.send_resp(WriteOneDataResponse {
                            remote_version: req.version,
                            success: false,
                            message: format!("Write file error: {:?}, path: {:?}", e, p),
                        });
                        return;
                    }
                }
                write_one_data_request::data_item::Data::RawBytes(bytes) => {
                    tracing::debug!("writing data part{} bytes", idx);
                    self.view.kv_store_engine().set(
                        KeyTypeDataSetItem {
                            uid: req.unique_id.as_bytes(),
                            idx: idx as u8,
                        },
                        &bytes,
                    );
                }
            }
        }
        self.view.kv_store_engine().flush();
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
    pub async fn get_data_item(&self, unique_id: String, idx: u8) -> Option<DataItem> {
        let Some(itembytes) = self.view.kv_store_engine().get(KeyTypeDataSetItem {
            uid: unique_id.as_bytes(),
            idx: idx as u8,
        }) else {
            return None;
        };
        Some(DataItem {
            data: Some(Data::RawBytes(itembytes)),
        })
    }

    pub async fn set_dataversion(&self, req: DataVersionRequest) -> WSResult<()> {
        // follower just update the version from master
        let old = self
            .view
            .kv_store_engine()
            .get(KeyTypeDataSetMeta(req.unique_id.as_bytes()));
        if let Some(old) = old {
            if old.version > req.version {
                return Err(WsDataError::SetExpiredDataVersion {
                    target_version: req.version,
                    cur_version: old.version,
                    data_id: req.unique_id.clone(),
                }
                .into());
                // responsor
                //     .send_resp(DataVersionResponse {
                //         version: old.version,
                //     })
                //     .await;
                // tracing::warn!("has larger version {}", old.version);
                // return Ok(());
            }
        }
        self.view.kv_store_engine().set(
            KeyTypeDataSetMeta(req.unique_id.as_bytes()),
            &DataSetMeta {
                version: req.version,
                data_metas: req.data_metas.into_iter().map(|v| v.into()).collect(),
                synced_nodes: HashSet::new(),
            },
        );
        self.view.kv_store_engine().flush();
        Ok(())
    }

    pub async fn write_data(
        &self,
        unique_id: String,
        data_metas: Vec<DataMeta>,
        datas: Vec<DataItem>,
    ) {
        if data_metas.len() == 0 {
            tracing::warn!("write_data must have >0 data metas");
            return;
        }
        if datas.len() != data_metas.len() {
            tracing::warn!("write_data data metas and datas length not match");
            return;
        }
        if DataModeDistribute::BroadcastRough as i32 == data_metas[0].distribute {
            self.write_data_broadcast_rough(unique_id, data_metas, datas)
                .await;
        }
    }
    async fn write_data_broadcast_rough(
        &self,
        unique_id: String,
        data_metas: Vec<DataMeta>,
        datas: Vec<DataItem>,
    ) {
        let p2p = self.view.p2p();
        let resp = self
            .rpc_call_data_version
            .call(
                self.view.p2p(),
                p2p.nodes_config.get_master_node(),
                DataVersionRequest {
                    unique_id: unique_id.clone(),
                    version: 0,
                    data_metas,
                },
                Some(Duration::from_secs(60)),
            )
            .await;
        let resp = match resp {
            Err(e) => {
                tracing::warn!("write_data_broadcast_rough require version error: {:?}", e);
                return;
            }
            Ok(ok) => ok,
        };

        tracing::debug!("start broadcast data with version");
        let version = resp.version;
        // use the got version to send to global paralell
        let mut tasks = vec![];

        for (_idx, node) in p2p.nodes_config.all_nodes_iter().enumerate() {
            let n = *node.0;
            let view = self.view.clone();
            let datas = datas.clone();
            let unique_id = unique_id.clone();
            // let datas = unsafe { util::SendNonNull(util::non_null(&datas)) };

            let t = tokio::spawn(async move {
                view.data_general()
                    .rpc_call_write_once_data
                    .call(
                        view.p2p(),
                        n,
                        WriteOneDataRequest {
                            unique_id,
                            version,
                            data: datas,
                        },
                        Some(Duration::from_secs(60)),
                    )
                    .await
            });

            tasks.push(t);
        }
        for t in tasks {
            let res = t.await.unwrap();
            match res {
                Err(e) => {
                    tracing::warn!("write_data_broadcast_rough broadcast error: {:?}", e);
                }
                Ok(ok) => {
                    if !ok.success {
                        tracing::warn!(
                            "write_data_broadcast_rough broadcast error: {:?}",
                            ok.message
                        );
                    }
                }
            }
        }
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

#[derive(Serialize, Deserialize)]
pub struct DataSetMeta {
    // unique_id: Vec<u8>,
    pub version: u64,
    pub data_metas: Vec<DataMetaSys>,
    pub synced_nodes: HashSet<NodeID>,
}

// pub struct DataDescriptionPart {
//     mode_dist: DataModeDistribute,
//     mode_cache: DataModeCache,
// }
// pub struct DataDescription {
//     small: DataDescriptionPart,
//     big: DataDescriptionPart,
// }

// impl Default for DataDescription {
//     fn default() -> Self {
//         Self {
//             small: DataDescriptionPart {
//                 mode_dist: DataModeDistribute::GlobalSyncRough,
//                 mode_cache: DataModeCache::AlwaysInMem,
//             },
//             big: DataDescriptionPart {
//                 mode_dist: DataModeDistribute::GlobalSyncRough,
//                 mode_cache: DataModeCache::AlwaysInFs,
//             },
//         }
//     }
// }

// // data binds
// pub trait Data {
//     fn disc() -> DataDescription;
// }

// impl Data for AppMeta {
//     fn disc() -> DataDescription {
//         DataDescription::default()
//     }
// }
