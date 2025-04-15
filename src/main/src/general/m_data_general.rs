use super::{
    m_kv_store_engine::{KeyTypeDataSetItem, KeyTypeDataSetMeta, KvStoreEngine},
    m_os::OperatingSystem,
    network::{
        m_p2p::{P2PModule, RPCCaller, RPCHandler, RPCResponsor},
        proto::{
            self,
            write_one_data_request::{data_item::Data, DataItem},
            DataMeta, DataModeDistribute, DataVersionScheduleRequest, WriteOneDataRequest,
            WriteOneDataResponse,
        },
        proto_ext::ProtoExtDataItem,
    },
};
use crate::{
    general::network::proto::write_one_data_request,
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, NodeID},
    util::JoinHandleWrapper,
};
use crate::{result::WsDataError, sys::LogicalModulesRef};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use ws_derive::LogicalModule;

// use super::m_appmeta_manager::AppMeta;

logical_module_view_impl!(DataGeneralView);
logical_module_view_impl!(DataGeneralView, p2p, P2PModule);
logical_module_view_impl!(DataGeneralView, data_general, DataGeneral);
logical_module_view_impl!(DataGeneralView, kv_store_engine, KvStoreEngine);
logical_module_view_impl!(DataGeneralView, os, OperatingSystem);

pub type DataVersion = u64;

pub const DATA_UID_PREFIX_APP_META: &str = "app";

// const DATA_UID_PREFIX_OBJ: &str = "obj";

#[derive(LogicalModule)]
pub struct DataGeneral {
    view: DataGeneralView,
    pub rpc_call_data_version: RPCCaller<DataVersionScheduleRequest>,

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
        // ## verify data
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

    pub async fn set_dataversion(&self, req: DataVersionScheduleRequest) -> WSResult<()> {
        // follower just update the version from master
        let old = self
            .view
            .kv_store_engine()
            .get(KeyTypeDataSetMeta(req.unique_id.as_bytes()));

        let Some(mut old) = old else {
            return Err(WsDataError::DataSetNotFound {
                uniqueid: req.unique_id,
            }
            .into());
        };

        // only the latest version has the permission
        if old.version > req.version {
            return Err(WsDataError::SetExpiredDataVersion {
                target_version: req.version,
                cur_version: old.version,
                data_id: req.unique_id.clone(),
            }
            .into());
        }

        // update the version
        old.version = req.version;

        self.view.kv_store_engine().set(
            KeyTypeDataSetMeta(req.unique_id.as_bytes()),
            &old, // &DataSetMetaV1 {
                  //     version: req.version,
                  //     data_metas: req.data_metas.into_iter().map(|v| v.into()).collect(),
                  //     synced_nodes: HashSet::new(),
                  // },
        );
        self.view.kv_store_engine().flush();
        Ok(())
    }

    ///  check the design here
    ///  https://fvd360f8oos.feishu.cn/docx/XoFudWhAgox84MxKC3ccP1TcnUh#share-Rtxod8uDqoIcRwxOM1rccuXxnQg
    pub async fn write_data(
        &self,
        unique_id: String,
        // data_metas: Vec<DataMeta>,
        datas: Vec<DataItem>,
        context_openode_opetype_operole: Option<(
            NodeID,
            proto::DataOpeType,
            proto::data_schedule_context::OpeRole,
        )>,
    ) {
        let p2p = self.view.p2p();

        // Step 1: need the master to do the decision
        // - require for the latest version for write permission
        // - require for the distribution and cache mode
        let resp = {
            let resp = self
                .rpc_call_data_version
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
                                data_sz_bytes: datas.iter().map(|v| v.data_sz_bytes()).collect(),
                                ope_role: Some(ope_role),
                            },
                        ),
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
            resp
        };

        // Step2: dispatch the data source and caches
        {
            let mut write_source_data_tasks = vec![];

            // write the data split to kv
            for one_data_splits in resp.split {
                let mut last_node_begin: Option<(NodeID, usize)> = None;
                let flush_the_data = |nodeid: NodeID, begin: usize| {
                    let t = tokio::spawn(async move {});
                    write_source_data_tasks.push(t);
                };
                for (idx, node) in one_data_splits.node_ids.iter().enumerate() {
                    if let Some((node, begin)) = last_node_begin {
                        if node != *node {
                            // flush the data
                        } else {
                            last_node_begin = Some((*node, idx));
                        }
                    } else {
                        last_node_begin = Some((*node, idx));
                    }
                }

                // one_data_splits.node_ids.
            }
        }

        // if DataModeDistribute::BroadcastRough as i32 == data_metas[0].distribute {
        //     self.write_data_broadcast_rough(unique_id, data_metas, datas)
        //         .await;
        // }
    }
    async fn write_data_broadcast_rough(
        &self,
        unique_id: String,
        data_metas: Vec<DataMeta>,
        datas: Vec<DataItem>,
    ) {
        let p2p = self.view.p2p();

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

/// the data's all in one meta
/// https://fvd360f8oos.feishu.cn/docx/XoFudWhAgox84MxKC3ccP1TcnUh#share-Tqqkdxubpokwi5xREincb1sFnLc
#[derive(Serialize, Deserialize)]
pub struct DataSetMetaV1 {
    // unique_id: Vec<u8>,
    pub version: u64,
    pub data_metas: Vec<DataMetaSys>,
    pub synced_nodes: HashSet<NodeID>,
}
#[derive(Serialize, Deserialize)]
pub struct DataSetMetaV2 {
    // unique_id: Vec<u8>,
    pub api_version: u8,
    pub version: u64,
    pub cache_mode: u16,
    pub datas_splits: Vec<Vec<NodeID>>,
}

pub struct DataSetMetaBuilder {
    building: Option<DataSetMetaV2>,
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
        self.building.as_mut().unwrap().cache_mode &= 0x00111111;
        self
    }

    pub fn cache_mode_pos_allnode(&mut self) -> &mut Self {
        self.building.as_mut().unwrap().cache_mode &= 0x11001111;
        self
    }

    pub fn cache_mode_pos_specnode(&mut self) -> &mut Self {
        self.building.as_mut().unwrap().cache_mode &= 0x11011111;
        self
    }

    pub fn version(&mut self, version: u64) -> &mut Self {
        self.building.as_mut().unwrap().version = version;
        self
    }

    pub fn set_data_splits(&mut self, splits: Vec<Vec<NodeID>>) -> &mut Self {
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
            data_metas,
            synced_nodes,
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
