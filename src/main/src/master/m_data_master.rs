use std::collections::HashSet;
use std::time::Duration;

use crate::general::m_data_general::{
    CacheModeVisitor, DataGeneral, DataSetMetaBuilder, DataSplit, EachNodeSplit,
};
use crate::general::m_kv_store_engine::{
    KeyType, KeyTypeDataSetMeta, KvAdditionalConf, KvStoreEngine,
};
use crate::general::network::m_p2p::{P2PModule, RPCCaller, RPCHandler, RPCResponsor};
use crate::general::network::proto::{
    self, DataVersionScheduleRequest, DataVersionScheduleResponse,
};
use crate::result::{WSResult, WSResultExt};
use crate::sys::{LogicalModulesRef, NodeID};
use crate::util::JoinHandleWrapper;
use crate::{
    general::network::http_handler::HttpHandler,
    logical_module_view_impl,
    sys::{LogicalModule, LogicalModuleNewArgs},
};
use async_trait::async_trait;
use rand::{thread_rng, Rng};
use ws_derive::LogicalModule;

logical_module_view_impl!(DataMasterView);
logical_module_view_impl!(DataMasterView, data_master, Option<DataMaster>);
logical_module_view_impl!(DataMasterView, data_general, DataGeneral);
logical_module_view_impl!(DataMasterView, p2p, P2PModule);
logical_module_view_impl!(DataMasterView, http_handler, Box<dyn HttpHandler>);
logical_module_view_impl!(DataMasterView, kv_store_engine, KvStoreEngine);

#[derive(LogicalModule)]
pub struct DataMaster {
    view: DataMasterView,
    rpc_handler: RPCHandler<proto::DataVersionScheduleRequest>,
    rpc_caller_data_meta_update: RPCCaller<proto::DataMetaUpdateRequest>,
}

#[async_trait]
impl LogicalModule for DataMaster {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            rpc_handler: RPCHandler::new(),
            view: DataMasterView::new(args.logical_modules_ref.clone()),
            rpc_caller_data_meta_update: RPCCaller::new(),
            // view: DataMasterView::new(args.logical_modules_ref.clone()),
        }
    }
    // async fn init(&self) -> WSResult<()> {
    //     // self.view.http_handler().building_router().route("/:app/:fn", post(handler2))
    //     Ok(())
    // }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start as master");
        let view = self.view.clone();
        self.rpc_caller_data_meta_update.regist(view.p2p());
        self.rpc_handler
            .regist(self.view.p2p(), move |responsor, req| {
                let view = view.clone();
                let _ = tokio::spawn(async move {
                    view.data_master()
                        .rpc_handler_data_version_and_sche(responsor, req)
                        .await
                });

                Ok(())
            });
        Ok(vec![])
    }
}

impl DataMaster {
    fn set_data_cache_mode_default(builder: &mut DataSetMetaBuilder) {
        // default cache mode
        let _ = builder
            .cache_mode_map_common_kv()
            .cache_mode_pos_auto()
            .cache_mode_time_auto();
    }
    fn set_data_cache_mode_for_meta(
        req: &DataVersionScheduleRequest,
        builder: &mut DataSetMetaBuilder,
    ) {
        if let Some(context) = req.context.as_ref() {
            match context.ope_role.as_ref().unwrap() {
                proto::data_schedule_context::OpeRole::UploadApp(_data_ope_role_upload_app) => {
                    let _ = builder
                        .cache_mode_time_forever()
                        .cache_mode_pos_allnode()
                        .cache_mode_map_file();
                }
                proto::data_schedule_context::OpeRole::FuncCall(_data_ope_role_func_call) => {
                    Self::set_data_cache_mode_default(builder);
                }
            }
        } else {
            Self::set_data_cache_mode_default(builder);
        }
    }

    fn decide_cache_nodes(
        _ctx: &proto::DataScheduleContext,
        cache_mode: CacheModeVisitor,
    ) -> Vec<NodeID> {
        if cache_mode.is_time_auto() {
            // for time auto, we just do the cache when data is get
            return vec![];
        } else if cache_mode.is_time_forever() {
            if cache_mode.is_pos_auto() {
                // for pos auto, we just do the cache when data is get
                // simple strategy temporarily
                return vec![];
            } else if cache_mode.is_pos_specnode() {
                return vec![];
            } else {
                // all node just return empty, can be just refered from cache_mode
                // no need to redundant info in cache nodes
                return vec![];
            }
        } else {
            panic!("not supported time mode {:?}", cache_mode)
        }
    }

    fn decide_each_data_split(&self, ctx: &proto::DataScheduleContext) -> Vec<DataSplit> {
        // let DEFAULT_SPLIT_SIZE = 4 * 1024 * 1024;
        let mut datasplits = vec![];

        // simply select a node
        let node_id = {
            let rand_node_idx = thread_rng().gen_range(0..self.view.p2p().nodes_config.node_cnt());
            let mut iter = self.view.p2p().nodes_config.all_nodes_iter();
            for _ in 0..rand_node_idx {
                let _ = iter.next();
            }
            *iter
                .next()
                .expect("node count doesn't match all_nodes_iter")
                .0
        };

        for sz in ctx.each_data_sz_bytes.iter() {
            datasplits.push(DataSplit {
                splits: vec![EachNodeSplit {
                    node_id,
                    data_offset: 0,
                    data_size: *sz,
                }],
            });
        }
        tracing::debug!("decide_each_data_split res: {:?}", datasplits);
        datasplits
    }

    /// Check the dataset sync flow here:
    ///
    ///   https://fvd360f8oos.feishu.cn/docx/XoFudWhAgox84MxKC3ccP1TcnUh#share-Wg7Nd5iwooJiUAx79YqceHcHn4c
    async fn rpc_handler_data_version_and_sche(
        &self,
        responsor: RPCResponsor<DataVersionScheduleRequest>,
        req: DataVersionScheduleRequest,
    ) -> WSResult<()> {
        // ## check version and decide data split
        let kv_store_engine = self.view.kv_store_engine();
        let ctx = req
            .context
            .as_ref()
            .expect("context is required for DataScheduleContext");
        tracing::debug!("check version for data({:?})", req.unique_id);
        // update the dataset's version
        let new_meta = {
            let key_bytes = KeyTypeDataSetMeta(&req.unique_id).make_key();

            let update_version_lock = kv_store_engine.with_rwlock(&key_bytes);
            let _guard = update_version_lock.write();

            let dataset_meta = kv_store_engine.get(
                &KeyTypeDataSetMeta(&req.unique_id),
                true,
                KvAdditionalConf::default(),
            );
            let set_meta = dataset_meta.map_or_else(
                || {
                    let mut builder = DataSetMetaBuilder::new();
                    let _ = builder.version(1);
                    Self::set_data_cache_mode_for_meta(&req, &mut builder);
                    let _ = builder.set_data_splits(self.decide_each_data_split(ctx));
                    builder.build()
                },
                |(_kv_version, mut set_meta)| {
                    set_meta.version += 1;
                    set_meta
                    // let mut replace = setmeta.borrow_mut().take().unwrap();
                    // replace.version = set_meta.version + 1;
                    // replace
                },
            );
            // ##  update version local
            tracing::debug!(
                "update version local for data({:?}), the updated meta is {:?}",
                req.unique_id,
                set_meta
            );
            let _ = kv_store_engine
                .set(KeyTypeDataSetMeta(&req.unique_id), &set_meta, true)
                .unwrap();
            kv_store_engine.flush();
            set_meta
        };

        // update version peers
        // let mut call_tasks = vec![];
        let need_notify_nodes = {
            let mut need_notify_nodes = HashSet::new();
            for one_data_splits in &new_meta.datas_splits {
                for data_split in &one_data_splits.splits {
                    let _ = need_notify_nodes.insert(data_split.node_id);
                }
            }
            // TODO: do we need to notify cache nodes?
            need_notify_nodes
        };

        for need_notify_node in need_notify_nodes {
            let view = self.view.clone();
            // let mut req = req.clone();
            // req.version = new_meta.version;

            // don't need to retry or wait
            let serialized_meta = bincode::serialize(&new_meta).unwrap();
            let unique_id = req.unique_id.clone();
            let version = new_meta.version;
            let _call_task = tokio::spawn(async move {
                let p2p = view.p2p();
                tracing::debug!(
                    "updating version for data({:?}) to node: {}, this_node:  {}",
                    std::str::from_utf8(&unique_id).map_or_else(
                        |_err| { format!("{:?}", unique_id) },
                        |ok| { ok.to_owned() }
                    ),
                    need_notify_node,
                    p2p.nodes_config.this_node()
                );

                tracing::debug!(
                    "async notify `DataMetaUpdateRequest` to node {}",
                    need_notify_node
                );
                let resp = view
                    .data_master()
                    .rpc_caller_data_meta_update
                    .call(
                        p2p,
                        need_notify_node,
                        proto::DataMetaUpdateRequest {
                            unique_id,
                            version,
                            serialized_meta,
                        },
                        Some(Duration::from_secs(60)),
                    )
                    .await;
                if let Err(err) = resp {
                    tracing::error!(
                        "notify `DataMetaUpdateRequest` to node {} failed: {}",
                        need_notify_node,
                        err
                    );
                } else if let Ok(ok) = resp {
                    if ok.version != version {
                        tracing::error!("notify `DataMetaUpdateRequest` to node {} failed: version mismatch, expect: {}, remote: {}", need_notify_node, version, ok.version);
                    }
                }
            });
        }

        // call_tasks.push(call_task);

        let cache_nodes = Self::decide_cache_nodes(
            req.context.as_ref().unwrap(),
            CacheModeVisitor(new_meta.cache_mode),
        );

        tracing::debug!(
            "data:{:?} version required({}) and schedule done, caller will do following thing after receive `DataVersionScheduleResponse`",
            req.unique_id,
            new_meta.version
        );

        responsor
            .send_resp(DataVersionScheduleResponse {
                version: new_meta.version,
                cache_plan: Some(proto::DataCachePlan {
                    cache_mode: new_meta.cache_mode as u32,
                    cache_nodes,
                }),
                split: new_meta
                    .datas_splits
                    .into_iter()
                    .map(|v| v.into())
                    .collect(),
            })
            .await
            .todo_handle();
        Ok(())
    }
    // async fn rpc_handler_dataversion_synced_on_node(
    //     &self,
    //     responsor: RPCResponsor<DataVersionScheduleRequest>,
    //     req: DataVersionScheduleRequest,
    // ) -> WSResult<()> {
    //     // 1. check version
    //     let v = self
    //         .view
    //         .kv_store_engine()
    //         .get(KeyTypeDataSetMeta(req.unique_id.as_bytes()));
    //     let mut v = if let Some((_kvversion, v)) = v {
    //         if v.version != req.version {
    //             responsor
    //                 .send_resp(DataVersionScheduleResponse { version: v.version })
    //                 .await;
    //             tracing::warn!(
    //                 "version not match for data({}), cur: {}",
    //                 req.unique_id,
    //                 v.version
    //             );
    //             return Ok(());
    //         }
    //         v
    //     } else {
    //         responsor
    //             .send_resp(DataVersionScheduleResponse { version: 0 })
    //             .await;
    //         tracing::warn!("version not match for data({}), cur: {}", req.unique_id, 0);
    //         return Ok(());
    //     };
    //     // ## update
    //     if !v.synced_nodes.insert(responsor.node_id()) {
    //         tracing::warn!("!!!! node repeated sync, check for bug");
    //     }
    //     // write back
    //     self.view
    //         .kv_store_engine()
    //         .set(KeyTypeDataSetMeta(req.unique_id.as_bytes()), &v);
    //     self.view.kv_store_engine().flush();
    //     tracing::debug!(
    //         "synced version({}) of data({}) on node{}",
    //         v.version,
    //         req.unique_id,
    //         responsor.node_id()
    //     );
    //     responsor
    //         .send_resp(DataVersionScheduleResponse { version: v.version })
    //         .await;
    //     Ok(())
    // }
}
