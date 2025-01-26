use crate::general::app::m_executor::Executor;
use crate::general::app::DataEventTrigger;
use crate::general::network::m_p2p::{P2PModule, RPCCaller, RPCHandler, RPCResponsor};
use crate::general::network::proto::{
    self, DataVersionScheduleRequest, DataVersionScheduleResponse,
};
use crate::result::{WSResult, WSResultExt};
use crate::sys::{LogicalModulesRef, NodeID};
use crate::util::JoinHandleWrapper;
use crate::{
    general::data::{
        m_data_general::{
            CacheMode, DataGeneral, DataItemIdx, DataSetMeta, DataSetMetaBuilder, DataSplit,
            EachNodeSplit,
        },
        m_kv_store_engine::{KeyType, KeyTypeDataSetMeta, KvAdditionalConf, KvStoreEngine},
    },
    master::app::{fddg::FuncTriggerType, m_app_master::MasterAppMgmt},
};
use crate::{
    general::network::http_handler::HttpHandler,
    logical_module_view_impl,
    sys::{LogicalModule, LogicalModuleNewArgs},
};
use async_trait::async_trait;
use rand::{thread_rng, Rng};
use std::collections::HashSet;
use std::time::Duration;
use ws_derive::LogicalModule;

logical_module_view_impl!(DataMasterView);
logical_module_view_impl!(DataMasterView, data_master, Option<DataMaster>);
logical_module_view_impl!(DataMasterView, data_general, DataGeneral);
logical_module_view_impl!(DataMasterView, app_master, Option<MasterAppMgmt>);
logical_module_view_impl!(DataMasterView, p2p, P2PModule);
logical_module_view_impl!(DataMasterView, http_handler, Box<dyn HttpHandler>);
logical_module_view_impl!(DataMasterView, kv_store_engine, KvStoreEngine);
logical_module_view_impl!(DataMasterView, executor, Executor);

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
    // return cache mode, splits, cache nodes
    fn plan_for_write_data(
        &self,
        data_unique_id: &[u8],
        _context: &proto::DataScheduleContext,
        func_trigger_type: FuncTriggerType,
    ) -> WSResult<(Vec<CacheMode>, Vec<DataSplit>, Vec<NodeID>)> {
        fn set_data_cache_mode_for_meta(
            req: &DataVersionScheduleRequest,
            builder: &mut DataSetMetaBuilder,
        ) {
            fn default_set_data_cache_mode_for_meta(
                req: &DataVersionScheduleRequest,
                builder: &mut DataSetMetaBuilder,
            ) {
                // for each item(by split length), set cache mode
                for idx in 0..req.context.as_ref().unwrap().each_data_sz_bytes.len() {
                    let _ = builder
                        .cache_mode_time_forever(idx as DataItemIdx)
                        .cache_mode_pos_allnode(idx as DataItemIdx)
                        .cache_mode_map_common_kv(idx as DataItemIdx);
                }
            }
            if let Some(context) = req.context.as_ref() {
                match context.ope_role.as_ref().unwrap() {
                    proto::data_schedule_context::OpeRole::UploadApp(_data_ope_role_upload_app) => {
                        let _ = builder
                            // 0 is app meta data, map to common kv
                            .cache_mode_time_forever(0)
                            .cache_mode_pos_allnode(0)
                            .cache_mode_map_common_kv(0)
                            // 1 is app package data, map to file
                            .cache_mode_time_forever(1)
                            .cache_mode_pos_allnode(1)
                            .cache_mode_map_file(1);
                    }
                    proto::data_schedule_context::OpeRole::FuncCall(_data_ope_role_func_call) => {
                        default_set_data_cache_mode_for_meta(req, builder);
                    }
                }
            } else {
                tracing::warn!(
                    "context is None, use default cache mode, maybe we need to suitable for this case"
                );
                default_set_data_cache_mode_for_meta(req, builder);
            }
        }

        fn decide_each_data_split(
            datamaster: &DataMaster,
            ctx: &proto::DataScheduleContext,
        ) -> Vec<DataSplit> {
            // let DEFAULT_SPLIT_SIZE = 4 * 1024 * 1024;
            let mut datasplits = vec![];
            let p2p = datamaster.view.p2p();

            // simply select a node
            let node_id = {
                let rand_node_idx = thread_rng().gen_range(0..p2p.nodes_config.node_cnt());
                let mut iter = p2p.nodes_config.all_nodes_iter();
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

        if let Ok(data_unique_id_str) = std::str::from_utf8(data_unique_id) {
            // get data binded functions
            // https://fvd360f8oos.feishu.cn/wiki/Zp19wf9sdilwVKk5PlKcGy0CnBd
            // app_name -> (apptype, fn_name -> fn_meta)
            let binded_funcs: std::collections::HashMap<
                String,
                (
                    crate::general::app::AppType,
                    std::collections::HashMap<String, crate::general::app::FnMeta>,
                ),
            > = self
                .view
                .app_master()
                .fddg
                .get_binded_funcs(data_unique_id_str, func_trigger_type);

            // filter functions by condition function
            for (appname, (app_type, fn_names)) in binded_funcs.iter_mut() {
                fn_names.retain(|fn_name, fn_meta| {
                    if let Some(data_accesses) = fn_meta.data_accesses.as_ref() {
                        // find data access discription
                        if let Some((key_pattern, data_access)) =
                            data_accesses.iter().find(|(key_pattern, data_access)| {
                                if let Some(event) = data_access.event.as_ref() {
                                    match event {
                                        DataEventTrigger::WriteWithCondition { condition }
                                        | DataEventTrigger::NewWithCondition { condition } => false,
                                        DataEventTrigger::Write | DataEventTrigger::New => false,
                                    }
                                } else {
                                    false
                                }
                            })
                        {
                            // with condition, call the condition function
                            let condition_func = match data_access.event.as_ref().unwrap() {
                                DataEventTrigger::WriteWithCondition { condition }
                                | DataEventTrigger::NewWithCondition { condition } => condition,
                                _ => panic!("logical error, find condition must be with condition, so current code is wrong"),
                            };

                            // call the condition function
                            self.view.executor().handle_local_call(resp, req)
                            
                        } else {
                            // without condition
                            true
                        }
                    } else {
                        true
                    }
                });
            }

            // we need master to schedule functions to get func target nodes
            // then we make the splits to prefer the target nodes and write cache when wrting splits
        }

        Ok((DataSetMetaBuilder::new().build().cache_mode, vec![], vec![]))
    }

    /// Check the dataset sync flow here:
    ///
    ///   https://fvd360f8oos.feishu.cn/docx/XoFudWhAgox84MxKC3ccP1TcnUh#share-Wg7Nd5iwooJiUAx79YqceHcHn4c
    async fn rpc_handler_data_version_and_sche(
        &self,
        responsor: RPCResponsor<DataVersionScheduleRequest>,
        req: DataVersionScheduleRequest,
    ) -> WSResult<()> {
        let kv_store_engine = self.view.kv_store_engine();
        let ctx = req
            .context
            .as_ref()
            .expect("context is required for DataScheduleContext");
        let metakey = KeyTypeDataSetMeta(&req.unique_id);
        let metakey_bytes = metakey.make_key();
        tracing::debug!("check version for data({:?})", req.unique_id);

        // now we expand the meta
        let (new_meta, cache_nodes) = {
            let update_version_lock = kv_store_engine.with_rwlock(&metakey_bytes);
            let _guard = update_version_lock.write();

            let dataset_meta = kv_store_engine.get(&metakey, true, KvAdditionalConf::default());

            // the we will make the  split plan and cache plan
            //  then expand the meta
            //  this process will fail if other write updated the unique id
            let (item_cache_modes, new_splits, cache_nodes) =
                self.plan_for_write_data(&req.unique_id, ctx, FuncTriggerType::DataWrite)?;

            // let takeonce=Some((new_meta,new_))
            let set_meta = if let Some((_kv_version, set_meta)) = dataset_meta {
                tracing::debug!("update dataset meta for data({:?})", req.unique_id);
                let version = set_meta.version;
                let mut builder = DataSetMetaBuilder::from(set_meta);
                // version
                let _ = builder.version(version + 1);
                // data splits bf cache mod
                let _ = builder.set_data_splits(new_splits);
                // cache mode
                let _ = builder.set_cache_mode_for_all(item_cache_modes);
                builder.build()
            } else {
                tracing::debug!("new dataset meta for data({:?})", req.unique_id);
                let mut builder = DataSetMetaBuilder::new();
                // version
                let _ = builder.version(1);
                // data splits bf cache mod
                let _ = builder.set_data_splits(new_splits);
                // cache mode
                let _ = builder.set_cache_mode_for_all(item_cache_modes);
                builder.build()
            };

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
            (set_meta, cache_nodes)
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

        // let cache_nodes =
        //     Self::decide_cache_nodes(req.context.as_ref().unwrap(), new_meta.cache_mode);

        tracing::debug!(
            "data:{:?} version required({}) and schedule done, caller will do following thing after receive `DataVersionScheduleResponse`",
            req.unique_id,
            new_meta.version
        );

        responsor
            .send_resp(DataVersionScheduleResponse {
                version: new_meta.version,
                cache_mode: new_meta.cache_mode.into_iter().map(|v| v as u32).collect(),
                split: new_meta
                    .datas_splits
                    .into_iter()
                    .map(|v| v.into())
                    .collect(),
                cache_nodes,
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
