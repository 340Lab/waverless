use crate::general::app::m_executor::EventCtx;
use crate::general::app::m_executor::Executor;
use crate::general::app::m_executor::FnExeCtxAsync;
use crate::general::app::m_executor::FnExeCtxAsyncAllowedType;
use crate::general::app::AppMetaManager;
use crate::general::app::DataEventTrigger;
use crate::general::app::{AffinityPattern, AffinityRule, NodeTag};
use crate::general::network::m_p2p::{P2PModule, RPCCaller, RPCHandler, RPCResponsor};
use crate::general::network::proto::{
    self, DataVersionScheduleRequest, DataVersionScheduleResponse,
};
use crate::master::m_master::{FunctionTriggerContext, Master};
use crate::result::{WSResult, WSResultExt};
use crate::sys::{LogicalModulesRef, NodeID};
use crate::util::JoinHandleWrapper;
use crate::{
    general::data::{
        m_data_general::{
            CacheMode, DataGeneral, DataItemIdx, DataSetMeta, DataSetMetaBuilder, DataSplit,
            EachNodeSplit, CACHE_MODE_MAP_COMMON_KV_MASK, CACHE_MODE_TIME_FOREVER_MASK,
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
logical_module_view_impl!(DataMasterView, appmeta_manager, AppMetaManager);
logical_module_view_impl!(DataMasterView, app_master, Option<MasterAppMgmt>);
logical_module_view_impl!(DataMasterView, p2p, P2PModule);
logical_module_view_impl!(DataMasterView, http_handler, Box<dyn HttpHandler>);
logical_module_view_impl!(DataMasterView, kv_store_engine, KvStoreEngine);
logical_module_view_impl!(DataMasterView, executor, Executor);
logical_module_view_impl!(DataMasterView, master, Option<Master>);

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
        let _ = self.rpc_caller_data_meta_update.regist(view.p2p());
        let _ = self
            .rpc_handler
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
    async fn plan_for_write_data(
        &self,
        data_unique_id: &[u8],
        context: &proto::DataScheduleContext,
        func_trigger_type: FuncTriggerType,
    ) -> WSResult<(Vec<CacheMode>, Vec<DataSplit>, Vec<NodeID>)> {
        // 如果不是有效的 UTF-8 字符串，直接返回空结果
        let data_unique_id_str = match std::str::from_utf8(data_unique_id) {
            Ok(s) => s,
            Err(_) => return Ok((DataSetMetaBuilder::new().build().cache_mode, vec![], vec![])),
        };

        // 获取绑定的函数
        let binded_funcs = self
            .view
            .app_master()
            .fddg
            .get_binded_funcs(data_unique_id_str, func_trigger_type);

        // 收集所有调度节点作为缓存节点
        let mut cache_nodes = HashSet::new();

        // 对每个绑定的函数进行调度
        for (app_name, (_, fn_names)) in &binded_funcs {
            for (fn_name, _unused) in fn_names {
                // 选择调度节点 (暂时不考虑亲和性规则)
                let target_node = self.view.master().select_node();

                // 将调度节点加入缓存节点集合
                let _ = cache_nodes.insert(target_node);

                // 创建函数触发上下文
                let ctx = FunctionTriggerContext {
                    app_name: app_name.clone(),
                    fn_name: fn_name.clone(),
                    data_unique_id: data_unique_id.to_vec(),
                    target_nodes: vec![target_node], // 只在选中的节点上触发
                    timeout: Duration::from_secs(60),
                    event_type: DataEventTrigger::Write, // 使用Write事件类型
                };

                // 发送触发请求并处理可能的错误
                if let Err(e) = self.view.master().trigger_func_call(ctx).await {
                    tracing::error!(
                        "Failed to trigger function {}/{} on node {}: {:?}",
                        app_name,
                        fn_name,
                        target_node,
                        e
                    );
                }
            }
        }

        // 将缓存节点集合转换为向量
        let cache_nodes: Vec<NodeID> = cache_nodes.into_iter().collect();

        // 根据缓存节点生成数据分片
        let mut splits = Vec::new();
        for sz in context.each_data_sz_bytes.iter() {
            // 选择主分片节点 (使用第一个缓存节点或随机选择)
            let primary_node = cache_nodes.first().copied().unwrap_or_else(|| {
                let p2p = self.view.p2p();
                let rand_node_idx = thread_rng().gen_range(0..p2p.nodes_config.node_cnt());
                let mut iter = p2p.nodes_config.all_nodes_iter();
                for _ in 0..rand_node_idx {
                    let _ = iter.next();
                }
                *iter
                    .next()
                    .expect("node count doesn't match all_nodes_iter")
                    .0
            });

            // 创建主分片
            let mut split = DataSplit {
                splits: vec![EachNodeSplit {
                    node_id: primary_node,
                    data_offset: 0,
                    data_size: *sz,
                    cache_mode: 0,
                }],
            };

            // 为每个缓存节点添加完整数据副本 (除了主分片节点)
            for &cache_node in cache_nodes.iter() {
                if cache_node != primary_node {
                    split.splits.push(EachNodeSplit {
                        node_id: cache_node,
                        data_offset: 0,
                        data_size: *sz,
                        cache_mode: 0,
                    });
                }
            }

            splits.push(split);
        }

        // 设置缓存模式
        let mut builder = DataSetMetaBuilder::new();

        // 设置数据分片
        let _ = builder.set_data_splits(splits.clone());

        // 设置缓存模式 - 对所有缓存节点启用永久缓存
        let cache_modes = vec![
            CACHE_MODE_TIME_FOREVER_MASK | CACHE_MODE_MAP_COMMON_KV_MASK;
            context.each_data_sz_bytes.len()
        ];
        let _ = builder.set_cache_mode_for_all(cache_modes.clone());

        Ok((cache_modes, splits, cache_nodes))
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
            // the we will make the  split plan and cache plan
            //  then expand the meta
            //  this process will fail if other write updated the unique id
            let (item_cache_modes, new_splits, cache_nodes) = self
                .plan_for_write_data(&req.unique_id, ctx, FuncTriggerType::DataWrite)
                .await?;

            let update_version_lock = kv_store_engine.with_rwlock(&metakey_bytes);
            let _guard = update_version_lock.write();
            let dataset_meta = kv_store_engine.get(&metakey, true, KvAdditionalConf::default());

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
            let _ = kv_store_engine.set(KeyTypeDataSetMeta(&req.unique_id), &set_meta, true)?;
            kv_store_engine.flush();
            (set_meta, cache_nodes)
        };

        // update version peers
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
            let serialized_meta = bincode::serialize(&new_meta).unwrap();
            let unique_id = req.unique_id.clone();
            let version = new_meta.version;
            let _ = tokio::spawn(async move {
                let p2p = view.p2p();
                let display_id = std::str::from_utf8(&unique_id)
                    .map_or_else(|_err| format!("{:?}", unique_id), |ok| ok.to_owned());
                tracing::debug!(
                    "updating version for data({:?}) to node: {}, this_node:  {}",
                    display_id,
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
