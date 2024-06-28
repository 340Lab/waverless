use std::cell::RefCell;
use std::collections::HashSet;
use std::time::Duration;

use crate::general::m_data_general::{DataGeneral, DataSetMeta};
use crate::general::m_kv_store_engine::{KeyTypeDataSetMeta, KvStoreEngine};
use crate::general::network::m_p2p::{P2PModule, RPCHandler, RPCResponsor};
use crate::general::network::proto::{self, DataVersionRequest, DataVersionResponse};
use crate::result::WSResult;
use crate::sys::LogicalModulesRef;
use crate::util::JoinHandleWrapper;
use crate::{
    general::network::http_handler::HttpHandler,
    logical_module_view_impl,
    sys::{LogicalModule, LogicalModuleNewArgs},
};
use async_trait::async_trait;
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
    rpc_handler: RPCHandler<proto::DataVersionRequest>,
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
        self.rpc_handler
            .regist(self.view.p2p(), move |responsor, req| {
                let view = view.clone();
                let _ = tokio::spawn(async move {
                    view.data_master()
                        .rpc_handler_dataversion(responsor, req)
                        .await
                });

                Ok(())
            });
        Ok(vec![])
    }
}

impl DataMaster {
    async fn rpc_handler_dataversion_require(
        &self,
        responsor: RPCResponsor<DataVersionRequest>,
        req: DataVersionRequest,
    ) -> WSResult<()> {
        // ## check version
        tracing::debug!("check version for data({})", req.unique_id);
        let v = self
            .view
            .kv_store_engine()
            .get(KeyTypeDataSetMeta(req.unique_id.as_bytes()));
        // ##  update version local
        tracing::debug!("update version local for data({})", req.unique_id);
        let setmeta = RefCell::new(Some(DataSetMeta {
            version: 1,
            data_metas: req.data_metas.iter().map(|v| v.clone().into()).collect(),
            synced_nodes: HashSet::new(),
        }));
        let v = v.map_or_else(
            || setmeta.borrow_mut().take().unwrap(),
            |set_meta| {
                let mut replace = setmeta.borrow_mut().take().unwrap();
                replace.version = set_meta.version + 1;
                replace
            },
        );
        self.view
            .kv_store_engine()
            .set(KeyTypeDataSetMeta(req.unique_id.as_bytes()), &v);
        self.view.kv_store_engine().flush();

        // update version peers
        let mut call_tasks = vec![];
        for p in &self.view.p2p().nodes_config.peers {
            let view = self.view.clone();
            let mut req = req.clone();
            let node = *p.0;
            req.version = v.version;
            let call_task = tokio::spawn(async move {
                let p2p = view.p2p();
                tracing::debug!(
                    "updating version for data({}) to node: {}",
                    req.unique_id,
                    node
                );
                (
                    view.data_general()
                        .rpc_call_data_version
                        .call(p2p, node, req, Some(Duration::from_secs(60)))
                        .await,
                    node,
                )
            });
            call_tasks.push(call_task);
        }
        let mut cancel = false;
        for t in call_tasks {
            if !cancel {
                let (res, n) = t.await.unwrap();

                match res {
                    Err(e) => {
                        tracing::warn!(
                            "one node {} keep up version data failed, abort broadcast, err:{:?}",
                            n,
                            e
                        );
                        cancel = true;
                    }
                    Ok(ok) => {
                        tracing::debug!(
                            "update version for data({}) to node: {}",
                            req.unique_id,
                            n
                        );
                        if ok.version != v.version {
                            tracing::warn!(
                                "one node keep up failed, abort broadcast, remote version:{}, cur version:{}",
                                ok.version,v.version
                            );
                            cancel = true;
                        }
                    }
                }
            } else {
                t.abort();
            }
        }
        if cancel {
            return Ok(());
        }
        tracing::debug!(
            "data:{} version:{} require done, followers are waiting for new data",
            req.unique_id,
            v.version
        );
        responsor
            .send_resp(DataVersionResponse { version: v.version })
            .await;
        Ok(())
    }
    async fn rpc_handler_dataversion_synced_on_node(
        &self,
        responsor: RPCResponsor<DataVersionRequest>,
        req: DataVersionRequest,
    ) -> WSResult<()> {
        // 1. check version
        let v = self
            .view
            .kv_store_engine()
            .get(KeyTypeDataSetMeta(req.unique_id.as_bytes()));
        let mut v = if let Some(v) = v {
            if v.version != req.version {
                responsor
                    .send_resp(DataVersionResponse { version: v.version })
                    .await;
                tracing::warn!(
                    "version not match for data({}), cur: {}",
                    req.unique_id,
                    v.version
                );
                return Ok(());
            }
            v
        } else {
            responsor
                .send_resp(DataVersionResponse { version: 0 })
                .await;
            tracing::warn!("version not match for data({}), cur: {}", req.unique_id, 0);
            return Ok(());
        };
        // ## update
        if !v.synced_nodes.insert(responsor.node_id()) {
            tracing::warn!("!!!! node repeated sync, check for bug");
        }
        // write back
        self.view
            .kv_store_engine()
            .set(KeyTypeDataSetMeta(req.unique_id.as_bytes()), &v);
        self.view.kv_store_engine().flush();
        tracing::debug!(
            "synced version({}) of data({}) on node{}",
            v.version,
            req.unique_id,
            responsor.node_id()
        );
        responsor
            .send_resp(DataVersionResponse { version: v.version })
            .await;
        Ok(())
    }
    async fn rpc_handler_dataversion(
        &self,
        responsor: RPCResponsor<DataVersionRequest>,
        req: DataVersionRequest,
    ) -> WSResult<()> {
        if req.version > 0 {
            self.rpc_handler_dataversion_synced_on_node(responsor, req)
                .await?;
        } else {
            self.rpc_handler_dataversion_require(responsor, req).await?;
        }

        Ok(())
    }
}
