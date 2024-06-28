use std::collections::HashSet;

use crate::general::m_data_general::DataSetMeta;
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

logical_module_view_impl!(DataFollowerView);
logical_module_view_impl!(DataFollowerView, data_follower, Option<DataFollower>);
logical_module_view_impl!(DataFollowerView, p2p, P2PModule);
logical_module_view_impl!(DataFollowerView, http_handler, Box<dyn HttpHandler>);
logical_module_view_impl!(DataFollowerView, kv_store_engine, KvStoreEngine);

#[derive(LogicalModule)]
pub struct DataFollower {
    view: DataFollowerView,
    rpc_handler: RPCHandler<proto::DataVersionRequest>,
}

#[async_trait]
impl LogicalModule for DataFollower {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            rpc_handler: RPCHandler::new(),
            view: DataFollowerView::new(args.logical_modules_ref.clone()),
            // view: DataFollowerView::new(args.logical_modules_ref.clone()),
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
                    view.data_follower()
                        .rpc_handler_dataversion(responsor, req)
                        .await
                });

                Ok(())
            });
        Ok(vec![])
    }
}

impl DataFollower {
    async fn rpc_handler_dataversion(
        &self,
        responsor: RPCResponsor<DataVersionRequest>,
        req: DataVersionRequest,
    ) -> WSResult<()> {
        tracing::debug!(
            "follower receive version({}) for data({})",
            req.version,
            req.unique_id
        );
        // follower just update the version from master
        let old = self
            .view
            .kv_store_engine()
            .get(KeyTypeDataSetMeta(req.unique_id.as_bytes()));
        if let Some(old) = old {
            if old.version > req.version {
                responsor
                    .send_resp(DataVersionResponse {
                        version: old.version,
                    })
                    .await;
                tracing::warn!("follower has larger version {}", old.version);
                return Ok(());
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
        tracing::debug!("follower updated version({})", req.version);
        responsor
            .send_resp(DataVersionResponse {
                version: req.version,
            })
            .await;
        Ok(())
    }
}
