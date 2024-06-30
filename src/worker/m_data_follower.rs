use std::collections::HashSet;

use crate::general::m_data_general::{DataGeneral, DataSetMeta};
use crate::general::m_kv_store_engine::{KeyTypeDataSetMeta, KvStoreEngine};
use crate::general::network::m_p2p::{P2PModule, RPCHandler, RPCResponsor};
use crate::general::network::proto::{self, DataVersionRequest, DataVersionResponse};
use crate::result::{WSError, WSResult, WsDataError};
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
logical_module_view_impl!(DataFollowerView, data_general, DataGeneral);

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
        let targetv = req.version;
        tracing::debug!(
            "follower receive version({}) for data({})",
            req.version,
            req.unique_id
        );

        if let Err(e) = self.view.data_general().set_dataversion(req).await {
            tracing::warn!("set_dataversion failed, err:{:?}", e);
            let cur_version = match e {
                WSError::WsDataError(WsDataError::SetExpiredDataVersion {
                    cur_version, ..
                }) => cur_version,
                _ => 0,
            };
            responsor
                .send_resp(DataVersionResponse {
                    version: cur_version,
                })
                .await;
            return Err(e);
        }

        tracing::debug!("follower updated version({})", targetv);
        responsor
            .send_resp(DataVersionResponse { version: targetv })
            .await;
        Ok(())
    }
}
