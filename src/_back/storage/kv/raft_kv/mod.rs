//!
//! # Meta Dist Kv
//!
//! stores basical meta data like router table
//!

// mod openraft_adapter;
// pub mod tikvraft_proxy;
mod async_raft_kernel;
// use self::tikvraft_proxy::TiKvRaftModule;

// use self::async_raft_kernel::storage::ClientRequest;

use self::async_raft_kernel::storage::{ClientRequest, OpeType};

use super::{
    dist_kv::KvNode,
    kv_interface::{KvInterface, SetOptions},
};
use crate::{
    network::proto::{
        self,
        kv::{
            kv_request::KvPutRequest, kv_response::KvPairOpt, KeyRange, KvPair, KvPairs, KvRequest,
            MetaKvRequest,
        },
    },
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, MetaKvView, NodeID},
    util::JoinHandleWrapper,
};

use async_raft::{error::ClientReadError, raft::ClientWriteRequest, ClientWriteError};
// use async_raft::{error::ClientReadError, raft::ClientWriteRequest, ClientWriteError};
use async_trait::async_trait;
use prost::Message;
use ws_derive::LogicalModule;

pub type RaftModule = async_raft_kernel::AsyncRaftModule;

#[derive(LogicalModule)]
pub struct RaftKvNode {
    pub raft_inner: RaftModule,
    view: MetaKvView,
}

#[async_trait]
impl LogicalModule for RaftKvNode {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            raft_inner: RaftModule::new(args.clone()),
            view: MetaKvView::new(args.logical_modules_ref.clone()),
            // raft_module: TiKvRaftModule::new(args.clone()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let mut all = vec![];
        // handle remote get/set/delete

        all.append(&mut self.raft_inner.start().await?);
        Ok(all)
    }
}

#[async_trait]
impl KvNode for RaftKvNode {
    async fn ready(&self) -> bool {
        let res = self.raft_inner.raft().client_read().await;
        match res {
            Ok(()) => true,
            Err(e) => match e {
                ClientReadError::ForwardToLeader(Some(_l)) => true,
                _ => false,
            },
        }
    }
}

#[async_trait]
impl KvInterface for RaftKvNode {
    async fn get(&self, key_range: KeyRange) -> WSResult<Vec<KvPair>> {
        // get from local persist
        // self.view
        let local_kv = self.view.local_kv().as_ref().unwrap_or_else(|| {
            tracing::error!("local kv not found for raft kv");
            panic!("local kv not found for raft kv");
        });

        // TODO: rewrite the key with prefix
        local_kv.get(key_range).await
    }
    async fn set(&self, kvs: Vec<KvPair>, _opts: SetOptions) -> WSResult<Vec<KvPairOpt>> {
        // let req = ClientWriteRequest::new(ClientRequest {
        //     client: "".to_owned(),
        //     serial: 0,
        //     status: "".to_owned(),
        // });

        match self
            .raft_inner
            .raft()
            .client_write(ClientWriteRequest::new(ClientRequest {
                client: "".to_owned(),
                serial: 0,
                status: "".to_owned(),
                ope_type: OpeType::Set,
                operation: KvPairs { kvs }.encode_to_vec(),
            }))
            .await
        {
            Ok(_resp) => {
                // tracing::info!("set result: {:?}", v);
                // read from the storage
                Ok(vec![])
            }
            Err(err) => match err {
                ClientWriteError::RaftError(_) => {
                    tracing::error!("raft error");
                    Ok(vec![])
                }
                ClientWriteError::ForwardToLeader(req, leader) => {
                    if let Some(leader) = leader {
                        let res = self
                            .view
                            .p2p()
                            .call_rpc::<MetaKvRequest>(
                                leader as NodeID,
                                &MetaKvRequest {
                                    request: Some(KvRequest {
                                        op: Some(proto::kv::kv_request::Op::Set(KvPutRequest {
                                            kvs: KvPairs::decode(req.operation.as_slice())
                                                .unwrap()
                                                .into(),
                                        })),
                                    }),
                                },
                            )
                            .await?;
                        return Ok(res.response.unwrap().kvs.into());
                    }
                    Ok(vec![])
                }
            },
        }
    }
}
