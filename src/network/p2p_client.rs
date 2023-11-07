use super::proto;
use crate::{
    module_iter::*,
    result::{WSError, WSResult, WsNetworkLogicErr},
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModules, NodeID},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub struct RemoteCommunicator {}

impl RemoteCommunicator {
    // // no reponse needed
    // pub fn sync_send<R: prost::Message>(&self, req: R) -> WSResult<()> {

    //     req.encode_to_vec();
    // }

    // pub async fn send(&self, req: Request) -> WSResult<Vec<u8>> {
    //     Ok(vec![])
    // }

    // pub async fn send_for_response(&self, req: Request) -> WSResult<Vec<u8>> {
    //     Ok(vec![])
    // }
}

#[derive(LogicalModuleParent, LogicalModule)]
pub struct P2PClient {
    name: String,
}

impl P2PClient {
    pub fn regist_nodeid(&self) {
        // broadcast 2 request for node id from the first config node
    }
    pub fn get_remote_target(&self, nodeid: NodeID) -> RemoteCommunicator {
        RemoteCommunicator {}
    }
}

impl LogicalModule for P2PClient {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            name: format!("{}::{}", args.parent_name, Self::self_name()),
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        Ok(vec![])
    }
}
