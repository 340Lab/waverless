use std::{cell::UnsafeCell, collections::HashMap, net::SocketAddr, sync::Weak};

use crate::{
    kv::data_router::{self, DataRouter},
    kv::dist_kv_raft::tikvraft_proxy::RaftMsg,
    module_iter::*,
    module_state_trans::ModuleSignal,
    module_view::{self, P2PModuleLMView},
    result::{ErrCvt, NotMatchNodeErr, WSError, WSResult, WsNetworkLogicErr},
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModules, NodeID},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use parking_lot::RwLock;
use prost::{bytes::Bytes, Message};

use super::{p2p_quic::P2PQuicNode, serial::MsgPack};

#[async_trait]
pub trait P2P: Send + LogicalModule {
    async fn send_for_response(&self, nodeid: NodeID, req_data: Vec<u8>) -> WSResult<Vec<u8>>;
}

#[derive(LogicalModuleParent, LogicalModule)]
pub struct P2PModule {
    pub logical_modules_view: P2PModuleLMView,
    dispatch_map: RwLock<HashMap<u32, Box<dyn Fn(Bytes) -> WSResult<()>>>>,
    #[sub]
    pub p2p_kernel: P2PQuicNode,
    name: String,
    pub tx: tokio::sync::broadcast::Sender<ModuleSignal>,
}

impl LogicalModule for P2PModule {
    fn inner_new(mut args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        args.expand_parent_name(Self::self_name());
        let (tx, _rx) = tokio::sync::broadcast::channel(10);
        Self {
            logical_modules_view: P2PModuleLMView::new(),
            p2p_kernel: P2PQuicNode::new(args.clone()),
            dispatch_map: HashMap::new().into(),
            name: args.parent_name,
            tx,
        }
    }

    fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let sub = self.p2p_kernel.start()?;
        Ok(sub)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl P2PModule {
    pub fn listen(&self) -> tokio::sync::broadcast::Receiver<ModuleSignal> {
        self.tx.subscribe()
    }

    // 消息回来时，调用记录的回调函数
    pub fn regist_dispatch<M, F>(&self, f: F)
    where
        M: MsgPack,
        F: Fn(M) -> WSResult<()> + 'static + Send,
    {
        // self.p2p.regist_dispatch();
        let mut map = self.dispatch_map.write();
        let old = map.insert(
            M::msg_id(),
            Box::new(move |data| {
                let msg = M::decode(data).map_err(|err| ErrCvt(err).to_ws_network_logic_err())?;
                f(msg)
            }),
        );
        assert!(old.is_none());
    }

    // 自动完成response的匹配
    pub fn regist_rpc(&self) {
        // self.p2p.regist_rpc();
    }
    pub fn dispatch(&self, id: u32, data: Bytes) -> WSResult<()> {
        let read = self.dispatch_map.read();
        if let Some(cb) = read.get(&id) {
            cb(data)?;
            Ok(())
        } else {
            tracing::warn!("not match id: {}", id);
            Err(WsNetworkLogicErr::MsgIdNotDispatchable(id).into())
        }
    }
}
