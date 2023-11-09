use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use super::{
    msg_pack::{MsgPack, RPCReq},
    p2p_quic::P2PQuicNode,
};
use crate::{
    kv::data_router::{self, DataRouter},
    module_iter::*,
    module_state_trans::ModuleSignal,
    module_view::{self, P2PModuleLMView},
    network::proto,
    result::{ErrCvt, NotMatchNodeErr, WSError, WSResult, WsNetworkConnErr, WsNetworkLogicErr},
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModules, NodeID},
    util::JoinHandleWrapper,
};

use async_trait::async_trait;
use parking_lot::RwLock;
use prost::{bytes::Bytes, Message};

pub type TaskId = u32;
pub type MsgId = u32;

#[async_trait]
pub trait P2PKernel: LogicalModule {
    async fn send_for_response(&self, nodeid: NodeID, req_data: Vec<u8>) -> WSResult<Vec<u8>>;
    async fn send(
        &self,
        node: NodeID,
        task_id: TaskId,
        msg_id: MsgId,
        req_data: Vec<u8>,
    ) -> WSResult<()>;
}

#[derive(LogicalModuleParent, LogicalModule)]
pub struct P2PModule {
    pub logical_modules_view: P2PModuleLMView,
    dispatch_map: RwLock<
        HashMap<
            u32,
            Box<dyn Fn(NodeID, &Self, TaskId, Bytes) -> WSResult<()> + 'static + Send + Sync>,
        >,
    >,
    waiting_tasks:
        RwLock<HashMap<(TaskId, NodeID), tokio::sync::oneshot::Sender<Box<dyn MsgPack>>>>,
    rpc_holder: RwLock<HashMap<(MsgId, NodeID), Arc<tokio::sync::Mutex<()>>>>,
    #[sub]
    pub p2p_kernel: P2PQuicNode,
    name: String,
    pub state_trans_tx: tokio::sync::broadcast::Sender<ModuleSignal>,
    pub peers: Vec<(SocketAddr, NodeID)>,
    pub this_node: (SocketAddr, NodeID),
    pub nodeid2addr: HashMap<NodeID, SocketAddr>,
    pub next_task_id: AtomicU32,
}

// // dispatch_map Box<dyn Fn(Bytes) -> WSResult<()>>
// unsafe impl Send for P2PModule {}
// unsafe impl Sync for P2PModule {}

#[async_trait]
impl LogicalModule for P2PModule {
    fn inner_new(mut args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        let (peers, this) = args.peers_this.take().unwrap();
        args.expand_parent_name(Self::self_name());
        let (tx, _rx) = tokio::sync::broadcast::channel(10);
        Self {
            logical_modules_view: P2PModuleLMView::new(),
            p2p_kernel: P2PQuicNode::new(args.clone()),
            dispatch_map: HashMap::new().into(),
            waiting_tasks: HashMap::new().into(),
            rpc_holder: HashMap::new().into(),
            name: args.parent_name,
            state_trans_tx: tx,
            nodeid2addr: peers
                .iter()
                .chain(Some(&this))
                .map(|(addr, id)| (*id, *addr))
                .collect(),
            peers,
            this_node: this,
            next_task_id: AtomicU32::new(0),
        }
    }

    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let sub = self.p2p_kernel.start().await?;
        Ok(sub)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl P2PModule {
    pub fn listen(&self) -> tokio::sync::broadcast::Receiver<ModuleSignal> {
        self.state_trans_tx.subscribe()
    }

    // 消息回来时，调用记录的回调函数
    pub fn regist_dispatch<M, F>(&self, m: M, f: F)
    where
        M: MsgPack + Default,
        F: Fn(NodeID, &P2PModule, TaskId, M) -> WSResult<()> + Send + Sync + 'static,
    {
        // self.p2p.regist_dispatch();
        let mut map = self.dispatch_map.write();
        let old = map.insert(
            m.msg_id(),
            Box::new(move |nid, this, task_id, data| {
                let msg = M::decode(data).map_err(|err| ErrCvt(err).to_ws_network_logic_err())?;
                f(nid, this, task_id, msg)
            }),
        );
        assert!(old.is_none());
    }

    // 自动完成response的匹配
    pub fn regist_rpc<REQ, RESP, F>(&self, req_handler: F)
    where
        REQ: MsgPack + Default,
        RESP: MsgPack + Default,
        F: Fn(NodeID, &P2PModule, TaskId, REQ) -> WSResult<()> + Send + Sync + 'static,
    {
        self.regist_dispatch(REQ::default(), req_handler);
        // self.p2p.regist_rpc();
        self.regist_dispatch(RESP::default(), |nid, this, taskid, v| {
            let cb = this.waiting_tasks.write().remove(&(taskid, nid));
            if let Some(pack) = cb {
                pack.send(Box::new(v)).unwrap_or_else(|err| {
                    panic!("send back to waiting task failed: {:?}", err);
                });
            } else {
                tracing::warn!("taskid: {} not found", taskid);
            }
            Ok(())
        })
    }
    pub async fn send_resp<RESP>(
        &self,
        node_id: NodeID,
        task_id: TaskId,
        resp: RESP,
    ) -> WSResult<()>
    where
        RESP: MsgPack + Default,
    {
        self.p2p_kernel
            .send(
                node_id,
                task_id,
                RESP::default().msg_id(),
                resp.encode_to_vec(),
            )
            .await
    }

    #[inline]
    pub async fn call_rpc<R>(&self, node_id: NodeID, req: &R) -> WSResult<R::Resp>
    where
        R: RPCReq,
    {
        self.call_rpc_inner::<R, R::Resp>(node_id, req).await
    }

    async fn call_rpc_inner<REQ, RESP>(&self, node_id: NodeID, r: &REQ) -> WSResult<RESP>
    where
        REQ: MsgPack,
        RESP: MsgPack,
    {
        // alloc from global
        let taskid: TaskId = self.next_task_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::oneshot::channel::<Box<dyn MsgPack>>();
        if self.rpc_holder.read().get(&(r.msg_id(), node_id)).is_none() {
            self.rpc_holder
                .write()
                .insert((r.msg_id(), node_id), Default::default());
        }
        let inner_lock = self
            .rpc_holder
            .read()
            .get(&(r.msg_id(), node_id))
            .unwrap()
            .clone();
        let hold_lock = inner_lock.lock().await;
        // tracing::info!("holding lock msg:{} node:{}", r.msg_id(), node_id);

        assert!(self
            .waiting_tasks
            .write()
            .insert((taskid, node_id), tx)
            .is_none());
        match self
            .p2p_kernel
            .send(node_id, taskid, r.msg_id(), r.encode_to_vec())
            .await
        {
            Ok(_) => {
                // tracing::info!("1holding lock msg:{} node:{}", r.msg_id(), node_id);
                if node_id == 3 {
                    // tracing::info!("rpc sent to node {} with taskid {}", node_id, taskid);
                }
            }
            Err(err) => {
                self.waiting_tasks.write().remove(&(taskid, node_id));
                // tracing::info!("1stop holding lock msg:{} node:{}", r.msg_id(), node_id);
                return Err(err);
            }
        }
        // TODO: handle timeout
        if node_id == 3 {
            // tracing::info!("waiting for response from {}", node_id);
        }
        // tracing::info!("2holding lock msg:{} node:{}", r.msg_id(), node_id);

        let resp = match tokio::time::timeout(Duration::from_millis(500), rx).await {
            Ok(resp) => resp.unwrap_or_else(|err| {
                panic!("waiting for response failed: {:?}", err);
            }),
            Err(err) => {
                self.waiting_tasks.write().remove(&(taskid, node_id));
                // let _ = self.p2p_kernel.close(node_id).await;
                if node_id == 3 {
                    tracing::warn!("rpc timeout: {:?} to node {}", err, node_id);
                }
                // tracing::warn!("rpc timeout: {:?} to node {}", err, node_id);
                // tracing::info!("2stop holding lock msg:{} node:{}", r.msg_id(), node_id);
                return Err(WsNetworkConnErr::RPCTimout(node_id).into());
            }
        };
        // tracing::info!("3holding lock msg:{} node:{}", r.msg_id(), node_id);
        if node_id == 3 {
            // tracing::info!("got response from {}", node_id);
        }
        let resp = resp.downcast::<RESP>().unwrap();
        // tracing::info!("3stop holding lock msg:{} node:{}", r.msg_id(), node_id);

        Ok(*resp)
    }

    pub fn dispatch(&self, nid: NodeID, id: MsgId, taskid: TaskId, data: Bytes) -> WSResult<()> {
        let read = self.dispatch_map.read();
        if let Some(cb) = read.get(&id) {
            cb(nid, self, taskid, data)?;
            Ok(())
        } else {
            tracing::warn!("not match id: {}", id);
            Err(WsNetworkLogicErr::MsgIdNotDispatchable(id).into())
        }
    }
    pub fn get_addr_by_id(&self, id: NodeID) -> WSResult<SocketAddr> {
        self.nodeid2addr
            .get(&id)
            .cloned()
            .ok_or_else(|| WsNetworkLogicErr::InvaidNodeID(id).into())
    }
}

// #[async_trait]
// impl P2P for P2PModule {
//     async fn send_for_response(&self, nodeid: NodeID, req_data: Vec<u8>) -> WSResult<Vec<u8>> {
//         // let (tx, rx) = tokio::sync::oneshot::channel();
//         // self.p2p_kernel.send(nodeid, req_data, tx).await?;
//         // let res = rx.await?;

//         Ok(vec![])
//     }

//     async fn send(&self, nodeid: NodeID, msg_id:u64,req_data: Vec<u8>) -> WSResult<()> {
//         self.p2p_kernel.send(nodeid, req_data).await?;
//         Ok(())
//     }
// }
