use std::{
    collections::HashMap,
    marker::PhantomData,
    net::SocketAddr,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use super::{
    m_p2p_quic::P2PQuicNode,
    msg_pack::{MsgPack, RPCReq},
};
use crate::{
    config::NodesConfig,
    logical_module_view_impl,
    result::{ErrCvt, WSResult, WSResultExt, WsNetworkConnErr, WsNetworkLogicErr},
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef, NodeID},
    util::JoinHandleWrapper,
};

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use prost::bytes::Bytes;
use ws_derive::LogicalModule;

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

#[derive(Default)]
pub struct MsgSender<M: MsgPack> {
    _phantom: std::marker::PhantomData<M>,
}

#[derive(Default)]
pub struct MsgHandler<M: MsgPack> {
    _phantom: std::marker::PhantomData<M>,
}

#[derive(Default)]
pub struct RPCCaller<R: RPCReq> {
    _phantom: std::marker::PhantomData<R>,
}

#[derive(Default)]
pub struct RPCHandler<R: RPCReq> {
    _phantom: std::marker::PhantomData<R>,
}

impl<M: MsgPack> MsgSender<M> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
    pub async fn send(&self, p2p: &P2PModule, node_id: NodeID, msg: M) -> WSResult<()> {
        p2p.p2p_kernel
            .send(node_id, 0, msg.msg_id(), msg.encode_to_vec())
            .await
    }
}

impl<M: MsgPack> MsgHandler<M> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
    pub fn regist(
        &self,
        p2p: &P2PModule,
        msg_handler: impl Fn(Responser, M) -> WSResult<()> + Send + Sync + 'static,
    ) where
        M: MsgPack + Default,
    {
        p2p.regist_dispatch::<M, _>(M::default(), msg_handler);
    }
}

impl<R: RPCReq> RPCCaller<R> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
    pub fn regist(&self, p2p: &P2PModule) {
        p2p.regist_rpc_send::<R>();
    }
    pub async fn call(
        &self,
        p2p: &P2PModule,
        node_id: NodeID,
        req: R,
        dur: Option<Duration>,
    ) -> WSResult<R::Resp> {
        #[cfg(feature = "rpc-log")]
        tracing::debug!(
            "call rpc {:?} from {} to {}",
            req,
            p2p.nodes_config.this_node(),
            node_id
        );
        p2p.call_rpc::<R>(node_id, req, dur).await
    }
}

impl<R: RPCReq> RPCHandler<R> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
    pub fn regist<F>(&self, p2p: &P2PModule, req_handler: F)
    where
        F: Fn(RPCResponsor<R>, R) -> WSResult<()> + Send + Sync + 'static,
    {
        p2p.regist_rpc_recv::<R, F>(req_handler);
    }
    pub async fn call(
        &self,
        p2p: &P2PModule,
        node_id: NodeID,
        req: R,
        dur: Option<Duration>,
    ) -> WSResult<R::Resp> {
        p2p.call_rpc::<R>(node_id, req, dur).await
    }
}

#[derive(LogicalModule)]
pub struct P2PModule {
    // pub logical_modules_view: P2PModuleLMView,
    dispatch_map: RwLock<
        HashMap<
            u32,
            Box<
                dyn Fn(NodeID, &Self, TaskId, DispatchPayload) -> WSResult<()>
                    + 'static
                    + Send
                    + Sync,
            >,
        >,
    >,
    waiting_tasks: crossbeam_skiplist::SkipMap<
        (TaskId, NodeID),
        Mutex<Option<tokio::sync::oneshot::Sender<Box<dyn MsgPack>>>>,
    >,
    pub p2p_kernel: P2PQuicNode,
    // pub state_trans_tx: tokio::sync::broadcast::Sender<ModuleSignal>,
    pub nodes_config: NodesConfig,
    pub next_task_id: AtomicU32,
    view: P2PView,
}

// // dispatch_map Box<dyn Fn(Bytes) -> WSResult<()>>
// unsafe impl Send for P2PModule {}
// unsafe impl Sync for P2PModule {}

logical_module_view_impl!(P2PView);
logical_module_view_impl!(P2PView, p2p, P2PModule);

#[async_trait]
impl LogicalModule for P2PModule {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        let nodes_config = args.nodes_config.clone();
        // args.expand_parent_name(Self::self_name());
        // let (tx, _rx) = tokio::sync::broadcast::channel(10);
        Self {
            p2p_kernel: P2PQuicNode::new(args.clone()),
            dispatch_map: HashMap::new().into(),
            waiting_tasks: Default::default(),
            nodes_config,
            next_task_id: AtomicU32::new(0),
            view: P2PView::new(args.logical_modules_ref.clone()),
        }
    }

    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let sub = self.p2p_kernel.start().await?;
        Ok(sub)
    }
}

pub struct RPCResponsor<R: RPCReq> {
    _p: PhantomData<R>,
    responsor: Responser,
}
impl<R: RPCReq> RPCResponsor<R> {
    pub async fn send_resp(&self, resp: R::Resp) -> WSResult<()> {
        self.responsor.send_resp(resp).await
    }
    pub fn node_id(&self) -> NodeID {
        self.responsor.node_id
    }
    pub fn task_id(&self) -> TaskId {
        self.responsor.task_id
    }
}

pub struct Responser {
    task_id: TaskId,
    pub node_id: NodeID,
    view: P2PView,
}

impl Responser {
    pub async fn send_resp<RESP>(&self, resp: RESP) -> WSResult<()>
    where
        RESP: MsgPack + Default,
    {
        #[cfg(feature = "rpc-log")]
        tracing::debug!(
            "resp rpc {:?} from {} to {}",
            resp,
            self.view.p2p().nodes_config.this_node(),
            self.node_id
        );
        if self.view.p2p().nodes_config.this.0 == self.node_id {
            self.view.p2p().dispatch(
                self.node_id,
                resp.msg_id(),
                self.task_id,
                DispatchPayload::Local(Box::new(resp)),
            )
        } else {
            self.view
                .p2p()
                .send_resp(self.node_id, self.task_id, resp)
                .await
        }
    }
}

pub enum DispatchPayload {
    Remote(Bytes),
    /// zero copy of sub big memory ptr like vector,string,etc.
    Local(Box<dyn MsgPack>),
}

impl From<Bytes> for DispatchPayload {
    fn from(b: Bytes) -> Self {
        DispatchPayload::Remote(b)
    }
}

impl P2PModule {
    pub fn find_peer_id(&self, addr: &SocketAddr) -> Option<NodeID> {
        self.nodes_config.peers.iter().find_map(
            |(id, peer)| {
                if peer.addr == *addr {
                    Some(*id)
                } else {
                    None
                }
            },
        )
    }
    // pub fn listen(&self) -> tokio::sync::broadcast::Receiver<ModuleSignal> {
    //     self.state_trans_tx.subscribe()
    // }

    // 消息回来时，调用记录的回调函数
    fn regist_dispatch<M, F>(&self, m: M, f: F)
    where
        M: MsgPack + Default,
        F: Fn(Responser, M) -> WSResult<()> + Send + Sync + 'static,
    {
        // self.p2p.regist_dispatch();
        let mut map = self.dispatch_map.write();
        let old = map.insert(
            m.msg_id(),
            Box::new(move |nid, p2p, task_id, data| {
                let msg = match data {
                    DispatchPayload::Remote(b) => {
                        assert!(nid != p2p.view.p2p().nodes_config.this.0);
                        M::decode(b).map_err(|err| ErrCvt(err).to_ws_network_logic_err())?
                    }
                    DispatchPayload::Local(b) => {
                        assert!(nid == p2p.view.p2p().nodes_config.this.0);
                        *b.downcast::<M>().unwrap()
                    }
                };
                // if msg.msg_id() == 3 {
                //     tracing::info!("dispatch {:?} from: {}", msg, nid);
                // }
                // tracing::debug!("dispatch from {} msg:{:?}", nid, msg);
                #[cfg(feature = "rpc-log")]
                tracing::debug!(
                    "handling rpc {:?} from {} to {}",
                    msg,
                    nid,
                    p2p.nodes_config.this_node(),
                );
                f(
                    Responser {
                        task_id,
                        node_id: nid,
                        view: p2p.view.clone(),
                    },
                    msg,
                )
            }),
        );
        assert!(old.is_none());
    }

    fn regist_rpc_send<REQ>(&self)
    where
        REQ: RPCReq,
    {
        // self.p2p.regist_rpc();
        self.regist_dispatch(REQ::Resp::default(), |resp, v| {
            let cb = resp
                .view
                .p2p()
                .waiting_tasks
                .remove(&(resp.task_id, resp.node_id));
            if let Some(pack) = cb {
                pack.value()
                    .lock()
                    .take()
                    .unwrap()
                    .send(Box::new(v))
                    .unwrap_or_else(|err| {
                        panic!("send back to waiting task failed: {:?}", err);
                    });
            } else {
                tracing::warn!("taskid: {} not found", resp.task_id);
            }
            Ok(())
        })
    }

    // 自动完成response的匹配
    fn regist_rpc_recv<REQ, F>(&self, req_handler: F)
    where
        REQ: RPCReq,
        // RESP: MsgPack + Default,
        F: Fn(RPCResponsor<REQ>, REQ) -> WSResult<()> + Send + Sync + 'static,
    {
        self.regist_dispatch(REQ::default(), move |resp, req| {
            req_handler(
                RPCResponsor {
                    _p: PhantomData,
                    responsor: resp,
                },
                req,
            )
        });
    }

    // pub fn regist_rpc<REQ, F>(&self, req_handler: F)
    // where
    //     REQ: RPCReq,
    //     // RESP: MsgPack + Default,
    //     F: Fn(Responser, REQ) -> WSResult<()> + Send + Sync + 'static,
    // {
    //     self.regist_rpc_recv::<REQ, F>(req_handler);
    //     self.regist_rpc_send::<REQ>();
    // }

    async fn send_resp<RESP>(&self, node_id: NodeID, task_id: TaskId, resp: RESP) -> WSResult<()>
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
    async fn call_rpc<R>(&self, node_id: NodeID, req: R, dur: Option<Duration>) -> WSResult<R::Resp>
    where
        R: RPCReq,
    {
        let dur = dur.unwrap_or(Duration::from_millis(10000));
        self.call_rpc_inner::<R, R::Resp>(node_id, req, dur).await
    }

    async fn call_rpc_inner<REQ, RESP>(
        &self,
        node_id: NodeID,
        r: REQ,
        dur: Duration,
    ) -> WSResult<RESP>
    where
        REQ: MsgPack,
        RESP: MsgPack,
    {
        // tracing::debug!("call_rpc_inner req{:?}", r);
        // alloc from global
        let taskid: TaskId = self.next_task_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::oneshot::channel::<Box<dyn MsgPack>>();

        if node_id == self.nodes_config.this.0 {
            let _ = self
                .waiting_tasks
                .insert((taskid, node_id), Some(tx).into());
            if let Err(e) = self.dispatch(
                //返回结果未处理     曾俊
                node_id,
                r.msg_id(),
                taskid,
                DispatchPayload::Local(Box::new(r)),
            ) {
                tracing::error!("Failed to dispatch rpc: {}", e);
            }
            //.todo_handle();
            //虞光勇修改，修改原因：在调用 todo_handle 方法时遇到了缺少参数的问题。需要确保在调用 todo_handle 方法时提供所需的字符串参数。
            //修改内容：加入字符串参数。
            // .todo_handle("This part of the code needs to be implemented.");
            let resp = rx.await.unwrap();
            let resp = resp.downcast::<RESP>().unwrap();

            return Ok(*resp);
        }

        // if self.rpc_holder.read().get(&(r.msg_id(), node_id)).is_none() {
        //     let res = self
        //         .rpc_holder
        //         .write()
        //         .insert((r.msg_id(), node_id), Default::default());
        //     assert!(res.is_none());
        // }
        // let inner_lock = self
        //     .rpc_holder
        //     .read()
        //     .get(&(r.msg_id(), node_id))
        //     .unwrap()
        //     .clone();
        // let _hold_lock = inner_lock.lock().await;
        // tracing::info!("holding lock msg:{} node:{}", r.msg_id(), node_id);

        let _ = self
            .waiting_tasks
            .insert((taskid, node_id), Some(tx).into());

        // tracing::debug!("rpc send to node {} with taskid {}", node_id, taskid);
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
                let _ = self.waiting_tasks.remove(&(taskid, node_id)).unwrap();
                // tracing::info!("1stop holding lock msg:{} node:{}", r.msg_id(), node_id);
                tracing::error!(
                    "rpc send failed: {:?}, request({:?}) from node({:?})",
                    err,
                    r,
                    self.nodes_config.this_node()
                );
                return Err(err);
            }
        }

        // tracing::debug!(
        //     "rpc waiting for response from node {} for task {}",
        //     node_id,
        //     taskid
        // );
        let resp = match tokio::time::timeout(dur, rx).await {
            Ok(resp) => resp.unwrap_or_else(|err| {
                panic!("waiting for response failed: {:?}", err);
            }),
            Err(err) => {
                // maybe removed or not
                let _ = self.waiting_tasks.remove(&(taskid, node_id));
                // let _ = self.p2p_kernel.close(node_id).await;

                tracing::error!(
                    "rpc timeout: {:?} to node {} with req {:?}",
                    err,
                    node_id,
                    r
                );

                // tracing::warn!("rpc timeout: {:?} to node {}", err, node_id);
                // tracing::info!("2stop holding lock msg:{} node:{}", r.msg_id(), node_id);
                return Err(WsNetworkConnErr::RPCTimout(node_id).into());
            }
        };

        let resp = resp.downcast::<RESP>().unwrap();

        Ok(*resp)
    }

    pub fn dispatch(
        &self,
        nid: NodeID,
        id: MsgId,
        taskid: TaskId,
        data: DispatchPayload,
    ) -> WSResult<()> {
        let read = self.dispatch_map.read();
        if let Some(cb) = read.get(&id) {
            // tracing::debug!("dispatch {} from: {}", id, nid);
            cb(nid, self, taskid, data)?;
            Ok(())
        } else {
            tracing::warn!(
                "not match id: {}, this node: {}",
                id,
                self.nodes_config.this_node()
            );
            Err(WsNetworkLogicErr::MsgIdNotDispatchable(id).into())
        }
    }
    pub fn get_addr_by_id(&self, id: NodeID) -> WSResult<SocketAddr> {
        self.nodes_config.peers.get(&id).map_or_else(
            || Err(WsNetworkLogicErr::InvaidNodeID(id).into()),
            |v| Ok(v.addr),
        )
    }
}
