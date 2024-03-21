use std::{
    collections::HashMap,
    marker::PhantomData,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use super::{
    m_p2p_quic::P2PQuicNode,
    msg_pack::{MsgPack, RPCReq},
};
use crate::{
    config::NodesConfig,
    logical_module_view_impl,
    result::{ErrCvt, WSResult, WsNetworkConnErr, WsNetworkLogicErr},
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef, NodeID},
    util::JoinHandleWrapper,
};

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use prost::bytes::Bytes;
use ws_derive::LogicalModule;

pub type TaskId = u32;
pub type MsgId = u32;

/* The core function and interface of P2P module are defined. It is mainly used to manage the communication between P2P nodes, 
including the sending and receiving of messages. */
#[async_trait]
pub trait P2PKernel: LogicalModule {

    /* This function is used to send messages to other nodes and wait for a response from the other node. The 'nodeid' parameter indicates 
    the ID of the target node, and 'req_data' indicates the message content to be sent. The 'WSResult<Vec<u8>>' function returns a Future of 
    type WSResult<vec<U8>> containing the content of the response message from the other node. */

    async fn send_for_response(&self, nodeid: NodeID, req_data: Vec<u8>) -> WSResult<Vec<u8>>;

    /* This function is used to send a message to a specified node without waiting for a response from the other node. Parameter 'node' 
    indicates the ID of the target node, 'task_id' indicates the task ID of the message, 'msg_id' indicates the ID of the message, and 'req_data' 
    indicates the content of the message to be sent. The function returns a Future of type 'WSResult<()>', representing the result of the 
    message being sent. */
    async fn send(
        &self,
        node: NodeID,
        task_id: TaskId,
        msg_id: MsgId,
        req_data: Vec<u8>,
    ) -> WSResult<()>;
}

/// For message sending
#[derive(Default)]
pub struct MsgSender<M: MsgPack> {
    _phantom: std::marker::PhantomData<M>,
}

impl<M: MsgPack> MsgSender<M> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    /* This function is used to send a message to the specified node. The 'p2p' parameter is a reference to the P2PModule instance, which is used 
    to obtain the P2P core functionality. 'node_id' Indicates the ID of the target node. 'msg' is the message to be sent and is of type generic M, 
    which is the message type that implements the MsgPack trait. The function sends the message to the specified node by calling the send method 
    of p2p_kernel, and returns a Future of type WSResult<()>, representing the result of the message. */
    pub async fn send(&self, p2p: &P2PModule, node_id: NodeID, msg: M) -> WSResult<()> {
        p2p.p2p_kernel
            .send(node_id, 0, msg.msg_id(), msg.encode_to_vec())
            .await
    }
}


/// For message processing
#[derive(Default)]
pub struct MsgHandler<M: MsgPack> {
    _phantom: std::marker::PhantomData<M>,
}

impl<M: MsgPack> MsgHandler<M> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    /* This function is used for message processing. The p2p parameter is a reference to the P2PModule instance, which is used to obtain 
    the P2P core functionality. msg_handler is a closure for processing received messages that takes a Responser argument and a generic 
    M message and returns a Future of type WSResult<()> representing the result of processing the message. At the same time, the function 
    records the id of the message being processed and the function handling it by calling the p2p regist_dispatch method (calling the 
    callback function of the record), which uses the default value of generic M to identify the message type. */
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

/// Used to send RPC requests to other nodes
#[derive(Default)]
pub struct RPCCaller<R: RPCReq> {
    _phantom: std::marker::PhantomData<R>,
}

impl<R: RPCReq> RPCCaller<R> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    /* Used to register the request type of the RPC call and call p2p's regist_dispatch method in the function (call record 
        callback function) to record the message id being processed and the function that handled it */
    pub fn regist(&self, p2p: &P2PModule) {
        p2p.regist_rpc_send::<R>();
    }

    /* This function is used to make an RPC call request to the specified node, accepting the p2p instance, node ID, RPC request, 
    and an optional timeout time as parameters. In the internal, it calls the call_rpc() method to send the RPC request, and returns an 
    asynchronous result of type WSResult, containing the response of the RPC request. */
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

// Used to process received RPC requests
#[derive(Default)]
pub struct RPCHandler<R: RPCReq> {
    _phantom: std::marker::PhantomData<R>,
}

impl<R: RPCReq> RPCHandler<R> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    /* Handlers for logging RPC requests. Accepts a handler function req_handler that takes an RPCResponsor<R> parameter and an 
    R parameter and returns a WSResult<()>. Internally, it calls the p2p regist_rpc_recv() method to record the received RPC request and its handlers. */
    pub fn regist<F>(&self, p2p: &P2PModule, req_handler: F)
    where
        F: Fn(RPCResponsor<R>, R) -> WSResult<()> + Send + Sync + 'static,
    {
        p2p.regist_rpc_recv::<R, F>(req_handler);
    }

    /* It is used to initiate an RPC call request to a specified node. Accept the p2p instance, node ID, RPC request, 
    and optional timeout as parameters. Internally, it invokes p2p's call_rpc() method to send the RPC request and returns 
    an asynchronous result of type WSResult containing the result of the RPC response. */
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

/// The core function of P2P module includes various data structures and methods to deal with P2P communication.
#[derive(LogicalModule)]
pub struct P2PModule {
    // pub logical_modules_view: P2PModuleLMView,
    // A read/write lock that stores the mapping of message distribution between nodes and is used to distribute messages to the corresponding handlers based on message type and task ID.
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

    /// A skip table that stores tasks waiting for a response, including task ids, node ids, and response senders.
    waiting_tasks: crossbeam_skiplist::SkipMap<
        (TaskId, NodeID),
        Mutex<Option<tokio::sync::oneshot::Sender<Box<dyn MsgPack>>>>,
    >,

    /// A hash table of read/write locks to store the status information of an ongoing RPC call.
    rpc_holder: RwLock<HashMap<(MsgId, NodeID), Arc<tokio::sync::Mutex<()>>>>,

    /// P2PQuicNode instance, which handles the specific logic of P2P communication.
    pub p2p_kernel: P2PQuicNode,
    // pub state_trans_tx: tokio::sync::broadcast::Sender<ModuleSignal>,

    /// Node configuration information.
    pub nodes_config: NodesConfig,

    /// An atomic U32 type used to generate a unique task ID.
    pub next_task_id: AtomicU32,

    /// P2PView example, which is used to provide view information about the P2P module.
    view: P2PView,
}

// // dispatch_map Box<dyn Fn(Bytes) -> WSResult<()>>
// unsafe impl Send for P2PModule {}
// unsafe impl Sync for P2PModule {}

logical_module_view_impl!(P2PView);
logical_module_view_impl!(P2PView, p2p, P2PModule);

#[async_trait]
impl LogicalModule for P2PModule {
    /* LogicalModule trait 中定义的一个关联函数，用于创建 P2PModule 的新实例。接受一个 LogicalModuleNewArgs 类型的参数，
    其中包含了创建实例所需的各种参数。 */

    /* An associated function defined in the LogicalModule trait to create a new instance of P2PModule. 
    Accepts a parameter of type LogicalModuleNewArgs, which contains the various parameters required to create the instance. */
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
            rpc_holder: HashMap::new().into(),
            nodes_config,
            next_task_id: AtomicU32::new(0),
            view: P2PView::new(args.logical_modules_ref.clone()),
        }
    }
    // LogicalModule trait 中定义的一个关联函数，用于启动 P2PModule。在内部调用了 P2PQuicNode 的start方法，用于启动quic协议的网络连接
    
    /* An associated function defined in the LogicalModule trait to start P2PModule. Internally, the start method 
    of P2PQuicNode is called to start the network connection of the quic protocol */
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let sub = self.p2p_kernel.start().await?;
        Ok(sub)
    }
}

/// RPC调用的响应方，用于发送RPC调用的响应结果。
pub struct RPCResponsor<R: RPCReq> {
    _p: PhantomData<R>,
    responsor: Responser,
}

impl<R: RPCReq> RPCResponsor<R> {
    /* 一个异步函数，用于发送RPC调用的响应结果。resp 参数，表示要发送的响应结果，类型为 R::Resp, 调用
     responsor 的 send_resp() 方法，并等待其完成，将结果返回 */

    /* An asynchronous function that sends the result of an RPC call in response. The resp parameter, 
    which represents the response result to be sent, is of type R::Resp, calls responsor's send_resp() 
    method, waits for it to complete, and returns the result */
    pub async fn send_resp(&self, resp: R::Resp) -> WSResult<()> {
        self.responsor.send_resp(resp).await
    }
    // 获取相应节点的 id
    // Gets the id of the corresponding node
    pub fn node_id(&self) -> NodeID {
        self.responsor.node_id
    }
    // 获取响应的任务id
    // Gets the task id of the response
    pub fn task_id(&self) -> TaskId {
        self.responsor.task_id
    }
}

/// 代表了RPC调用的响应方的信息，包括任务ID、节点ID以及P2P视图。
/// Information that represents the responder to the RPC call, including the task ID, node ID, and P2P view.
pub struct Responser {
    task_id: TaskId,
    pub node_id: NodeID,
    view: P2PView,
}

impl Responser {

    /* 一个异步函数，用于发送RPC调用的响应结果。接受 resp 参数，表示要发送的响应结果，类型为实现了 MsgPack + Default trait 的 RESP 类型。
        如果当前节点是响应方所在的节点（即本地节点），则调用本地的消息分发函数来处理响应。如果当前节点不是响应方所在的节点，则通过 P2P 
        视图发送响应给目标节点。最终返回一个 WSResult<()> 类型的结果，表示响应发送的结果。*/

    /* An asynchronous function that sends the result of an RPC call in response. Accepts the resp parameter, representing 
    the response result to be sent, of the RESP type that implements the MsgPack + Default trait. If the current node is 
    the node where the responder resides (that is, the local node), the local message distribution function is invoked to 
    process the response. If the current node is not the node where the responder resides, the response is sent to the 
    target node through the P2P view. Finally, a result of type WSResult<()> is returned, representing the result sent 
    in response. */
    pub async fn send_resp<RESP>(&self, resp: RESP) -> WSResult<()>
    where
        RESP: MsgPack + Default,
    {
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

/// 枚举，定义了用于消息分发的载荷类型，它可以是远程数据（Remote(Bytes)）或者是本地数据（Local(Box<dyn MsgPack>)）。
/* Enumeration, which defines the type of payload used for message distribution, which can be either Remote 
    data (Remote(Bytes)) or Local data (Local(Box<dyn MsgPack>)) */
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
    /* 根据节点的地址查找对应的节点ID。实现：遍历节点配置信息，查找与指定地址匹配的节点ID，并返回结果。 */
    /* Search for the node ID based on the node address. Implementation: Traverse the node configuration 
        information, find the node ID that matches the specified address, and return the result. */
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
    /* 将消息类型 M 对应的处理函数 f 注册到分发映射表中，以便接收到相应消息时能够调用对应的处理函数进行处理。 */
    /* The handler f corresponding to message type M is registered in the distribution mapping table so that the 
        corresponding handler function can be invoked for processing when receiving the corresponding message. */
    fn regist_dispatch<M, F>(&self, m: M, f: F)
    where
        M: MsgPack + Default,
        F: Fn(Responser, M) -> WSResult<()> + Send + Sync + 'static,
    {
        // self.p2p.regist_dispatch();
        // map 这里是一个获得了写权限的 hashmap，key是消息id，value是处理消息的函数。
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
                // tracing::debug!("dispatch from {} msg:{:?}", nid, msg);
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

    /* 注册了一个用于发送RPC请求的处理函数，当接收到RPC响应时，调用相应的回调函数发送响应给等待的任务。 */
    /* A handler for sending RPC requests is registered, and when an RPC response is received, the corresponding 
        callback function is called to send the response to the waiting task. */
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
    /* 注册了一个用于接收RPC请求的处理函数，当接收到RPC请求时，调用注册的处理函数进行处理。 */
    /* A handler for receiving RPC requests is registered, and when an RPC request is received, the 
        registered handler is invoked for processing. */
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
    
    /* 通过 P2P 核心节点向指定节点发送响应消息，并等待响应发送完成。 */
    /* The P2P core node sends a response message to the specified node, and waits for the response to complete. */
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

    /* 调用指定节点的RPC服务，并等待返回响应消息。 */
    /* Calls the RPC service of the specified node and waits for a response message to be returned. */
    #[inline]
    async fn call_rpc<R>(&self, node_id: NodeID, req: R, dur: Option<Duration>) -> WSResult<R::Resp>
    where
        R: RPCReq,
    {
        let dur = dur.unwrap_or(Duration::from_millis(500));
        self.call_rpc_inner::<R, R::Resp>(node_id, req, dur).await
    }


    /* 用于实际执行RPC调用的内部逻辑。它首先生成一个唯一的任务ID，然后创建一个单次通道（oneshot channel）用于接收响应消息。
    接着根据节点ID判断是本地调用还是远程调用，如果是本地调用，则直接调用分发函数进行处理；如果是远程调用，则通过P2P核心节点
    发送RPC请求消息，并等待一定时长（由dur参数指定）来接收响应消息。如果超时未收到响应，则返回错误。最后，将收到的响应消息
    反序列化为指定的响应类型并返回。 */
    /* The internal logic used to actually execute the RPC call. It first generates a unique task ID and then creates 
    a oneshot channel for receiving response messages. Then according to the node ID to determine whether the local 
    call or remote call, if the local call, directly call the distribution function for processing; In the case of a 
    remote call, an RPC request message is sent through the P2P core node and a certain amount of time (specified by 
    the dur parameter) is waited to receive the response message. If no response is received, an error is returned. 
    Finally, the received response message is deserialized to the specified response type and returned. */
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
            self.dispatch(
                node_id,
                r.msg_id(),
                taskid,
                DispatchPayload::Local(Box::new(r)),
            );
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
                tracing::error!("rpc send failed: {:?}", err);
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

                tracing::error!("rpc timeout: {:?} to node {}", err, node_id);

                // tracing::warn!("rpc timeout: {:?} to node {}", err, node_id);
                // tracing::info!("2stop holding lock msg:{} node:{}", r.msg_id(), node_id);
                return Err(WsNetworkConnErr::RPCTimout(node_id).into());
            }
        };

        let resp = resp.downcast::<RESP>().unwrap();

        Ok(*resp)
    }

    /* 根据消息ID查找注册的处理函数，并调用相应的处理函数进行消息处理。 */
    /* The registered handler is found based on the message ID and the corresponding handler is called for message processing. */
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
            tracing::warn!("not match id: {}", id);
            Err(WsNetworkLogicErr::MsgIdNotDispatchable(id).into())
        }
    }

    /* 根据节点ID在节点配置信息中查找对应的节点地址，并返回结果。 */
    /* The system searches for the node address in the node configuration information based on the node ID and returns the result. */
    pub fn get_addr_by_id(&self, id: NodeID) -> WSResult<SocketAddr> {
        self.nodes_config.peers.get(&id).map_or_else(
            || Err(WsNetworkLogicErr::InvaidNodeID(id).into()),
            |v| Ok(v.addr),
        )
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
