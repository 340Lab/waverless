//! This example demonstrates accepting connections and messages
//! on a socket/port and replying on the same socket/port using a
//! bidirectional stream.
//!
//! We implement a simple P2P node that listens for incoming messages
//! from an arbitrary number of peers. If a peer sends us "marco" we reply
//! with "polo".
//!
//! Our node accepts a list of SocketAddr for peers on the command-line.
//! Upon startup, we send "marco" to each peer in the list and print
//! the reply.  If the list is empty, we don't send any message.
//!
//! We then proceed to listening for new connections/messages.


use async_trait::async_trait;

use parking_lot::{Mutex, RwLock};
use prost::bytes::Bytes;
use qp2p::{Connection, ConnectionIncoming, Endpoint, WireMsg};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
    vec,
};
use tokio::task::JoinHandle;
use ws_derive::LogicalModule;

use crate::{
    // module_view::P2PQuicNodeLMView,
    logical_module_view_impl, result::{ErrCvt, WSResult, WsNetworkConnErr, WsSerialErr}, sys::{LogicalModulesRef,BroadcastMsg, BroadcastSender, LogicalModule, LogicalModuleNewArgs, NodeID}, util::JoinHandleWrapper
};

use super::m_p2p::{MsgId, P2PKernel, P2PModule, TaskId};

// #[derive(Default, Ord, PartialEq, PartialOrd, Eq, Clone, Copy)]
// struct XId(pub [u8; 32]);

struct P2PQuicNodeLocked {
    sub_tasks: Vec<JoinHandle<()>>,
}

/* 用于存储P2PQuicNode节点的共享数据，包括对等连接、广播发送器等。它提供了一些函数来管理对等节点之间的连接。 */
/* Used to store shared data of P2PQuicNode nodes, including peer connections and broadcast transmitters. 
    It provides functions to manage connections between peers. */
struct P2PQuicNodeShared {

    locked: Mutex<P2PQuicNodeLocked>,

    /* 广播发送器，用于向所有连接的对等节点发送消息。 */
    /* A broadcast transmitter that sends messages to all connected peers. */
    btx: BroadcastSender,
    // shared_connection_map: tokio::sync::Mutex<HashMap<SocketAddr, ConnectionStuff>>,

    /* 保存与每个对等节点（peer）的连接信息的哈希映射，其中SocketAddr作为键，值包括一个读写锁，保护对等节点的连接向量，
        每个连接对应一个Connection实例。还有一个原子整数，用于维护轮询索引和活动连接数，避免了对连接向量的频繁锁定。 */
    /* Saves a hash map of the Connection information to each peer, where SocketAddr is the key and the value 
    includes a read/write lock that protects the peer's connection vector, with each connection corresponding 
    to a connection instance. There is also an atomic integer that maintains the polling index and the number 
    of active connections, avoiding frequent locking of the join vector. */
    peer_connections: RwLock<
        HashMap<
            SocketAddr,
            Arc<(
                // all the active connections to this peer
                tokio::sync::RwLock<Vec<Connection>>,
                // the round robin index for the above vector
                AtomicUsize,
                // the number of active connections to this peer, avoid locking the above vector to call len()
                AtomicUsize,
            )>,
        >,
    >,
}

impl P2PQuicNodeShared {

    /* 预留给定对等节点的连接资源。首先检查给定对等节点是否已经存在于连接映射中，如果不存在，则创建一个新的连接信息并插入
        到映射中。以确保每个对等节点都有一个对应的连接信息，便于后续进行连接管理。 */
    /* Reserve connection resources for the specified peer node. First check whether the given peer already exists 
    in the connection map, and if not, create a new connection information and insert it into the map. To ensure 
    that each peer node has a corresponding connection information, facilitate subsequent connection management. */
    async fn reserve_peer_conn(&self, peer: SocketAddr) {
        if !self.peer_connections.read().contains_key(&peer) {
            let _ = self.peer_connections.write().insert(
                peer,
                Arc::new((
                    tokio::sync::RwLock::new(Vec::new()),
                    AtomicUsize::new(0),
                    AtomicUsize::new(0),
                )),
            );
        }
    }
}

logical_module_view_impl!(View);
logical_module_view_impl!(View, p2p, P2PModule);


/// 用于表示基于QUIC协议的P2P节点，其中包含了P2P节点的视图和共享资源。
/// It is used to represent a P2P node based on the QUIC protocol, which contains the view and shared resources of the P2P node.
#[derive(LogicalModule)]
pub struct P2PQuicNode {
    /* P2P节点的视图，包含了P2P模块的信息，如节点配置、对等节点等。 */
    /* The view of a P2P node contains information about the P2P module, such as node configuration and peer nodes. */
    pub logical_modules_view: View,
    /* Arcs pointing to shared resources are wrapped with Arc smart Pointers to ensure secure access in multithreaded environments. */
    shared: Arc<P2PQuicNodeShared>,
}


impl P2PQuicNode {
    /* 返回P2P节点中的P2P模块的引用，通过视图获取到P2P模块，以便后续进行基于P2P模块的操作。 */
    /* Returns the reference of the P2P module in the P2P node, and obtains the P2P module from the view to facilitate 
        subsequent operations based on the P2P module. */
    fn p2p_base(&self) -> &P2PModule {
        self.logical_modules_view.p2p()
    }
}

#[async_trait]
impl LogicalModule for P2PQuicNode {

    /* 用于创建新的 P2PQuicNode 实例。初始化节点，包括创建视图和共享资源，并返回初始化后的节点实例。 */
    /* Used to create a new P2PQuicNode instance. Initializes the node, including creating views and 
        shared resources, and returns the initialized node instance. */
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            logical_modules_view: View::new(args.logical_modules_ref.clone()),
            shared: P2PQuicNodeShared {
                btx: args.btx,
                locked: Mutex::new(P2PQuicNodeLocked { sub_tasks: vec![] }),
                peer_connections: HashMap::new().into(),
            }
            .into(),
            // name: format!("{}::{}", args.parent_name, Self::self_name()),
        }
    }

    /* 用于启动节点，包括建立连接、监听新连接等操作 */
    /* It is used to start a node, including establishing connections and listening for new connections */
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let this_addr = self.p2p_base().nodes_config.this.1.addr;
        // create an endpoint for us to listen on and send from.
        let (endpoint, mut incoming_conns) = Endpoint::builder()
            .addr(SocketAddr::new(
                IpAddr::from_str("0.0.0.0").unwrap(),
                this_addr.port(),
            ))
            .keep_alive_interval(Duration::from_millis(100))
            // timeout of send or receive or connect, bigger than interval
            .idle_timeout(2 * 1_000 /* 3600s = 1h */)
            .server()
            .map_err(|err| {
                tracing::error!("addr: {}, err: {}", this_addr, err);
                ErrCvt(err).to_ws_network_conn_err()
            })?;

        let shared = self.shared.clone();

        let mut net_tasks: Vec<JoinHandleWrapper> = vec![];
        
        /* 遍历配置文件中的每个节点，并为它们创建一个主动连接任务。对于每个节点，节点将尝试连接，如果成功建立连接，
            则发送本节点的地址信息，并处理连接后续的消息。 */
        for (n, n_config) in &self.p2p_base().nodes_config.peers {
            let endpoint = endpoint.clone();
            let addr = n_config.addr;   // 配置文件中给出的地址
            let shared = shared.clone();
            let view = self.logical_modules_view.clone();
            let n = n.clone();
            net_tasks.push(
                tokio::spawn(async move {
                    loop {
                        tracing::info!("try to connect to {}", n);
                        let res = endpoint.connect_to(&addr).await;
                        match res {
                            Ok((connection, incoming)) => {
                                tracing::info!("connected to {}", addr);
                                let _=connection.send((Bytes::new(),Bytes::new(), Bytes::from(this_addr.to_string()))).await;
                                // send self addr
                                handle_connection(
                                    addr,
                                    &view,
                                    shared.clone(),
                                    &endpoint,
                                    connection,
                                    incoming,
                                )
                                .await;
                                // tracing::info!("handled conflict_connection {}", addr);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "connect to {} failed, error: {:?}, will retry",
                                    addr,
                                    e
                                );
                                tokio::time::sleep(Duration::from_secs(10)).await;
                            }
                        }
                    }
                })
                .into(),
            );
        }


        let view = self.logical_modules_view.clone();
        net_tasks.push(
        tokio::spawn(async move {
            let mut rx = shared.btx.subscribe();
            // fn select_handle_next_inco
            tracing::info!("start listening for new connections on node {}", this_addr);
            loop {
                /* 使用 tokio::select! 宏，同时监听新连接和节点内部的消息通道，以便处理新连接和系统消息（如系统关闭）。 */
                /* Use tokio::select! Macro that listens for both new connections and message channels inside the node 
                    to process new connections and system messages (such as system shutdown). */
                tokio::select! {
                    next_incoming= incoming_conns.next() => {
                        if let Some((connection, mut incoming)) = next_incoming {
                            let res = incoming.next().await;
                            
                            match res {
                                Ok(msg) => {
                                    if let Some(WireMsg((_head, _, bytes))) = msg {
                                        let addr=String::from_utf8(bytes.to_vec()).unwrap().parse::<SocketAddr>().unwrap();
                                        tracing::info!("recv connect from {}", addr);

                                        // handle_conflict_connection(&view,&shared, &endpoint, connection, incoming);
                                        new_handle_connection_task(addr,&view, shared.clone(), endpoint.clone(), connection, incoming);
                                    }else{
                                        tracing::info!("didn't recv head"); 
                                        continue;   
                                    }
                                }
                                Err(err)=>{
                                    tracing::info!("recv head error {:?}",err);
                                    continue;
                                }
                            }
                        }else{
                            // no more connections, system shutdown
                            let _ =shared.btx.send(BroadcastMsg::SysEnd).unwrap_or_else(|err|{
                                panic!("send to btx failed, error: {:?}",err);
                            });
                        }
                    }
                    received=rx.recv() => {
                        let msg=match received{
                            Ok(msg) => msg,
                            Err(e) => {
                                // system shutdown
                                panic!("tx should live longer than rx, error: {:?}",e);
                            }
                        };

                        match msg{
                            /* 当系统接收到系统关闭消息时，停止监听新的连接请求，结束循环，完成节点的启动过程。 */
                            /* When the system receives the system shutdown message, the system stops listening for 
                                new connection requests, ends the loop, and completes the node startup process. */
                            BroadcastMsg::SysEnd => {
                                // system shutdown
                                break;
                            },
                        }
                    }
                }
            }
        }).into());

        Ok(net_tasks)
    }
}

// fn handle_conflict_connection(
//     view: &P2PQuicNodeLMView,
//     shared: &Arc<P2PQuicNodeShared>,
//     endpoint: &Endpoint,
//     connection: Connection,
//     mut incoming: ConnectionIncoming,
// ) {
//     let remote_addr = connection.remote_address();
//     // let mut shared_connection_map = shared.shared_connection_map.lock().await;
//     // let entry = shared_connection_map
//     //     .entry(connection.remote_address())
//     //     .or_insert(ConnectionStuff {
//     //         task_end_signal: None,
//     //         conflict_connection: None,
//     //     });
//     // let receiver = if let Some(conn) = entry.task_end_signal.as_ref() {
//     //     entry.conflict_connection = Some((connection, incoming));
//     //     conn.subscribe()
//     // } else {
//     let view = view.clone();
//     let shared = shared.clone();
//     let endpoint = endpoint.clone();
//     tokio::spawn(
//         async move { block_handle_connection(view, shared, endpoint, connection, incoming) },
//     );
// }

// async fn block_handle_connection(
//     view: P2PQuicNodeLMView,
//     shared: Arc<P2PQuicNodeShared>,
//     endpoint: Endpoint,
//     connection: Connection,
//     mut incoming: ConnectionIncoming,
//     c2: Connection,
//     ic2: ConnectionIncoming,
// ) {
//     let remote_addr = connection.remote_address();
//     // let res = shared
//     //     .conflict_handle
//     //     .lock()
//     //     .await
//     //     .entry(remote_addr)
//     //     .or_insert(Arc::new(tokio::sync::Mutex::new(())))
//     //     .clone();
//     // let hold = res.lock().await;
//     tracing::info!("handle connection to {}", remote_addr);
//     handle_connection(&view, shared, &endpoint, connection, incoming, c2, ic2).await;
// }

// receiver.recv().await;
// shared
//     .shared_connection_map
//     .lock()
//     .await
//     .remove(&remote_addr);

// async fn connect_to_others(endpoint: Endpoint,addr: SocketAddr){
//     let res=endpoint.connect_to(&addr).await
// }

/* 创建一个新的异步任务来处理连接。接受参数包括远程地址 remote_addr、节点视图 view、共享节点信息 shared、
    端点 endpoint、连接 connection 以及传入连接 incoming。 */
/* Create a new asynchronous task to handle the connection. Accepted parameters include remote address remote_addr, 
    node view, shared node information shared, endpoint endpoint, connection connection, and incoming connection. */
fn new_handle_connection_task(
    remote_addr: SocketAddr,
    view: &View,
    shared: Arc<P2PQuicNodeShared>,
    endpoint: Endpoint,
    connection: Connection,
    incoming: ConnectionIncoming,
) {
    let view = view.clone();
    shared
        .clone()
        .locked
        .lock()
        .sub_tasks
        .push(tokio::spawn(async move {
            handle_connection(remote_addr,&view, shared, &endpoint, connection, incoming).await;
        }));
}

/* 处理与其他节点的连接。 */
/* Handles connections to other nodes */
async fn handle_connection(
    remote_addr: SocketAddr,
    view: &View,
    shared: Arc<P2PQuicNodeShared>,
    _endpoint: &Endpoint,
    connection: Connection,
    mut incoming: ConnectionIncoming,
) {
    println!("\n---");
    println!("Listening on: {:?}", remote_addr);
    println!("---\n");

    // 根据远程地址在节点视图中查找对应的节点ID。
    // Locate the node ID in the node view based on the remote address.
    let remote_id = view.p2p().find_peer_id(&remote_addr).unwrap_or_else(|| {
        panic!(
            "remote_addr {:?} not found in peer_id_map {:?}",
            remote_addr,
            view.p2p().nodes_config
        );
    });

    /* 调用共享节点信息中的 reserve_peer_conn 函数来为远程节点保留连接。从共享节点信息中获取与远程地址对应的连接信息，
        并将当前连接添加到连接列表中。 */
    /* Call the reserve_peer_conn function in the shared node information to reserve the connection for the 
    remote node. Obtain the connection information corresponding to the remote address from the shared node 
    information and add the current connection to the connection list. */
    shared.reserve_peer_conn(remote_addr).await;
    let peer_conns = shared
        .peer_connections
        .read()
        .get(&remote_addr)
        .unwrap()
        .clone();
    let conn_id = connection.id();
    peer_conns.0.write().await.push(connection);
    let _ = peer_conns.2.fetch_add(1, Ordering::Relaxed);


    /* 使用循环不断接收传入的消息，解析消息头中的消息ID和任务ID，并调用节点视图中的 dispatch 函数来分发消息。处理完毕后，
        将当前连接从连接列表中移除，并更新连接数量。 */
    /* Use a loop to continuously receive incoming messages, parse the message ID and task ID in the message header, 
    and call the dispatch function in the node view to distribute the message. After processing is complete, remove 
    the current connection from the connection list and update the number of connections. */
    loop {
        let res = incoming.next().await;
        match res {
            Ok(msg) => {
                if let Some(WireMsg((_, _, mut bytes))) = msg {
                    let headlen=bytes.split_to(1)[0];
                    let head=bytes.split_to(headlen as usize);
                    match deserialize_msg_id_task_id(&head) {
                        Ok((msg_id, task_id)) => {
                            view.p2p().dispatch(remote_id, msg_id, task_id, bytes.into());
                        }
                        Err(err) => {
                            tracing::warn!("incoming deserial head error: {:?}", err);
                        }
                    }
                    // dispatch msgs
                } else {
                    tracing::warn!("incoming no msg");
                    break;
                }
            }
            Err(err) => {
                tracing::warn!("incoming error: {:?}", err);
                break;
            }
        }
    }

    peer_conns.0.write().await.retain(|v| v.id() != conn_id);
    let _ = peer_conns.2.fetch_sub(1, Ordering::Relaxed);

    // loop over incoming messages

    // });
    // end.store(true, Ordering::Release);
    // send_sub.await;
    println!("\n---");
    println!("End on: {:?}", remote_addr);
    println!("---\n");
    // shared.locked.lock().sub_tasks.push(handle);
}

/// 将字节流解析为消息ID和任务ID。
/// Parse the byte stream into a message ID and a task ID.
fn deserialize_msg_id_task_id(head: &[u8]) -> WSResult<(MsgId, TaskId)> {
    let (msg_id, task_id) = bincode::deserialize::<(MsgId, TaskId)>(head)
        .map_err(|err| WsSerialErr::BincodeErr(err))?;
    Ok((msg_id, task_id))
}
/// 将消息ID和任务ID序列化为字节流。
/// Serialize the message ID and task ID to a byte stream.
fn serialize_msg_id_task_id(msg_id: MsgId, task_id: TaskId) -> Vec<u8> {
    let mut head: Vec<u8> = bincode::serialize(&(msg_id, task_id)).unwrap();
    head.insert(0, head.len() as u8);
    head
}

#[async_trait]
impl P2PKernel for P2PQuicNode {

    async fn send_for_response(&self, _nodeid: NodeID, _req_data: Vec<u8>) -> WSResult<Vec<u8>> {
        Ok(Vec::new())
    }
    
    /* 发送请求给指定节点。 */
    /* Sends the request to the specified node. */
    async fn send(
        &self,
        node: NodeID,
        task_id: TaskId,
        msg_id: MsgId,
        req_data: Vec<u8>,
    ) -> WSResult<()> {
        // 获取目标节点的Socket地址。
        // Gets the Socket address of the target node.
        let addr = self.p2p_base().get_addr_by_id(node)?;

        // 从 peer_connections 中获取与目标节点建立的连接信息。
        // Gets information about connections made to the target node from peer_connections.
        let peerconns = {
            let hold = self.shared.peer_connections.read();
            hold.get(&addr).map(|v| v.clone())
        };
        if let Some(peer_conns) = peerconns {
            // round robin
            /* 选择一个连接，并将请求数据发送给目标节点。如果发送失败，记录错误信息并继续尝试其他连接，直到所有连接尝试完毕。
                如果所有连接都失败，则返回相应的错误信息。 */
            /* Select a connection and send the request data to the target node. If sending fails, record an error 
            message and continue trying other connections until all connection attempts are complete. If all connections 
            fail, the corresponding error message is returned. */
            let all_count = peer_conns.2.load(Ordering::Relaxed);
            for _ in 0..all_count {
                // length might change
                let reading_conns = peer_conns.0.read().await;
                if reading_conns.len() == 0 {
                    break;
                }
                let idx = peer_conns.1.fetch_add(1, Ordering::Relaxed) % reading_conns.len();

                let bytes = unsafe {
                    let dataref = req_data.as_ptr();
                    // transfer to static slice
                    let data = std::slice::from_raw_parts::<'static>(dataref, req_data.len());
                    let mut v=serialize_msg_id_task_id(msg_id, task_id);
                    v.extend_from_slice(data);
                    Bytes::from(v)
                };

                if let Err(err) = reading_conns[idx]
                    .send((
                        Bytes::new(),
                        Bytes::new(),
                        bytes,
                        // Bytes::new(),
                    ))
                    .await
                {
                    tracing::error!("peer conn {:?} send error: {:?}", addr, err);
                    continue;
                }

                return Ok(());
            }

            // if let Some(r) = peer_conn.close_reason() {
            //     tracing::error!("peer conn {:?} closed for {:?} when send", addr, r);
            // }
            // peer_conn
            //     .send((
            //         serialize_msg_id_task_id(msg_id, task_id),
            //         Bytes::new(),
            //         Bytes::from(req_data),
            //     ))
            //     .await
            //     .map_err(|err| ErrCvt(err).to_ws_network_conn_err())?;
            // tracing::info!("active peer conn count {:?}", all_count);
            Err(WsNetworkConnErr::ConnectionExpired(node).into())
        } else {
            Err(WsNetworkConnErr::ConnectionNotEstablished(node).into())
        }
    }
    // async fn close(&self, node: NodeID) -> WSResult<()> {
    //     let peer_connections = self.shared.peer_connections.read().await;
    //     let addr = self.p2p_base().get_addr_by_id(node)?;
    //     if let Some(peer_conn) = peer_connections.get(&addr) {
    //         peer_conn.close(Some("close by p2p kernel".to_owned()));
    //         Ok(())
    //     } else {
    //         Err(WsNetworkConnErr::ConnectionNotEstablished(node).into())
    //     }
    // }
}

// #[tokio::main]
// async fn main() -> Result<()> {
//     const MSG_MARCO: &str = "marco";
//     const MSG_POLO: &str = "polo";

//     // collect cli args
//     let args: Vec<String> = env::args().collect();

//     // // if we received args then we parse them as SocketAddr and send a "marco" msg to each peer.
//     // if args.len() > 1 {
//     //     for arg in args.iter().skip(1) {
//     //         let peer: SocketAddr = arg
//     //             .parse()
//     //             .expect("Invalid SocketAddr.  Use the form 127.0.0.1:1234");
//     //         let msg = Bytes::from(MSG_MARCO);
//     //         println!("Sending to {peer:?} --> {msg:?}\n");
//     //         let (conn, mut incoming) = node.connect_to(&peer).await?;
//     //         conn.send((Bytes::new(), Bytes::new(), msg.clone())).await?;
//     //         // `Endpoint` no longer having `connection_pool` to hold established connection.
//     //         // Which means the connection get closed immediately when it reaches end of life span.
//     //         // And causes the receiver side a sending error when reply via the in-coming connection.
//     //         // Hence here have to listen for the reply to avoid such error
//     //         let reply = incoming.next().await.unwrap();
//     //         println!("Received from {peer:?} --> {reply:?}");
//     //     }

//     //     println!("Done sending");
//     // }

//     Ok(())
// }
