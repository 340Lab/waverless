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
use prost::{bytes::Bytes, Message};
use qp2p::{Connection, ConnectionIncoming, Endpoint, WireMsg};
use std::{
    collections::{HashMap, VecDeque},
    env,
    io::Read,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
    vec,
};
use tokio::{select, sync::Notify, task::JoinHandle};
use ws_derive::LogicalModule;

use crate::{
    module_iter::*,
    module_state_trans::ModuleSignal,
    module_view::P2PQuicNodeLMView,
    result::{ErrCvt, WSResult, WsNetworkConnErr, WsSerialErr},
    sys::{
        BroadcastMsg, BroadcastSender, LogicalModule, LogicalModuleNewArgs, LogicalModules, NodeID,
    },
    util::JoinHandleWrapper,
};

use super::p2p::{MsgId, P2PKernel, P2PModule, TaskId};

// #[derive(Default, Ord, PartialEq, PartialOrd, Eq, Clone, Copy)]
// struct XId(pub [u8; 32]);

type ConnectTaskId = usize;

struct P2PQuicNodeLocked {
    sub_tasks: Vec<JoinHandle<()>>,
}

struct ConnectionStuff {
    task_end_signal: Option<tokio::sync::broadcast::Sender<()>>,
    conflict_connection: Option<(Connection, ConnectionIncoming)>,
}

struct P2PQuicNodeShared {
    locked: Mutex<P2PQuicNodeLocked>,
    btx: BroadcastSender,
    // shared_connection_map: tokio::sync::Mutex<HashMap<SocketAddr, ConnectionStuff>>,
    peer_connections:
        RwLock<HashMap<SocketAddr, Arc<(tokio::sync::RwLock<Vec<Connection>>, AtomicUsize)>>>,
}

impl P2PQuicNodeShared {
    async fn reserve_peer_conn(&self, peer: SocketAddr) {
        if !self.peer_connections.read().contains_key(&peer) {
            self.peer_connections.write().insert(
                peer,
                Arc::new((tokio::sync::RwLock::new(Vec::new()), AtomicUsize::new(0))),
            );
        }
    }
}

#[derive(LogicalModuleParent, LogicalModule)]
pub struct P2PQuicNode {
    pub logical_modules_view: P2PQuicNodeLMView,
    shared: Arc<P2PQuicNodeShared>,
    name: String,
}

impl P2PQuicNode {
    fn p2p_base(&self) -> &P2PModule {
        self.logical_modules_view.p2p()
    }
}

#[async_trait]
impl LogicalModule for P2PQuicNode {
    fn inner_new(mut args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            logical_modules_view: P2PQuicNodeLMView::new(),
            shared: P2PQuicNodeShared {
                btx: args.btx,
                locked: Mutex::new(P2PQuicNodeLocked { sub_tasks: vec![] }),
                peer_connections: HashMap::new().into(),
            }
            .into(),
            name: format!("{}::{}", args.parent_name, Self::self_name()),
        }
    }
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        // create an endpoint for us to listen on and send from.
        let (endpoint, mut incoming_conns) = Endpoint::builder()
            .addr(self.p2p_base().this_node.0)
            .keep_alive_interval(Duration::from_millis(100))
            // timeout of send or receive or connect, bigger than interval
            .idle_timeout(2 * 1_000 /* 3600s = 1h */)
            .server()
            .map_err(|err| ErrCvt(err).to_ws_network_conn_err())?;

        let shared = self.shared.clone();
        let this_addr = self.p2p_base().this_node.0;

        let mut net_tasks: Vec<JoinHandleWrapper> = vec![];

        for (peer, n) in &self.p2p_base().peers {
            let endpoint = endpoint.clone();
            let addr = *peer;
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

                                handle_connection(
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
                                tracing::error!(
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
                tokio::select! {
                    next_incoming= incoming_conns.next() => {
                        if let Some((connection, incoming)) = next_incoming {
                            tracing::info!("recv connect from {}", connection.remote_address());
                            // handle_conflict_connection(&view,&shared, &endpoint, connection, incoming);
                            new_handle_connection_task(&view, shared.clone(), endpoint.clone(), connection, incoming);
                        }else{
                            // no more connections, system shutdown
                            shared.btx.send(BroadcastMsg::SysEnd).map_or_else(|v|v,|err|{panic!("rx is still here, error: {:?}",err)});
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

fn new_handle_connection_task(
    view: &P2PQuicNodeLMView,
    shared: Arc<P2PQuicNodeShared>,
    endpoint: Endpoint,
    connection: Connection,
    mut incoming: ConnectionIncoming,
) {
    let view = view.clone();
    tokio::spawn(async move {
        handle_connection(&view, shared, &endpoint, connection, incoming).await;
    });
}

async fn handle_connection(
    view: &P2PQuicNodeLMView,
    shared: Arc<P2PQuicNodeShared>,
    endpoint: &Endpoint,
    connection: Connection,
    mut incoming: ConnectionIncoming,
) {
    println!("\n---");
    println!("Listening on: {:?}", connection.remote_address());
    println!("---\n");

    // let end = Arc::new(AtomicBool::new(false));

    // let handle = tokio::spawn(async move {
    let remote_addr = connection.remote_address();
    let remote_id = view
        .p2p()
        .peers
        .iter()
        .find(|v| v.0 == remote_addr)
        .unwrap()
        .1;

    shared.reserve_peer_conn(remote_addr).await;
    let peer_conns = shared
        .peer_connections
        .read()
        .get(&remote_addr)
        .unwrap()
        .clone();
    let conn_id = connection.id();
    peer_conns.0.write().await.push(connection);

    loop {
        let res = incoming.next().await;
        match res {
            Ok(msg) => {
                if let Some(WireMsg((head, _, bytes))) = msg {
                    match deserialize_msg_id_task_id(&head) {
                        Ok((msg_id, task_id)) => {
                            if view.p2p().this_node.1 == 3 && remote_id == 1 {
                                // println!("Received from {remote_addr:?} --> {bytes:?}");
                            }
                            // println!("Received from {remote_addr:?} --> {bytes:?}");
                            view.p2p().dispatch(remote_id, msg_id, task_id, bytes);
                            // if bytes == *MSG_MARCO {
                            //     let reply = Bytes::from(MSG_POLO);
                            //     connection
                            //         .send((Bytes::new(), Bytes::new(), reply.clone()))
                            //         .await?;
                            //     println!("Replied to {src:?} --> {reply:?}");
                            // }
                            // println!();
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

    // loop over incoming messages

    // });
    // end.store(true, Ordering::Release);
    // send_sub.await;
    println!("\n---");
    println!("End on: {:?}", remote_addr);
    println!("---\n");
    // shared.locked.lock().sub_tasks.push(handle);
}

fn deserialize_msg_id_task_id(head: &[u8]) -> WSResult<(MsgId, TaskId)> {
    let (msg_id, task_id) = bincode::deserialize::<(MsgId, TaskId)>(head)
        .map_err(|err| WsSerialErr::BincodeErr(err))?;
    Ok((msg_id, task_id))
}
fn serialize_msg_id_task_id(msg_id: MsgId, task_id: TaskId) -> Bytes {
    let head: Vec<u8> = bincode::serialize(&(msg_id, task_id)).unwrap();
    Bytes::from(head)
}

#[async_trait]
impl P2PKernel for P2PQuicNode {
    async fn send_for_response(&self, nodeid: NodeID, req_data: Vec<u8>) -> WSResult<Vec<u8>> {
        Ok(Vec::new())
    }
    async fn send(
        &self,
        node: NodeID,
        task_id: TaskId,
        msg_id: MsgId,
        req_data: Vec<u8>,
    ) -> WSResult<()> {
        let addr = self.p2p_base().get_addr_by_id(node)?;

        let peerconns = {
            let hold = self.shared.peer_connections.read();
            hold.get(&addr).map(|v| v.clone())
        };
        if let Some(peer_conns) = peerconns {
            // round robin
            let all_count = peer_conns.0.read().await.len();
            for _ in 0..all_count {
                // length might change
                let reading_conns = peer_conns.0.read().await;
                let idx = peer_conns.1.fetch_add(1, Ordering::Relaxed) % reading_conns.len();

                let bytes = unsafe {
                    let dataref = req_data.as_ptr();
                    // transfer to static slice
                    let data = std::slice::from_raw_parts::<'static>(dataref, req_data.len());
                    Bytes::from(data)
                };

                if let Err(err) = reading_conns[idx]
                    .send((
                        serialize_msg_id_task_id(msg_id, task_id),
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
