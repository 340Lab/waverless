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

struct P2PQuicNodeShared {
    locked: Mutex<P2PQuicNodeLocked>,
    btx: BroadcastSender,
    // shared_connection_map: tokio::sync::Mutex<HashMap<SocketAddr, ConnectionStuff>>,
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

#[derive(LogicalModule)]
pub struct P2PQuicNode {
    pub logical_modules_view: View,
    shared: Arc<P2PQuicNodeShared>,
}

impl P2PQuicNode {
    fn p2p_base(&self) -> &P2PModule {
        self.logical_modules_view.p2p()
    }
}

#[async_trait]
impl LogicalModule for P2PQuicNode {
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

        for (n, n_config) in &self.p2p_base().nodes_config.peers {
            let endpoint = endpoint.clone();
            let addr = n_config.addr;
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


    let remote_id = view.p2p().find_peer_id(&remote_addr).unwrap_or_else(|| {
        panic!(
            "remote_addr {:?} not found in peer_id_map {:?}",
            remote_addr,
            view.p2p().nodes_config
        );
    });

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

    loop {
        let res = incoming.next().await;
        match res {
            Ok(msg) => {
                if let Some(WireMsg((head, _, bytes))) = msg {
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
    async fn send_for_response(&self, _nodeid: NodeID, _req_data: Vec<u8>) -> WSResult<Vec<u8>> {
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
