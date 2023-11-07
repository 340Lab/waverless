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

use parking_lot::Mutex;
use prost::{bytes::Bytes, Message};
use qp2p::{Connection, ConnectionIncoming, Endpoint, WireMsg};
use std::{
    collections::HashMap,
    env,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
    vec,
};
use tokio::{select, task::JoinHandle};
use ws_derive::LogicalModule;

use crate::{
    module_iter::*,
    module_state_trans::ModuleSignal,
    module_view::P2PQuicNodeLMView,
    result::{ErrCvt, WSResult},
    sys::{
        BroadcastMsg, BroadcastSender, LogicalModule, LogicalModuleNewArgs, LogicalModules, NodeID,
    },
    util::JoinHandleWrapper,
};

use super::p2p::P2P;

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
    shared_connection_map: tokio::sync::Mutex<HashMap<SocketAddr, ConnectionStuff>>,
}

#[derive(LogicalModuleParent, LogicalModule)]
pub struct P2PQuicNode {
    pub logical_modules_view: P2PQuicNodeLMView,
    shared: Arc<P2PQuicNodeShared>,
    name: String,
    peers: Vec<SocketAddr>,
    this_addr: SocketAddr,
}

impl LogicalModule for P2PQuicNode {
    fn inner_new(mut args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        let (peers, this) = args.peers_this.take().unwrap();
        Self {
            logical_modules_view: P2PQuicNodeLMView::new(),
            shared: P2PQuicNodeShared {
                btx: args.btx,
                locked: Mutex::new(P2PQuicNodeLocked { sub_tasks: vec![] }),
                shared_connection_map: HashMap::new().into(),
            }
            .into(),
            name: format!("{}::{}", args.parent_name, Self::self_name()),
            peers,
            this_addr: this,
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        // create an endpoint for us to listen on and send from.
        let (endpoint, mut incoming_conns) = Endpoint::builder()
            .addr(self.this_addr)
            .keep_alive_interval(Duration::from_millis(1000))
            // timeout of send or receive or connect, bigger than interval
            .idle_timeout(2 * 1_000 /* 3600s = 1h */)
            .server()
            .map_err(|err| ErrCvt(err).to_ws_network_conn_err())?;
        let view = self.logical_modules_view.clone();
        let shared = self.shared.clone();
        let this_addr = self.this_addr;

        let mut net_tasks: Vec<JoinHandleWrapper> = vec![];

        for peer in &self.peers {
            let endpoint = endpoint.clone();
            let addr = *peer;
            let shared = shared.clone();
            net_tasks.push(
                tokio::spawn(async move {
                    loop {
                        tracing::info!("try to connect to {}", addr);
                        let res = endpoint.connect_to(&addr).await;
                        match res {
                            Ok((connection, incoming)) => {
                                tracing::info!("connected to {}", addr);
                                handle_conflict_connection(
                                    &shared, &endpoint, connection, incoming,
                                )
                                .await;
                                tracing::info!("handled conflict_connection {}", addr);
                            }
                            Err(e) => {
                                tracing::error!(
                                    "connect to {} failed, error: {:?}, will retry",
                                    addr,
                                    e
                                );
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                })
                .into(),
            );
        }

        net_tasks.push(
        tokio::spawn(async move {
            let mut rx = shared.btx.subscribe();
            let _ = view.p2p().tx.send(ModuleSignal::Running);

            // fn select_handle_next_inco
            tracing::info!("start listening for new connections on node {}", this_addr);
            loop {
                tokio::select! {
                    next_incoming= incoming_conns.next() => {
                        if let Some((connection, incoming)) = next_incoming {
                            tracing::info!("recv connect from {}", connection.remote_address());
                            handle_conflict_connection(&shared, &endpoint, connection, incoming).await;
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

async fn handle_conflict_connection(
    shared: &Arc<P2PQuicNodeShared>,
    endpoint: &Endpoint,
    connection: Connection,
    mut incoming: ConnectionIncoming,
) {
    let (mut receiver, remote_addr) = {
        let remote_addr = connection.remote_address();
        let mut shared_connection_map = shared.shared_connection_map.lock().await;
        let entry = shared_connection_map
            .entry(connection.remote_address())
            .or_insert(ConnectionStuff {
                task_end_signal: None,
                conflict_connection: None,
            });
        let receiver = if let Some(conn) = entry.task_end_signal.as_ref() {
            entry.conflict_connection = Some((connection, incoming));
            conn.subscribe()
        } else {
            new_handle_connection_task(
                entry,
                shared.clone(),
                endpoint.clone(),
                connection,
                incoming,
            );

            entry.task_end_signal.as_ref().unwrap().subscribe()
        };
        (receiver, remote_addr)
    };

    receiver.recv().await;
    shared
        .shared_connection_map
        .lock()
        .await
        .remove(&remote_addr);
}

// async fn connect_to_others(endpoint: Endpoint,addr: SocketAddr){
//     let res=endpoint.connect_to(&addr).await
// }

fn new_handle_connection_task(
    connstuff: &mut ConnectionStuff,
    shared: Arc<P2PQuicNodeShared>,
    endpoint: Endpoint,
    connection: Connection,
    mut incoming: ConnectionIncoming,
) {
    let (tx, rx) = tokio::sync::broadcast::channel(10);
    connstuff.task_end_signal = Some(tx.clone());
    tokio::spawn(async move {
        handle_connection(shared, &endpoint, connection, incoming).await;

        tx.send(()).unwrap();
    });
}

async fn handle_connection(
    shared: Arc<P2PQuicNodeShared>,
    endpoint: &Endpoint,
    connection: Connection,
    mut incoming: ConnectionIncoming,
) {
    println!("\n---");
    println!("Listening on: {:?}", connection.remote_address());
    println!("---\n");

    let end = Arc::new(AtomicBool::new(false));

    // let handle = tokio::spawn(async move {
    let src = connection.remote_address();

    let send_sub = {
        let end = end.clone();
        tokio::spawn(async move {
            while !end.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_secs(1)).await;
                tracing::info!("sub send to {:?}", connection.remote_address());
                match connection
                    .send((Bytes::new(), Bytes::new(), Bytes::from("hello")))
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("send error: {:?}", e);
                        break;
                    }
                }
            }
            tracing::info!("sub send to {:?} end", connection.remote_address());
        })
    };

    loop {
        let res = incoming.next().await;
        match res {
            Ok(msg) => {
                if let Some(WireMsg((_, _, bytes))) = msg {
                    println!("Received from {src:?} --> {bytes:?}");
                    // if bytes == *MSG_MARCO {
                    //     let reply = Bytes::from(MSG_POLO);
                    //     connection
                    //         .send((Bytes::new(), Bytes::new(), reply.clone()))
                    //         .await?;
                    //     println!("Replied to {src:?} --> {reply:?}");
                    // }
                    println!();

                    // dispatch msgs
                } else {
                    tracing::warn!("incoming no msg");
                    break;
                }
            }
            Err(err) => {
                tracing::warn!("incoming error: {:?}", err);
            }
        }
    }
    // loop over incoming messages

    // });
    end.store(true, Ordering::Release);
    send_sub.await;
    println!("\n---");
    println!("End on: {:?}", src);
    println!("---\n");
    // shared.locked.lock().sub_tasks.push(handle);
}

#[async_trait]
impl P2P for P2PQuicNode {
    async fn send_for_response(&self, nodeid: NodeID, req_data: Vec<u8>) -> WSResult<Vec<u8>> {
        Ok(Vec::new())
    }
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
