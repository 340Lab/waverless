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
    sync::Arc,
    vec,
};
use tokio::task::JoinHandle;

use crate::{
    result::{ErrCvt, WSResult},
    sys::{
        BroadcastMsg, BroadcastSender, LogicalModule, LogicalModuleNewArgs, LogicalModules, NodeID,
    },
};

use super::p2p::P2P;

// #[derive(Default, Ord, PartialEq, PartialOrd, Eq, Clone, Copy)]
// struct XId(pub [u8; 32]);

type ConnectTaskId = usize;

struct P2PQuicNodeLocked {
    sub_tasks: Vec<JoinHandle<()>>,
}

struct P2PQuicNodeShared {
    locked: Mutex<P2PQuicNodeLocked>,
    btx: BroadcastSender,
}

pub struct P2PQuicNode {
    shared: Arc<P2PQuicNodeShared>,
}

async fn handle_connection(
    shared: Arc<P2PQuicNodeShared>,
    endpoint: &Endpoint,
    connection: Connection,
    mut incoming: ConnectionIncoming,
) {
    println!("\n---");
    println!("Listening on: {:?}", endpoint.local_addr());
    println!("---\n");

    let handle = tokio::spawn(async move {
        let src = connection.remote_address();

        // loop over incoming messages
        while let Ok(Some(WireMsg((_, _, bytes)))) = incoming.next().await {
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
        }
    });

    shared.locked.lock().sub_tasks.push(handle);
}

impl LogicalModule for P2PQuicNode {
    fn new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            shared: P2PQuicNodeShared {
                btx: args.btx,
                locked: Mutex::new(P2PQuicNodeLocked { sub_tasks: vec![] }),
            }
            .into(),
        }
    }
    fn start(&self) -> WSResult<Vec<JoinHandle<()>>> {
        // create an endpoint for us to listen on and send from.
        let (endpoint, mut incoming_conns) = Endpoint::builder()
            .addr((Ipv4Addr::LOCALHOST, 0))
            .idle_timeout(60 * 60 * 1_000 /* 3600s = 1h */)
            .server()
            .map_err(|err| ErrCvt(err).to_ws_network_conn_err())?;

        let shared = self.shared.clone();
        let listen_for_new_conn = tokio::spawn(async move {
            let mut rx = shared.btx.subscribe();

            // fn select_handle_next_inco

            loop {
                tokio::select! {
                    next_incoming= incoming_conns.next() => {
                        if let Some((connection, incoming)) = next_incoming {
                            handle_connection(shared.clone(), &endpoint, connection, incoming).await;
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
        });

        Ok(vec![listen_for_new_conn])
    }
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
