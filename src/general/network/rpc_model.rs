use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use lazy_static::lazy_static;
use parking_lot::RwLock;
use prost::Message;
use std::{
    collections::HashMap,
    hash::Hash,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};
use tokio::{net::UnixListener, sync::oneshot};

use crate::result::{WSResult, WsRpcErr};

// start from the begining
#[async_trait]
pub trait RpcCustom: Sized + 'static {
    fn spawn() -> tokio::task::JoinHandle<()> {
        tokio::spawn(accept_task::<Self>())
    }
    fn bind() -> UnixListener;
    async fn verify(buf: &[u8]) -> Option<HashValue>;
    // fn deserialize(id: u16, buf: &[u8]);
}

async fn accept_task<R: RpcCustom>() {
    // std::fs::remove_file(AGENT_SOCK_PATH).unwrap();

    // clean_sock_file(AGENT_SOCK_PATH);
    // let listener = tokio::net::UnixListener::bind(AGENT_SOCK_PATH).unwrap();

    let listener = R::bind();
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let _ = tokio::spawn(listen_task::<R>(socket));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HashValue {
    Int(i64),
    Str(String),
}

pub trait MsgIdBind: Sized + Message + Default + 'static {
    fn id() -> u16;
}

pub trait ReqMsg: MsgIdBind {
    type Resp: MsgIdBind;
}

// server - client model rpc, server just wait for connection
// return false is is starting
pub fn start_remote_once(conn: HashValue) -> bool {
    let mut conn_map = CONN_MAP.write();
    if conn_map.contains_key(&conn) {
        return false;
    }
    let _ = conn_map.insert(conn.clone(), ConnState::Connecting(Vec::new()));
    let _ = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(8)).await;
        let mut conn_map = CONN_MAP.write();
        let cancel = if let Some(&ConnState::Connecting(_)) = conn_map.get(&conn) {
            true
        } else {
            false
        };
        if cancel {
            tracing::warn!("failed to conn to process rpc {:?}", conn);
            let _ = conn_map.remove(&conn);
        }
    });
    true
}

pub async fn call<Req: ReqMsg>(
    req: Req,
    conn: HashValue,
    timeout: Duration,
) -> WSResult<Req::Resp> {
    // wait for connection if not connected

    let mut req_rx_opt = None;
    let mut sender_opt = None;
    {
        let mut conn_map = CONN_MAP.write();
        match conn_map.get_mut(&conn) {
            Some(ConnState::Connecting(waiters)) => {
                let (req_tx, req_rx) = oneshot::channel();
                waiters.push(req_tx);

                req_rx_opt = Some(req_rx)
            }
            None => {
                return Err(WsRpcErr::ConnectionNotEstablished(conn).into());

                // // Register a new connection in Connecting state
                // let (req_tx, req_rx) = oneshot::channel();
                // let _ = conn_map.insert(conn.clone(), ConnState::Connecting(vec![req_tx]));

                // req_rx_opt = Some(req_rx)
            }
            Some(ConnState::Connected(sender)) => {
                // Directly return the existing sender in the connected state
                sender_opt = Some(sender.clone());
            }
        }
    };

    let tx = if let Some(req_rx) = req_rx_opt {
        let tx_might = req_rx.await;
        match tx_might {
            Err(_err) => {
                tracing::warn!("tx is removed because verification failed");
                return Err(WsRpcErr::ConnectionNotEstablished(conn).into());
            }
            Ok(res) => res,
        }
    } else {
        sender_opt.unwrap()
    };

    // register the call back
    let (wait_tx, wait_rx) = oneshot::channel();
    let next_task = NEXT_TASK_ID.fetch_add(1, Ordering::SeqCst);
    let _ = CALL_MAP.write().insert(next_task, wait_tx);

    // send the request
    tracing::debug!("send request: {:?}", req);
    tx.send(req.encode_to_vec()).await.unwrap();

    // if timeout, take out the registered channel tx, return error
    let channel_resp = match tokio::time::timeout(timeout, wait_rx).await {
        Ok(ok) => ok,
        Err(_) => {
            //remove the cb channel
            tracing::debug!("call timeout: {:?}", req);
            assert!(CALL_MAP.write().remove(&next_task).is_some());
            return Err(WsRpcErr::RPCTimout(conn).into());
        }
    }
    .expect("cb channel must has a value if not timeout");

    match Req::Resp::decode(&mut channel_resp.as_slice()) {
        Ok(ok) => Ok(ok),
        Err(_err) => Err(WsRpcErr::InvalidMsgData { msg: Box::new(req) }.into()),
    }
}

// pub enum ConnState {
//     Connecting,
//     Connected(tokio::sync::mpsc::Sender<()>),
//     Disconnected,
// }

#[derive(EnumAsInner)]
enum ConnState {
    /// record the waiters
    Connecting(Vec<oneshot::Sender<tokio::sync::mpsc::Sender<Vec<u8>>>>),
    Connected(tokio::sync::mpsc::Sender<Vec<u8>>),
}

lazy_static! {
    /// This is an example for using doc comment attributes
    static ref CONN_MAP: RwLock<HashMap<HashValue,ConnState>> = RwLock::new(HashMap::new());

    static ref CALL_MAP: RwLock<HashMap<u32,oneshot::Sender<Vec<u8>>>> = RwLock::new(HashMap::new());

    static ref NEXT_TASK_ID: AtomicU32 = AtomicU32::new(0);
}

async fn listen_task<R: RpcCustom>(socket: tokio::net::UnixStream) {
    println!("new connection: {:?}", socket.peer_addr().unwrap());
    let (mut sockrx, socktx) = socket.into_split();

    let mut buf = [0; 1024];
    let mut len = 0;

    let Some(rx) = listen_task_ext::verify_remote::<R>(&mut sockrx, &mut len, &mut buf).await
    else {
        println!("verify failed");
        return;
    };

    listen_task_ext::spawn_send_loop(rx, socktx);

    listen_task_ext::read_loop(&mut sockrx, &mut len, &mut buf).await;
}

pub(super) mod listen_task_ext {
    use bytes::{Buf, Bytes};
    use prost::bytes;
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::unix::{OwnedReadHalf, OwnedWriteHalf},
        sync::mpsc::Receiver,
    };

    use crate::general::network::rpc_model::ConnState;

    use super::{RpcCustom, CONN_MAP};

    pub(super) async fn verify_remote<R: RpcCustom>(
        sockrx: &mut OwnedReadHalf,
        len: &mut usize,
        buf: &mut [u8],
    ) -> Option<Receiver<Vec<u8>>> {
        async fn verify_remote_inner<R: RpcCustom>(
            sockrx: &mut OwnedReadHalf,
            len: &mut usize,
            buf: &mut [u8],
        ) -> Option<Receiver<Vec<u8>>> {
            // println!("waiting for verify head len");
            if !wait_for_len(sockrx, len, 4, buf).await {
                println!("failed to read verify head len");
                return None;
            }

            let verify_msg_len = consume_len(0, buf, len);

            // println!("waiting for verify msg {}", verify_msg_len);
            if !wait_for_len(sockrx, len, verify_msg_len, buf).await {
                println!("failed to read verify msg");
                return None;
            }
            // println!("wait done");

            let Some(id) = R::verify(&buf[4..4 + verify_msg_len]).await else {
                println!("verify failed");
                return None;
            };
            let (tx, rx) = tokio::sync::mpsc::channel(10);
            let mut tx = Some(tx);
            let waiters = {
                let mut write_conn_map = CONN_MAP.write();
                let waiters = match write_conn_map.remove(&id).expect(
                    "waiters must be inited before receive the verify msg
                        because it's a faster way for function calling",
                ) {
                    ConnState::Connecting(waiters) => waiters,
                    _ => unreachable!(),
                };
                if waiters.len() > 0 {
                    let _ = write_conn_map
                        .insert(id, ConnState::Connected(tx.as_ref().unwrap().clone()));
                } else {
                    let _ = write_conn_map.insert(id, ConnState::Connected(tx.take().unwrap()));
                }
                waiters
            };

            for waiter in waiters {
                // it's ok to send failed
                let _ = waiter.send(tx.as_ref().unwrap().clone());
            }

            // println!("verify success");
            Some(rx)
        }
        let res = tokio::time::timeout(
            Duration::from_secs(5),
            verify_remote_inner::<R>(sockrx, len, buf),
        )
        .await
        .unwrap_or_else(|_elapse| None);
        // println!("verify return");
        res
    }

    pub(super) async fn read_loop(socket: &mut OwnedReadHalf, len: &mut usize, buf: &mut [u8]) {
        loop {
            match socket.read(buf).await {
                Ok(n) => {
                    if n == 0 {
                        println!("connection closed");
                        return;
                    }
                    // println!("recv: {:?}", buf[..n]);
                    *len += n;
                }
                Err(e) => {
                    println!("failed to read from socket; err = {:?}", e);
                    return;
                }
            }
        }
    }

    pub fn spawn_send_loop(
        mut rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
        mut socktx: OwnedWriteHalf,
    ) {
        let _ = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(res) => {
                        socktx.write_all(&res).await.unwrap();
                    }
                    None => todo!(),
                }
            }
        });
    }

    pub(super) async fn wait_for_len(
        socket: &mut OwnedReadHalf,
        len: &mut usize,
        tarlen: usize,
        buf: &mut [u8],
    ) -> bool {
        while *len < tarlen {
            println!("current len: {}, target len: {}", *len, tarlen);
            match socket.read(buf).await {
                Ok(n) => {
                    if n == 0 {
                        println!("connection closed");
                        return false;
                    }
                    // println!("recv: {:?}", buf[..n]);
                    *len += n;
                }
                Err(e) => {
                    println!("failed to read from socket; err = {:?}", e);
                    return false;
                }
            }
        }
        true
    }

    // 4字节u32 长度
    pub(super) fn consume_len(off: usize, buf: &mut [u8], len: &mut usize) -> usize {
        // let ret = bincode::deserialize::<i32>(&buf[off..off + 4]).unwrap() as usize;
        let ret = Bytes::copy_from_slice(&buf[off..off + 4]).get_i32();
        *len -= 4;
        ret as usize
    }
}
