pub mod proc_proto {
    include!(concat!(env!("OUT_DIR"), "/process_rpc_proto.rs"));
}
use crate::general::network::rpc_model::{CONN_MAP, ConnState};
use crate::{
    general::network::rpc_model::{self, HashValue, MsgIdBind, ReqMsg, RpcCustom},
    modules_global_bridge::process_func::{
        ModulesGlobalBrigeAppMetaManager, ModulesGlobalBrigeInstanceManager,
    },
    result::{WSResult, WsRpcErr},
    sys::LogicalModulesRef,
    worker::func::shared::process_rpc::proc_proto::AppStarted,
};
use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use proc_proto::{kv_request::Op, kv_response::{self, CommonKvResponse, Resp}, KvPair, KvResponse, KvResponses};
use prost::{
    bytes::{BufMut, BytesMut},
    Message,
};
use std::{collections::HashMap, path::Path, time::Duration};
use tokio::sync::oneshot;

use self::proc_proto::{FuncCallReq, FuncCallResp};

use super::SharedInstance;

// const AGENT_SOCK_PATH: &str = "agent.sock";

fn clean_sock_file(path: impl AsRef<Path>) {
    let _ = std::fs::remove_file(path);
}

pub struct ProcessRpc;

lazy_static::lazy_static! {
    static ref WATING_VERIFY: Mutex<HashMap<String, Vec<oneshot::Sender<AppStarted>>>>=Mutex::new(HashMap::new());
    static ref MODULES: Option<LogicalModulesRef>=None;
}

#[async_trait]
impl RpcCustom for ProcessRpc {
    type SpawnArgs = String;

    // 创建了一个 Unix socket 监听器，用于接收客户端的连接。当有连接时，会通过这个 socket 接收数据。
    fn bind(a: String) -> tokio::net::UnixListener {
        clean_sock_file(&a);
        tokio::net::UnixListener::bind(&a).unwrap()
    }
    // fn deserialize(id: u16, buf: &[u8]) {
    //     let res = match id {
    //         1 => {
    //             let pack = proto::FuncCallResp::decode(buf);
    //             let _ = tokio::spawn(async move {
    //                 // return the result
    //             });
    //         }
    //         _ => unimplemented!(),
    //     };
    // }


    // TODO 每个 return None 的地方打 log 查看
    // 一旦有数据到达，后端会先进行数据验证
    async fn verify(buf: &[u8]) -> Option<HashValue> {

        // 首先尝试将其解码为 proc_proto::AppStarted 结构体
        let res = proc_proto::AppStarted::decode(buf);
        let res: proc_proto::AppStarted = match res {
            Ok(res) => res,

            // 如果解码失败，则请求被忽略
            Err(_) => {
                tracing::debug!("解码失败，请求被忽略");
                return None;
            }
        };
        // 然后再根据 appid 查找对应的实例，并设置其验证状态
        unsafe {
            let appman = ProcessRpc::global_m_app_meta_manager();
            let ishttp = {
                let appmanmetas = appman.meta.read().await;
                let Some(app) = appmanmetas.get_app_meta(&res.appid).await else {
                    tracing::warn!("app {} not found, invalid verify !", res.appid);
                    return None;
                };
                app.contains_http_fn()
            };

            let with_http_port = res.http_port.is_some();
            if ishttp && !with_http_port
            // || (!ishttp && with_http_port) <<< seems ok
            {
                tracing::debug!("ishttp && !with_http_port 校验失败");
                return None;
            }

            // update to the instance
            let insman = ProcessRpc::global_m_instance_manager().unwrap();
            let instance = insman.app_instances.get(&res.appid).expect(&format!(
                "instance should be inited before get the verify {}",
                res.appid
            ));
            let Some(s): Option<&SharedInstance> = instance.value().as_shared() else {
                tracing::warn!("only receive the verify from the instance that is shared");
                return None;
            };
            if !s.0.set_verifyed(res.clone()) {
                tracing::warn!("failed to set verifyed");
                return None;
            }
        }

        Some(HashValue::Str(res.appid))
    }

    // 处理远程调用的主要逻辑。它根据消息的 id 来识别不同类型的请求，并根据请求的类型来处理。
    fn handle_remote_call(conn: &HashValue, id: u8, buf: &[u8]) -> bool {
        tracing::debug!("handle_remote_call: id: {}", id);
        let _ = match id {
            4 => (),
            5 => (),
            id => {
                tracing::warn!("handle_remote_call: unsupported id: {}", id);
                return false;
            }
        };
        // TODO 再加一类处理新消息类型的
        let err = match id {
            4 => match proc_proto::UpdateCheckpoint::decode(buf) {
                Ok(_req) => {
                    tracing::debug!("function requested for checkpoint, but we ignore it");
                    // let conn = conn.clone();
                    // let _ = tokio::spawn(async move {
                    //     unsafe {
                    //         let ins_man = ProcessRpc::global_m_instance_manager().unwrap();
                    //         ins_man.update_checkpoint(conn.as_str().unwrap()).await;
                    //     }
                    // });
                    return true;
                }
                Err(e) => e,
            },
            5 => match proc_proto::KvRequests::decode(buf) {
                Ok(req) => {
                    tracing::debug!("test java kv");
                    // TODO 要不要有处理逻辑，类似 4 下面的 spawn 这块

                    // 先接收到KvRequests，根据内容转换为 KvResponses，再传出去
                    let mut kv_responses: Vec<KvResponse> = Vec::new();

                    // 遍历 KvRequests 中的每一个请求
                    for request in req.requests.clone().iter() {
                        match request.op.clone() {
                            Some(op)=>{
                                match op {
                                    Op::Set(kv_put) => {
                                        // 将 kv_put.kv.key、kv_put.kv.value 解码为 String
                                        let key_str = String::from_utf8(kv_put.kv.key.clone()).unwrap();
                                        let value_str = String::from_utf8(kv_put.kv.value.clone()).unwrap();
                                        tracing::debug!("kv_put, key={:?}, value={:?}", key_str, value_str);

                                        let key = (key_str + "_response").into_bytes();
                                        let value = (value_str + "_response").into_bytes();

                                        // 构造 KvResponse
                                        let kv_response = KvResponse { 
                                            resp: Some(Resp::CommonResp(CommonKvResponse{kvs: vec![KvPair{key, value}]}))
                                        };
                                        kv_responses.push(kv_response);
                                    },
                                    Op::Get(kv_get) => {
                                        let start_str = String::from_utf8(kv_get.range.start.clone()).unwrap();
                                        let end_str = String::from_utf8(kv_get.range.end.clone()).unwrap();
                                        tracing::debug!("kv_get, start={:?}, end={:?}", start_str, end_str);

                                        let key = (start_str + "_response").into_bytes();
                                        let value = (end_str + "_response").into_bytes();

                                        // 构造 KvResponse
                                        let kv_response = KvResponse { 
                                            resp: Some(Resp::CommonResp(CommonKvResponse{kvs: vec![KvPair{key, value}]}))
                                        };
                                        kv_responses.push(kv_response);
                                    },
                                    Op::Delete(kv_delete) => {
                                        let start_str = String::from_utf8(kv_delete.range.start.clone()).unwrap();
                                        let end_str = String::from_utf8(kv_delete.range.end.clone()).unwrap();
                                        tracing::debug!("kv_delete, start={:?}, end={:?}", start_str, end_str);

                                        let key = (start_str + "_response").into_bytes();
                                        let value = (end_str + "_response").into_bytes();

                                        // 构造 KvResponse
                                        let kv_response = KvResponse { 
                                            resp: Some(Resp::CommonResp(CommonKvResponse{kvs: vec![KvPair{key, value}]}))
                                        };
                                        kv_responses.push(kv_response);
                                    },
                                    Op::Lock(kv_lock) => {
                                        let read_or_write = kv_lock.read_or_write;
                                        let release_ids = kv_lock.release_id;
                                        let start_str = String::from_utf8(kv_lock.range.start.clone()).unwrap();
                                        let end_str = String::from_utf8(kv_lock.range.end.clone()).unwrap();
                                        tracing::debug!("kv_lock, read_or_write={:?}, release_id={:?}, start={:?}, end={:?}", read_or_write, release_ids, start_str, end_str);

                                        let key = (start_str + "_response").into_bytes();
                                        let value = (end_str + "_response").into_bytes();

                                        // 构造 KvResponse
                                        let kv_response = KvResponse { 
                                            resp: Some(Resp::CommonResp(CommonKvResponse{kvs: vec![KvPair{key, value}]}))
                                        };
                                        kv_responses.push(kv_response);
                                    },
                                }
                            },
                            None => {},
                        }
                    }

                    let kv_responses = KvResponses { responses: kv_responses };

                    let conn = conn.clone();
                    let _: tokio::task::JoinHandle<Result<(), WsRpcErr>> = tokio::spawn(async move {
                        let tx = {
                            let mut conn_map = CONN_MAP.write(); // 确保使用 await
                            match conn_map.get_mut(&conn) {
                                Some(state) => {
                                    state.tx.clone()
                                },
                                None => {
                                    // 返回一个错误结果
                                    return Err(WsRpcErr::ConnectionNotEstablished(conn).into());
                                }
                            }
                        };
                        
                        // 其他逻辑
                        let mut buf = BytesMut::with_capacity(kv_responses.encoded_len() + 8);
                        // 长度
                        buf.put_i32(kv_responses.encoded_len() as i32);
                        // taskid
                        buf.put_i32(9999);
                        // 区别类型 reqType
                        buf.put_i32(1);

                        tx.send(buf.into()).await.unwrap();

                        Ok(())
                    });

                    return true;
                }
                Err(e) => e,
            },
            _ => unreachable!(),
        };
        tracing::warn!("handle_remote_call error: {:?}", err);
        true
    }
}

impl MsgIdBind for proc_proto::AppStarted {
    fn id() -> u16 {
        1
    }
}

impl MsgIdBind for proc_proto::FuncCallReq {
    fn id() -> u16 {
        2
    }
}

impl MsgIdBind for proc_proto::FuncCallResp {
    fn id() -> u16 {
        3
    }
}

impl MsgIdBind for proc_proto::KvRequests {
    fn id() -> u16 {
        5
    }
}

impl ReqMsg for FuncCallReq {
    type Resp = FuncCallResp;
}

// TODO 看一下是不是根据这个 app或者func 来区分是什么类型的请求
pub async fn call_func(app: &str, func: &str, arg: String) -> WSResult<FuncCallResp> {

    tracing::debug!("CALL_FUNC: app:{}, func:{}, arg:{}", app, func, arg);

    rpc_model::call(
        FuncCallReq {
            func: func.to_owned(),
            arg_str: arg,
        },
        HashValue::Str(app.into()),
        Duration::from_secs(20),
    )
    .await
}
