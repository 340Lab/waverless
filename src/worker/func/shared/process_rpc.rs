pub mod proc_proto {
    include!(concat!(env!("OUT_DIR"), "/process_rpc_proto.rs"));
}

use crate::{
    general::network::rpc_model::{self, HashValue, MsgIdBind, ReqMsg, RpcCustom},
    modules_global_bridge::process_func::{
        ModulesGlobalBrigeAppMetaManager, ModulesGlobalBrigeInstanceManager,
    },
    result::WSResult,
    sys::LogicalModulesRef,
    worker::func::shared::process_rpc::proc_proto::FuncStarted,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use prost::Message;
use std::{collections::HashMap, path::Path, time::Duration};
use tokio::sync::oneshot;

use self::proc_proto::{FuncCallReq, FuncCallResp};

use super::SharedInstance;

const AGENT_SOCK_PATH: &str = "agent.sock";

fn clean_sock_file(path: impl AsRef<Path>) {
    let _ = std::fs::remove_file(path);
}

pub struct ProcessRpc;

lazy_static::lazy_static! {
    static ref WATING_VERIFY: Mutex<HashMap<String, Vec<oneshot::Sender<FuncStarted>>>>=Mutex::new(HashMap::new());
    static ref MODULES: Option<LogicalModulesRef>=None;
}

#[async_trait]
impl RpcCustom for ProcessRpc {
    fn bind() -> tokio::net::UnixListener {
        clean_sock_file(AGENT_SOCK_PATH);
        tokio::net::UnixListener::bind(AGENT_SOCK_PATH).unwrap()
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

    async fn verify(buf: &[u8]) -> Option<HashValue> {
        let res = proc_proto::FuncStarted::decode(buf);
        let res: proc_proto::FuncStarted = match res {
            Ok(res) => res,
            Err(_) => {
                return None;
            }
        };

        unsafe {
            let appman = ProcessRpc::global_m_app_meta_manager();
            let ishttp = {
                let appmanmetas = appman.meta.read().await;
                let Some(app) = appmanmetas.get_app_meta(&res.fnid) else {
                    tracing::warn!("app not found, invalid verify !");
                    return None;
                };
                app.contains_http_fn()
            };

            let with_http_port = res.http_port.is_some();
            if ishttp && !with_http_port
            // || (!ishttp && with_http_port) <<< seems ok
            {
                return None;
            }

            // update to the instance
            let insman = ProcessRpc::global_m_instance_manager().unwrap();
            let fins = insman
                .app_instances
                .get(&res.fnid)
                .expect("instance should be inited before get the verify");
            let Some(s): Option<&SharedInstance> = fins.value().as_shared() else {
                tracing::warn!("only receive the verify from the instance that is shared");
                return None;
            };
            if !s.0.set_verifyed(res.clone()) {
                return None;
            }
        }

        Some(HashValue::Str(res.fnid))
    }
}

impl MsgIdBind for proc_proto::FuncStarted {
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

impl ReqMsg for FuncCallReq {
    type Resp = FuncCallResp;
}

pub async fn call_func(app: &str, func: &str, arg: String) -> WSResult<FuncCallResp> {
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
