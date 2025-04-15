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
    worker::func::shared::process_rpc::proc_proto::AppStarted,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use prost::Message;
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

    async fn verify(buf: &[u8]) -> Option<HashValue> {
        let res = proc_proto::AppStarted::decode(buf);
        let res: proc_proto::AppStarted = match res {
            Ok(res) => res,
            Err(_) => {
                return None;
            }
        };

        unsafe {
            tracing::debug!("verify begin");
            let appman = ProcessRpc::global_m_app_meta_manager();
            struct Defer;
            impl Drop for Defer {
                fn drop(&mut self) {
                    tracing::debug!("verify end");
                }
            }
            let _d = Defer;

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
                return None;
            }
        }

        Some(HashValue::Str(res.appid))
    }

    fn handle_remote_call(_conn: &HashValue, id: u8, buf: &[u8]) -> bool {
        tracing::debug!("handle_remote_call: id: {}", id);
        let _ = match id {
            4 => (),
            id => {
                tracing::warn!("handle_remote_call: unsupported id: {}", id);
                return false;
            }
        };
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
