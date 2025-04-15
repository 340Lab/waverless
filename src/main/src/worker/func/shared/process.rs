// process function just run in unique process

use std::sync::Arc;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use parking_lot::RwLock;
use tokio::{process::Command, sync::oneshot};

use crate::{
    general::{
        app::AppType,
        network::rpc_model::{self, HashValue},
    },
    worker::func::{shared::java, InstanceTrait},
};

use super::process_rpc::{self, proc_proto};

#[derive(EnumAsInner)]
pub enum ProcessInstanceConnState {
    Connecting(Vec<oneshot::Sender<proc_proto::AppStarted>>),
    Connected(proc_proto::AppStarted),
}

pub type PID = u32;
pub struct ProcessInstanceStateInner(
    ProcessInstanceConnState,
    Option<(tokio::process::Child, Option<PID>)>,
);

impl Drop for ProcessInstanceStateInner {
    fn drop(&mut self) {
        assert!(self.1.is_none());
    }
}

#[derive(Clone)]
pub struct ProcessInstanceState(Arc<RwLock<ProcessInstanceStateInner>>);

#[derive(Clone)]
pub struct ProcessInstance {
    /// handle to socket
    app: String,
    /// for take snapshot
    pub app_type: AppType,
    state: ProcessInstanceState,
}

// impl Drop for ProcessInstance {
//     fn drop(&mut self) {
//         // must be killed before drop
//         assert!(self.state.0.read().1.is_none());
//     }
// }

impl ProcessInstance {
    // pub fn raw_pid(&self) -> Option<PID> {
    //     self.state.0.read().1.as_ref().map(|v| v.0.id().unwrap())
    // }
    // pub fn checked_pid(&self) -> Option<PID> {
    //     self.state.0.read().1.as_ref().map(|v| v.1)
    // }
    pub async fn kill(&self) {
        let takeprocess = self.state.0.write().1.take();
        if let Some((mut p, id)) = takeprocess {
            let pid = p.id().unwrap();
            tracing::debug!("killing app {} on pid raw:{} check:{:?}", self.app, pid, id);
            // let pid = p.id().unwrap();
            p.kill().await.unwrap();

            // cmd kill id
            if let Some(id) = id {
                let _ = Command::new("kill")
                    .arg("-9")
                    .arg(id.to_string())
                    .status()
                    .await;
            } else {
                // use jcmd to find and kill
                if self.app_type == AppType::Jar {
                    if let Ok(pid) = java::find_pid(&self.app).await {
                        let _ = Command::new("kill")
                            .arg("-9")
                            .arg(pid.to_string())
                            .status()
                            .await;
                    }
                }
            }

            // clean the conn_map in rpc_model
            tracing::debug!("close conn for p: {}", self.app);
            rpc_model::close_conn(&HashValue::Str(self.app.clone()));

            // let _ = Command::new("kill")
            //     .arg("-9") // Use signal 9 (SIGKILL) for forceful termination
            //     .arg(pid.to_string())
            //     .stdout(Stdio::piped())
            //     .stderr(Stdio::piped())
            //     .output()
            //     .await
            //     .expect("Failed to execute kill command");
            // tracing::debug!(
            //     "check port {:?}",
            //     Command::new("lsof")
            //         .arg("-:i8080") // Use signal 9 (SIGKILL) for forceful termination
            //         .stdout(Stdio::piped())
            //         .stderr(Stdio::piped())
            //         .output()
            //         .await
            //         .expect("Failed to execute kill command")
            // );
        }
    }
    pub fn new(app: String, app_type: AppType) -> Self {
        Self {
            app_type,
            app,
            state: ProcessInstanceState(Arc::new(RwLock::new(ProcessInstanceStateInner(
                ProcessInstanceConnState::Connecting(Vec::new()),
                None,
            )))),
        }
    }
    pub fn bind_process(&self, child: tokio::process::Child) {
        let mut state_w = self.state.0.write();
        state_w.1 = Some((child, None));
    }
    pub fn bind_checked_pid(&self, pid: PID) {
        let mut state_w = self.state.0.write();
        let _ = state_w.1.as_mut().map(|v| v.1 = Some(pid));
    }
    pub fn set_verifyed(&self, verify_msg: proc_proto::AppStarted) -> bool {
        let mut state_w = self.state.0.write();
        state_w.1.as_mut().unwrap().1 = Some(verify_msg.pid);
        let state_w = &mut state_w.0;
        let Some(waiters) = state_w.as_connecting_mut() else {
            tracing::warn!("verify received when already verified");
            return false;
        };
        while let Some(w) = waiters.pop() {
            let _ = w.send(verify_msg.clone()).unwrap();
        }
        *state_w = ProcessInstanceConnState::Connected(verify_msg);
        true
    }

    pub async fn wait_for_verify(&self) -> proc_proto::AppStarted {
        if let Some(v) = self.state.0.read().0.as_connected() {
            return v.clone();
        }

        let waiter = {
            let mut state_wr = self.state.0.write();
            match &mut state_wr.0 {
                ProcessInstanceConnState::Connected(verify_msg) => {
                    tracing::debug!("connected, don't need wait");
                    return verify_msg.clone();
                }
                ProcessInstanceConnState::Connecting(waiters) => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    rx
                }
            }
        };
        tracing::debug!("connecting, wait for verify");
        waiter.await.expect(
            "tx lives in ProcessInstanceStateInner::Connecting and 
            destroyed when { notify all the waiters then transfer to Connected }
            so it's impossible to drop the tx before the rx",
        )
        // let (tx, rx) = oneshot::channel();
        // let mut wating_verify = WATING_VERIFY.lock();
        // let wating = wating_verify.entry(self.app.clone()).or_insert_with(Vec::new);
        // wating.push(tx);
        // drop(wating_verify);
        // let _ = rx.await;
    }
    pub fn before_checkpoint(&self) {
        // state to starting

        let mut state = self.state.0.write();
        match &state.0 {
            ProcessInstanceConnState::Connected(_) => {
                //just trans to connecting
            }
            ProcessInstanceConnState::Connecting(_) => {
                // verify not received yet
                // imposible, verify msg is the first analyzed,
                // this function can only be called when function require for update check
                unreachable!("update_checkpoint before verify")
            }
        }
        state.0 = ProcessInstanceConnState::Connecting(Vec::new());
    }
}

#[async_trait]
impl InstanceTrait for ProcessInstance {
    fn instance_name(&self) -> String {
        self.app.clone()
    }
    async fn execute(
        &self,
        fn_ctx: &mut crate::worker::func::FnExeCtx,
    ) -> crate::result::WSResult<Option<String>> {
        // if rpc_model::start_remote_once(rpc_model::HashValue::Str(fn_ctx.func.to_owned())) {
        //     // cold start the java process
        // }

        // if fn_ctx.func_meta.allow_rpc_call()
        {
            let _ = self.wait_for_verify().await;
            tracing::debug!(
                "wait_for_verify done, call app:{}, func:{}",
                fn_ctx.app,
                fn_ctx.func
            );
            tracing::debug!("before process_rpc::call_func ");
            let res = process_rpc::call_func(&fn_ctx.app, &fn_ctx.func, fn_ctx.http_str_unwrap())
                .await;
            tracing::debug!("after process_rpc::call_func ");
            return res
                .map(|v| Some(v.ret_str));
            // return process_rpc::call_func(&fn_ctx.app, &fn_ctx.func, fn_ctx.http_str_unwrap())
            //     .await
            //     .map(|v| Some(v.ret_str));
        }

        // if let Some(httpmethod) = fn_ctx.func_meta.allow_http_call() {
        //     let fnverify = self.wait_for_verify().await;
        //     let Some(http_port) = &fnverify.http_port else {
        //         return Err(WsFuncError::FuncBackendHttpNotSupported {
        //             fname: fn_ctx.func.to_owned(),
        //         }
        //         .into());
        //     };
        //     let http_url = format!("http://127.0.0.1:{}/{}", http_port, fn_ctx.func);
        //     let res = match httpmethod {
        //         HttpMethod::Get => reqwest::get(http_url).await,
        //         HttpMethod::Post => {
        //             reqwest::Client::new()
        //                 .post(http_url)
        //                 .body(fn_ctx.http_str_unwrap())
        //                 .send()
        //                 .await
        //         }
        //     };

        //     let ok = match res {
        //         Err(e) => {
        //             return Err(WsFuncError::FuncHttpFail {
        //                 app: fn_ctx.app.clone(),
        //                 func: fn_ctx.func.clone(),
        //                 http_err: e,
        //             }
        //             .into());
        //         }
        //         Ok(ok) => ok,
        //     };

        //     return ok
        //         .text()
        //         .await
        //         .map_err(|e| {
        //             WsFuncError::FuncHttpFail {
        //                 app: fn_ctx.app.clone(),
        //                 func: fn_ctx.func.clone(),
        //                 http_err: e,
        //             }
        //             .into()
        //         })
        //         .map(|ok| Some(ok));
        // }
        // unreachable!("Missing call description in func meta");
    }
}

// pub struct InstanceManagerProcessState {

// }
