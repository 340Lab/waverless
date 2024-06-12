// process function just run in unique process

use std::sync::Arc;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use parking_lot::RwLock;
use tokio::sync::oneshot;

use crate::{
    general::{
        m_appmeta_manager::{AppType, HttpMethod},
        network::rpc_model::{self, HashValue},
    },
    result::WsFuncError,
    worker::func::InstanceTrait,
};

use super::process_rpc::{self, proc_proto};

#[derive(EnumAsInner)]
pub enum ProcessInstanceStateInner {
    Connecting(Vec<oneshot::Sender<proc_proto::AppStarted>>),
    Connected(proc_proto::AppStarted),
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

impl ProcessInstance {
    pub fn new(app: String, app_type: AppType) -> Self {
        Self {
            app_type,
            app,
            state: ProcessInstanceState(Arc::new(RwLock::new(
                ProcessInstanceStateInner::Connecting(Vec::new()),
            ))),
        }
    }

    pub fn set_verifyed(&self, verify_msg: proc_proto::AppStarted) -> bool {
        let mut state_w = self.state.0.write();
        let Some(waiters) = state_w.as_connecting_mut() else {
            tracing::warn!("verify received when already verified");
            return false;
        };
        while let Some(w) = waiters.pop() {
            let _ = w.send(verify_msg.clone()).unwrap();
        }
        *state_w = ProcessInstanceStateInner::Connected(verify_msg);
        true
    }

    pub async fn wait_for_verify(&self) -> proc_proto::AppStarted {
        if let Some(v) = self.state.0.read().as_connected() {
            return v.clone();
        }

        let waiter = {
            let mut state_wr = self.state.0.write();
            match &mut *state_wr {
                ProcessInstanceStateInner::Connected(verify_msg) => {
                    return verify_msg.clone();
                }
                ProcessInstanceStateInner::Connecting(waiters) => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    rx
                }
            }
        };

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
        match &*state {
            ProcessInstanceStateInner::Connected(_) => {
                //just trans to connecting
            }
            ProcessInstanceStateInner::Connecting(_) => {
                // verify not received yet
                // imposible, verify msg is the first analyzed,
                // this function can only be called when function require for update check
                unreachable!("update_checkpoint before verify")
            }
        }
        *state = ProcessInstanceStateInner::Connecting(Vec::new());

        // clean the conn_map in rpc_model
        tracing::debug!("close conn for checkpoint app: {}", self.app);
        rpc_model::close_conn(&HashValue::Str(self.app.clone()));
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
        if fn_ctx.func_meta.allow_rpc_call() {
            let _ = self.wait_for_verify().await;
            return process_rpc::call_func(&fn_ctx.app, &fn_ctx.func, fn_ctx.http_str_unwrap())
                .await
                .map(|v| Some(v.ret_str));
        }

        if let Some(httpmethod) = fn_ctx.func_meta.allow_http_call() {
            let fnverify = self.wait_for_verify().await;
            let Some(http_port) = &fnverify.http_port else {
                return Err(WsFuncError::FuncBackendHttpNotSupported {
                    fname: fn_ctx.func.to_owned(),
                }
                .into());
            };
            let http_url = format!("http://127.0.0.1:{}/{}", http_port, fn_ctx.func);
            let res = match httpmethod {
                HttpMethod::Get => reqwest::get(http_url).await,
                HttpMethod::Post => {
                    reqwest::Client::new()
                        .post(http_url)
                        .body(fn_ctx.http_str_unwrap())
                        .send()
                        .await
                }
            };

            let ok = match res {
                Err(e) => {
                    return Err(WsFuncError::FuncHttpFail {
                        app: fn_ctx.app.clone(),
                        func: fn_ctx.func.clone(),
                        http_err: e,
                    }
                    .into());
                }
                Ok(ok) => ok,
            };

            return ok
                .text()
                .await
                .map_err(|e| {
                    WsFuncError::FuncHttpFail {
                        app: fn_ctx.app.clone(),
                        func: fn_ctx.func.clone(),
                        http_err: e,
                    }
                    .into()
                })
                .map(|ok| Some(ok));
        }
        unreachable!("Missing call description in func meta");
    }
}

// pub struct InstanceManagerProcessState {

// }
