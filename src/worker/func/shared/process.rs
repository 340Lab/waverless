// process function just run in unique process

use std::sync::Arc;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use parking_lot::RwLock;
use tokio::sync::oneshot;

use crate::{
    general::{
        m_appmeta_manager::{AppType, HttpMethod},
        network::rpc_model,
    },
    result::WsFuncError,
    worker::func::{
        m_instance_manager::{EachAppCache, InstanceManager},
        InstanceTrait,
    },
};

use super::{
    java,
    process_rpc::{self, proc_proto},
    SharedInstance,
};

#[derive(EnumAsInner)]
pub enum ProcessInstanceStateInner {
    Connecting(Vec<oneshot::Sender<proc_proto::FuncStarted>>),
    Connected(proc_proto::FuncStarted),
}

#[derive(Clone)]
pub struct ProcessInstanceState(Arc<RwLock<ProcessInstanceStateInner>>);

#[derive(Clone)]
pub struct ProcessInstance {
    // handle to socket
    app: String,
    state: ProcessInstanceState,
}

impl ProcessInstance {
    pub fn new(app: String) -> Self {
        Self {
            app,
            state: ProcessInstanceState(Arc::new(RwLock::new(
                ProcessInstanceStateInner::Connecting(Vec::new()),
            ))),
        }
    }

    pub fn set_verifyed(&self, verify_msg: proc_proto::FuncStarted) -> bool {
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

    pub async fn wait_for_verify(&self) -> proc_proto::FuncStarted {
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
            return process_rpc::call_func(&fn_ctx.app, &fn_ctx.func, fn_ctx.http_str_unwrap())
                .await
                .map(|_v| Some("unimplemented".to_owned()));
        }

        if let Some(httpmethod) = fn_ctx.func_meta.allow_http_call() {
            let fnverify = self.wait_for_verify().await;
            let Some(http_port) = &fnverify.http_port else {
                return Err(WsFuncError::FuncBackendHttpNotSupported {
                    fname: fn_ctx.func.to_owned(),
                }
                .into());
            };
            let http_url = format!("http://127.0.0.1:{}", http_port);
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

pub trait InstanceManagerProcessTrait {
    // cold start if not ready
    fn get_process_instance(&self, app_type: &AppType, app: &str) -> ProcessInstance;
}

impl InstanceManagerProcessTrait for InstanceManager {
    /// # Panics
    /// We call it when we alreay know it's a process
    ///
    /// So panics will happen if the previous logic is wrong
    fn get_process_instance(&self, app_type: &AppType, app: &str) -> ProcessInstance {
        let instance = self.app_instances.get_or_insert_with(app.to_owned(), || {
            assert!(
                rpc_model::start_remote_once(rpc_model::HashValue::Str(app.to_owned())),
                "failed: register the connecting state, 
                remote verify only success when it's connecting state,
                we don't want a verify request without reason"
            );

            // Cold start
            match app_type {
                AppType::Jar => {
                    let instance = ProcessInstance::new(app.to_owned());
                    let _ = self.app_instances.insert(
                        app.to_owned(),
                        EachAppCache::Shared(SharedInstance(instance.clone())),
                    );
                    // Q2: what if when the verify comes before the insert?
                    // we should insert the instance before cold start
                    java::cold_start(app, self.view.os());

                    // TODO Q1: instance lives forever?
                    // maybe start ttl when verified

                    EachAppCache::Shared(instance.into())
                }
                AppType::Wasm => unreachable!("wasm only support owned instance"),
            }
        });

        return match instance.value() {
            // if it's a process instance, we just return it
            EachAppCache::Owned(_) => {
                unreachable!("not a process instance")
            }
            EachAppCache::Shared(shared) => shared.0.clone(),
        };
    }
}
