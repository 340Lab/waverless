use std::{mem::ManuallyDrop, sync::atomic::AtomicU32};

use super::app_meta::FnArg;
use crate::{
    general::network::{
        http_handler::ReqId,
        p2p::{RPCHandler, RPCResponsor},
        proto::{
            self,
            sche::{distribute_task_req, DistributeTaskResp},
        },
    },
    result::WSResult,
    sys::{ExecutorView, LogicalModule, LogicalModuleNewArgs},
    util::JoinHandleWrapper,
    worker::wasm::{WasmInstance, WasmValue},
};
use async_trait::async_trait;
use tokio::{sync::oneshot, task::JoinHandle};
#[cfg(target_os = "linux")]
use wasmedge_sdk::r#async::AsyncState;
use ws_derive::LogicalModule;

pub type SubTaskId = u32;

pub type SubTaskNotifier = oneshot::Sender<bool>;

pub type SubTaskWaiter = oneshot::Receiver<bool>;

#[derive(LogicalModule)]
pub struct Executor {
    sub_task_id: AtomicU32,
    rpc_handler_distribute_task: RPCHandler<proto::sche::DistributeTaskReq>,
    view: ExecutorView,
}

// pub struct FunctionCtxBuilder {
//     pub app: String,
//     pub req_id: ReqId,
//     // pub trigger_node: NodeID,
// }
// impl FunctionCtxBuilder {
//     pub fn new(app: String, req_id: ReqId) -> Self {
//         Self {
//             app,
//             req_id,
//             // trigger_node: 0,
//         }
//     }
//     pub fn build(self, func: String) -> FunctionCtx {
//         FunctionCtx {
//             app: self.app,
//             func,
//             req_id: self.req_id,
//             // trigger_node: self.trigger_node,
//         }
//     }
// }

#[derive(Clone)]
pub enum EventCtx {
    Http(String),
    KvSet { key: Vec<u8> },
}

impl EventCtx {
    pub fn conv_to_wasm_params(&self, fn_arg: &FnArg, vm: &WasmInstance) -> Option<Vec<WasmValue>> {
        fn prepare_vec_in_vm(vm: &WasmInstance, v: &[u8]) -> (i32, i32) {
            let ptr = vm
                .run_func(
                    Some(&vm.instance_names()[0]),
                    "allocate",
                    vec![WasmValue::from_i32(v.len() as i32)],
                )
                .unwrap()[0]
                .to_i32();
            let mut mem = ManuallyDrop::new(unsafe {
                Vec::from_raw_parts(
                    vm.named_module(&vm.instance_names()[0])
                        .unwrap()
                        .memory("memory")
                        .unwrap()
                        .data_pointer_mut(ptr as u32, v.len() as u32)
                        .unwrap(),
                    v.len(),
                    v.len(),
                )
            });
            mem.copy_from_slice(v);
            (ptr, v.len() as i32)
        }
        match (self, fn_arg) {
            (EventCtx::Http(text), FnArg::HttpText) => {
                let (ptr, len) = prepare_vec_in_vm(vm, text.as_bytes());
                Some(vec![WasmValue::from_i32(ptr), WasmValue::from_i32(len)])
            }
            (EventCtx::KvSet { key }, FnArg::KvKey(_)) => {
                let (ptr, len) = prepare_vec_in_vm(vm, &key);
                Some(vec![WasmValue::from_i32(ptr), WasmValue::from_i32(len)])
            }
            _ => None,
        }
    }
}

pub struct FunctionCtx {
    pub app: String,
    pub func: String,
    pub req_id: ReqId,
    pub event_ctx: EventCtx,
    /// remote scheduling tasks
    pub sub_waiters: Vec<JoinHandle<()>>, // pub trigger_node: NodeID,
}

#[async_trait]
impl LogicalModule for Executor {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            rpc_handler_distribute_task: RPCHandler::default(),
            view: ExecutorView::new(args.logical_modules_ref.clone()),
            sub_task_id: AtomicU32::new(0),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let view = self.view.clone();
        self.view.executor().rpc_handler_distribute_task.regist(
            self.view.p2p(),
            move |responser, r| {
                // tracing::info!("rpc recv: {:?}", r);
                let view = view.clone();
                let _ = tokio::spawn(async move {
                    view.executor().handle_distribute_task(responser, r).await;

                    // if let Err(err) = responser
                    //     .send_resp(proto::sche::DistributeTaskResp {})
                    //     .await
                    // {
                    //     tracing::error!("send sche resp failed with err: {}", err);
                    // }
                });
                Ok(())
            },
        );
        // self.view
        //     .p2p()
        //     .regist_rpc::<proto::sche::ScheReq, _>();
        Ok(vec![])
    }
}

impl Executor {
    pub fn register_sub_task(&self) -> SubTaskId {
        let taskid = self
            .sub_task_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        taskid
    }
    pub async fn handle_distribute_task(
        &self,
        resp: RPCResponsor<proto::sche::DistributeTaskReq>,
        req: proto::sche::DistributeTaskReq,
    ) {
        let app = req.app.to_owned();
        let func = req.func.to_owned();
        let ctx = FunctionCtx {
            app: req.app,
            func: req.func,
            req_id: 0,
            event_ctx: match req.trigger.unwrap() {
                distribute_task_req::Trigger::KvKeySet(key) => EventCtx::KvSet { key },
            },
            sub_waiters: vec![],
        };
        self.execute(ctx).await;
        if let Err(err) = resp.send_resp(DistributeTaskResp {}).await {
            tracing::error!("send sche resp for app:{app} fn:{func} failed with err: {err}");
        }
    }
    pub async fn handle_http_task(&self, appname: &str, req_id: ReqId, text: String) {
        let app_meta_man = self.view.instance_manager().app_meta_manager.read().await;
        if let Some(app) = app_meta_man.get_app_meta(appname) {
            if let Some(funcname) = app.http_trigger_fn() {
                let ctx = FunctionCtx {
                    app: appname.to_owned(),
                    func: funcname.to_owned(),
                    req_id,
                    event_ctx: EventCtx::Http(text),
                    sub_waiters: vec![],
                };
                drop(app_meta_man);
                self.execute(ctx).await;
                // self.execute().await;
            }
        }
    }
    // pub async fn execute_http_app(&self, fn_ctx_builder: FunctionCtxBuilder) {
    //     let app_meta_man = self.view.instance_manager().app_meta_manager.read().await;
    //     if let Some(app) = app_meta_man.get_app_meta(&fn_ctx_builder.app) {
    //         if let Some(func) = app.http_trigger_fn() {
    //             self.execute(fn_ctx_builder.build(func.to_owned())).await;
    //         }
    //     }
    // }
    // fn execute_sche_req(&self, sche_req: proto::sche::ScheReq) {
    //     // let vm = self
    //     //     .view
    //     //     .instance_manager()
    //     //     .load_instance(&sche_req.app)
    //     //     .await;

    //     // let _ = self
    //     //     .view
    //     //     .instance_manager()
    //     //     .instance_running_function
    //     //     .write()
    //     //     .insert(
    //     //         vm.instance_names()[0].clone(),
    //     //         Arc::new((sche_req.app.to_owned(), sche_req.func.to_owned())),
    //     //     );

    //     // self.view
    //     //     .instance_manager()
    //     //     .finish_using(&sche_req.app, vm)
    //     //     .await
    // }
    async fn execute(&self, fn_ctx: FunctionCtx) {
        let app = fn_ctx.app.clone();
        let func = fn_ctx.func.clone();
        let event = fn_ctx.event_ctx.clone();

        // Get app meta data for func and args
        let app_metas = self.view.instance_manager().app_meta_manager.read().await;
        if let Some(app_meta) = app_metas.get_app_meta(&app) {
            if let Some(fnmeta) = app_meta.get_fn_meta(&func) {
                let vm = self.view.instance_manager().load_instance(&app).await;
                let _ = self
                    .view
                    .instance_manager()
                    .instance_running_function
                    .write()
                    .insert(vm.instance_names()[0].clone(), fn_ctx);

                // TODO: input value should be passed from context, like http request or prev trigger
                let params = fnmeta
                    .args
                    .iter()
                    .map(|arg| event.conv_to_wasm_params(arg, &vm).unwrap())
                    .flatten()
                    .collect::<Vec<_>>();

                #[cfg(target_os = "linux")]
                let _ = vm
                    .run_func_async(
                        &AsyncState::new(),
                        Some(&vm.instance_names()[0]),
                        func,
                        params,
                    )
                    .await
                    .unwrap_or_else(|_| panic!("vm instance names {:?}", vm.instance_names()));
                #[cfg(target_os = "macos")]
                let _ = vm
                    .run_func(Some(&vm.instance_names()[0]), func, params)
                    .unwrap_or_else(|_| panic!("vm instance names {:?}", vm.instance_names()));

                let mut fn_ctx = self
                    .view
                    .instance_manager()
                    .instance_running_function
                    .write()
                    .remove(&vm.instance_names()[0])
                    .unwrap();

                while let Some(t) = fn_ctx.sub_waiters.pop() {
                    let _ = t.await.unwrap();
                }
                self.view.instance_manager().finish_using(&app, vm).await
            } else {
                tracing::warn!("app {} func {} not found", app, func);
                return;
            }
        } else {
            tracing::warn!("app {} not found", app);
            return;
        };

        // let _ = vm
        //     .run_func_async(
        //         &AsyncState::new(),
        //         Some(&vm.instance_names()[0]),
        //         func,
        //         None,
        //     )
        //     .await
        //     .unwrap_or_else(|_| panic!("vm instance names {:?}", vm.instance_names()));

        // TODO：wait for related tasks triggered.
    }
}
