use core::panic;
use std::{mem::ManuallyDrop, sync::atomic::AtomicU32};

use super::m_instance_manager::InstanceManager;
use crate::{
    general::{
        m_appmeta_manager::{AppMetaManager, FnArg},
        network::{
            http_handler::ReqId,
            m_p2p::{P2PModule, RPCHandler, RPCResponsor},
            proto::{
                self,
                sche::{distribute_task_req, DistributeTaskResp},
            },
        },
    },
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef},
    util::JoinHandleWrapper,
    worker::wasm::{WasmInstance, WasmValue},
};
use async_trait::async_trait;
use tokio::{sync::oneshot, task::JoinHandle};
#[cfg(target_os = "linux")]
use wasmedge_sdk::r#async::AsyncState;
use wasmedge_sdk::Vm;
use ws_derive::LogicalModule;

pub type SubTaskId = u32;

pub type SubTaskNotifier = oneshot::Sender<bool>;

pub type SubTaskWaiter = oneshot::Receiver<bool>;

logical_module_view_impl!(ExecutorView);
logical_module_view_impl!(ExecutorView, p2p, P2PModule);
logical_module_view_impl!(ExecutorView, appmeta_manager, AppMetaManager);
logical_module_view_impl!(ExecutorView, instance_manager, Option<InstanceManager>);
logical_module_view_impl!(ExecutorView, executor, Option<Executor>);

#[derive(LogicalModule)]
pub struct Executor {
    sub_task_id: AtomicU32,
    rpc_handler_distribute_task: RPCHandler<proto::sche::DistributeTaskReq>,
    view: ExecutorView,
}

pub trait VmExt {
    fn vm_instance_name(&self) -> String;
}

impl VmExt for Vm {
    fn vm_instance_name(&self) -> String {
        self.instance_names()
            .into_iter()
            .filter(|v| &*v != "env" && &*v != "wasi_snapshot_preview1")
            .next()
            .unwrap()
    }
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

#[derive(Clone, Debug)]
pub enum EventCtx {
    Http(String),
    KvSet { key: Vec<u8>, opeid: Option<u32> },
}

impl EventCtx {
    pub fn take_prev_kv_opeid(&mut self) -> Option<u32> {
        match self {
            EventCtx::KvSet { opeid, .. } => opeid.take(),
            _ => None,
        }
    }
    pub fn conv_to_wasm_params(&self, fn_arg: &FnArg, vm: &WasmInstance) -> Vec<WasmValue> {
        fn prepare_vec_in_vm(vm: &WasmInstance, v: &[u8]) -> (i32, i32) {
            let vm_ins = vm.vm_instance_name();
            let ptr = vm
                .run_func(
                    Some(&vm_ins),
                    "allocate",
                    vec![WasmValue::from_i32(v.len() as i32)],
                )
                .unwrap_or_else(|err| {
                    panic!("err:{:?} vm instance names:{:?}", err, vm.instance_names())
                })[0]
                .to_i32();
            let mut mem = ManuallyDrop::new(unsafe {
                Vec::from_raw_parts(
                    vm.named_module(&vm_ins)
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
                if text.len() == 0 {
                    return vec![];
                }
                let (ptr, len) = prepare_vec_in_vm(vm, text.as_bytes());
                vec![WasmValue::from_i32(ptr), WasmValue::from_i32(len)]
            }
            (EventCtx::KvSet { key, .. }, FnArg::KvKey(_)) => {
                let (ptr, len) = prepare_vec_in_vm(vm, &key);
                vec![WasmValue::from_i32(ptr), WasmValue::from_i32(len)]
            }
            (e, f) => panic!("not support event ctx and fn arg: {:?} {:?}", e, f),
        }
    }
}

pub struct FunctionCtx {
    pub app: String,
    pub func: String,
    pub req_id: ReqId,
    pub event_ctx: EventCtx,
    pub res: Option<String>,
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
        tracing::debug!("receive distribute task: {:?}", req);
        let app = req.app.to_owned();
        let func = req.func.to_owned();
        let ctx = FunctionCtx {
            app: req.app,
            func: req.func,
            req_id: 0,
            res: None,
            event_ctx: match req.trigger.unwrap() {
                distribute_task_req::Trigger::KvSet(set) => EventCtx::KvSet {
                    key: set.key,
                    opeid: Some(set.opeid),
                },
            },
            sub_waiters: vec![],
        };
        if let Err(err) = resp.send_resp(DistributeTaskResp {}).await {
            tracing::error!("send sche resp for app:{app} fn:{func} failed with err: {err}");
        }
        let _ = self.execute(ctx).await;
    }
    pub async fn handle_http_task(
        &self,
        route: &str,
        req_id: ReqId,
        text: String,
    ) -> Option<String> {
        let split = route.split("/").into_iter().collect::<Vec<_>>();

        // trigger app
        let appname = split[0];
        let app_meta_man = self.view.appmeta_manager().meta.read().await;
        if let Some(app) = app_meta_man.get_app_meta(appname) {
            if split.len() == 1 {
                if let Some(funcname) = app.http_trigger_fn() {
                    let ctx = FunctionCtx {
                        app: appname.to_owned(),
                        func: funcname.to_owned(),
                        req_id,
                        res: None,
                        event_ctx: EventCtx::Http(text),
                        sub_waiters: vec![],
                    };
                    drop(app_meta_man);
                    return self.execute(ctx).await;
                    // self.execute().await;
                } else {
                    tracing::warn!("app {} http trigger not found", appname);
                }
            } else if split.len() == 2 {
                let funcname = split[1];
                if let Some(_func) = app.get_fn_meta(funcname) {
                    let ctx = FunctionCtx {
                        app: appname.to_owned(),
                        func: funcname.to_owned(),
                        req_id,
                        res: None,
                        event_ctx: EventCtx::Http(text),
                        sub_waiters: vec![],
                    };
                    drop(app_meta_man);
                    return self.execute(ctx).await;
                } else {
                    tracing::warn!("func {} not found, exist:{:?}", funcname, app.fns());
                }
            } else {
                tracing::warn!("not support route: {}", route);
            }
        } else {
            tracing::warn!("app {} not found", appname);
        }

        None
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
    async fn execute(&self, fn_ctx: FunctionCtx) -> Option<String> {
        let app = fn_ctx.app.clone();
        let func = fn_ctx.func.clone();
        let event = fn_ctx.event_ctx.clone();

        // Get app meta data for func and args
        let app_metas = self.view.appmeta_manager().meta.read().await;
        if let Some(app_meta) = app_metas.get_app_meta(&app) {
            if let Some(fnmeta) = app_meta.get_fn_meta(&func) {
                let vm = self.view.instance_manager().load_instance(&app).await;
                let _ = self
                    .view
                    .instance_manager()
                    .instance_running_function
                    .write()
                    .insert(vm.vm_instance_name().to_owned(), fn_ctx);
                tracing::debug!(
                    "start run instance {} app {} fn {}",
                    vm.vm_instance_name(),
                    app,
                    func
                );
                // TODO: input value should be passed from context, like http request or prev trigger
                let params = fnmeta
                    .args
                    .iter()
                    .map(|arg| event.conv_to_wasm_params(arg, &vm))
                    .flatten()
                    .collect::<Vec<_>>();

                tracing::debug!("execure params: {:?}", params);

                #[cfg(target_os = "linux")]
                {
                    let mut a = None;
                    if vm.active_module().is_err() {
                        a = Some(vm.vm_instance_name())
                    }
                    if let Err(err) = vm
                        .run_func_async(&AsyncState::new(), a.as_ref().map(|s| &**s), func, params)
                        .await
                    {
                        tracing::error!("run func failed with err: {}", err);
                    }
                }
                #[cfg(target_os = "macos")]
                let _ = vm
                    .run_func(Some(&vm.instance_names()[0]), func, params)
                    .unwrap_or_else(|_| panic!("vm instance names {:?}", vm.instance_names()));

                let mut fn_ctx = self
                    .view
                    .instance_manager()
                    .instance_running_function
                    .write()
                    .remove(&vm.vm_instance_name())
                    .unwrap();
                tracing::debug!(
                    "finish run instance {} fn {}",
                    vm.vm_instance_name(),
                    fn_ctx.func
                );
                while let Some(t) = fn_ctx.sub_waiters.pop() {
                    let _ = t.await.unwrap();
                }
                self.view.instance_manager().finish_using(&app, vm).await;

                fn_ctx.res
            } else {
                tracing::warn!("app {} func {} not found", app, func);
                None
            }
        } else {
            tracing::warn!("app {} not found", app);
            None
        }

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
