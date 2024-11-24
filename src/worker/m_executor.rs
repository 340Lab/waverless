use crate::{
    general::{
        m_appmeta_manager::AppMetaManager,
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
    result::{WSResult, WsFuncError},
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef},
    util::JoinHandleWrapper,
    worker::func::{m_instance_manager::UnsafeFunctionCtx, EventCtx, FnExeCtx, InstanceTrait},
};
use async_trait::async_trait;

use std::{
    ptr::NonNull,
    sync::atomic::{AtomicU32, AtomicUsize},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;
#[cfg(target_os = "linux")]
use ws_derive::LogicalModule;

use super::func::m_instance_manager::InstanceManager;

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
    next_req_id: AtomicUsize,
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
            next_req_id: AtomicUsize::new(0),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let view = self.view.clone();
        self.view.executor().rpc_handler_distribute_task.regist(
            self.view.p2p(),
            move |responser, r: proto::sche::DistributeTaskReq| {
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
        let (apptype, fnmeta) = {
            let appmetaman_r = self.view.appmeta_manager().meta.read().await;
            let Some(appmeta) = appmetaman_r.get_app_meta(&app).await else {
                // TODO: return err
                unreachable!();
            };
            let Some(fnmeta) = appmeta.get_fn_meta(&func) else {
                // TODO: return err
                unreachable!();
            };
            (appmeta.app_type.clone(), fnmeta.clone())
        };

        let ctx = FnExeCtx {
            app: req.app,
            app_type: apptype,
            func_meta: fnmeta,
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

    pub async fn handle_http_task(&self, route: &str, text: String) -> WSResult<Option<String>> {
        let req_id: ReqId = self
            .next_req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        ////////////////////////////////////////////////////
        // route format ////////////////////////////////////
        // format route, remove last /
        let route = if route.ends_with('/') {
            &route[..route.len() - 1]
        } else {
            route
        };
        let split = route.split("/").into_iter().collect::<Vec<_>>();
        // check path ok
        if split.len() != 2 {
            tracing::warn!(
                "route {} not support, only support appname/funcname now",
                route
            );
            return Err(WsFuncError::InvalidHttpUrl(route.to_owned()).into());
        }

        /////////////////////////////////////////////////
        // existence ////////////////////////////////////
        // trigger app
        let appname = split[0];
        let funcname = split[1];
        let app_meta_man = self.view.appmeta_manager().meta.read().await;
        // check app exist
        let Some(app) = app_meta_man.get_app_meta(appname).await else {
            tracing::warn!("app {} not found", appname);
            return Err(WsFuncError::AppNotFound {
                app: appname.to_owned(),
            }
            .into());
        };
        // check func exist
        let Some(func) = app.get_fn_meta(funcname) else {
            tracing::warn!("func {} not found, exist:{:?}", funcname, app.fns());
            return Err(WsFuncError::FuncNotFound {
                app: appname.to_owned(),
                func: funcname.to_owned(),
            }
            .into());
        };

        /////////////////////////////////////////////////
        // valid call ///////////////////////////////////
        if func
            .calls
            .iter()
            .filter(|call| call.as_http().is_some())
            .next()
            .is_none()
        {
            tracing::warn!(
                "func {} not support http trigger, meta:{:?}",
                funcname,
                func
            );
            return Err(WsFuncError::FuncHttpNotSupported {
                fname: funcname.to_owned(),
                fmeta: func.clone(),
            }
            .into());
        }

        /////////////////////////////////////////////////
        // run //////////////////////////////////////////

        let ctx = FnExeCtx {
            app: appname.to_owned(),
            app_type: app.app_type.clone(),
            func: funcname.to_owned(),
            req_id,
            res: None,
            event_ctx: EventCtx::Http(text),
            sub_waiters: vec![],
            func_meta: func.clone(),
        };
        drop(app_meta_man);
        self.execute(ctx).await
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
    async fn execute(&self, mut fn_ctx: FnExeCtx) -> WSResult<Option<String>> {
        // let app = fn_ctx.app.clone();
        // let func = fn_ctx.func.clone();
        // let event = fn_ctx.event_ctx.clone();

        let instance = self
            .view
            .instance_manager()
            .load_instance(&fn_ctx.app_type, &fn_ctx.app)
            .await;

        let _ = self
            .view
            .instance_manager()
            .instance_running_function
            .write()
            .insert(
                instance.instance_name().to_owned(),
                UnsafeFunctionCtx(
                    NonNull::new(&fn_ctx as *const FnExeCtx as *mut FnExeCtx).unwrap(),
                ),
            );
        tracing::debug!(
            "start run instance {} app {} fn {}",
            instance.instance_name(),
            fn_ctx.app,
            fn_ctx.func
        );
        // TODO: input value should be passed from context, like http request or prev trigger

        let bf_exec_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        let res = instance.execute(&mut fn_ctx).await;

        // let return_to_agent_time = SystemTime::now()
        //     .duration_since(UNIX_EPOCH)
        //     .expect("Time went backwards")
        //     .as_millis() as u64;

        let res = res.map(|v| {
            v.map(|v| {
                let mut res: serde_json::Value = serde_json::from_str(&v).unwrap();
                let _ = res.as_object_mut().unwrap().insert(
                    "bf_exec_time".to_owned(),
                    serde_json::Value::from(bf_exec_time),
                );
                // let _ = res.as_object_mut().unwrap().insert(
                //     "return_to_agent_time".to_owned(),
                //     serde_json::Value::from(return_to_agent_time),
                // );
                serde_json::to_string(&res).unwrap()
            })
        });

        let _ = self
            .view
            .instance_manager()
            .instance_running_function
            .write()
            .remove(&instance.instance_name())
            .unwrap();

        tracing::debug!(
            "finish run instance {} fn {}, res:{:?}",
            instance.instance_name(),
            fn_ctx.func,
            res
        );

        while let Some(t) = fn_ctx.sub_waiters.pop() {
            let _ = t.await.unwrap();
        }
        self.view
            .instance_manager()
            .finish_using(&fn_ctx.app, instance)
            .await;

        res
        // TODO：wait for related tasks triggered.
    }
}
