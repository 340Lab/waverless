use crate::general::app::instance::m_instance_manager::InstanceManager;
use crate::general::app::instance::m_instance_manager::UnsafeFunctionCtx;
use crate::general::app::instance::InstanceTrait;
use crate::general::app::AppType;
use crate::general::app::FnMeta;
use crate::general::data::m_data_general::DATA_UID_PREFIX_FN_KV;
use crate::general::network::m_p2p::RPCCaller;
use crate::general::network::m_p2p::TaskId;
use crate::general::network::proto::FnTaskId;
use crate::result::WSError;
use crate::result::WSResultExt;
use crate::sys::NodeID;
use crate::{
    general::{
        app::AppMetaManager,
        network::{
            http_handler::ReqId,
            m_p2p::{P2PModule, RPCHandler, RPCResponsor},
            proto::{
                self, {distribute_task_req, DistributeTaskResp},
            },
        },
    },
    logical_module_view_impl,
    result::{WSResult, WsFuncError},
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;
use std::time::Duration;
use std::{
    ptr::NonNull,
    sync::atomic::{AtomicU32, AtomicUsize},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
#[cfg(target_os = "linux")]
use ws_derive::LogicalModule;

// pub type SubTaskId = u32;

// pub type SubTaskNotifier = oneshot::Sender<bool>;

// pub type SubTaskWaiter = oneshot::Receiver<bool>;

#[derive(Clone, Debug)]
pub enum EventCtx {
    Http(String),
    KvSet {
        key: Vec<u8>,
        opeid: Option<u32>,
        src_task_id: proto::FnTaskId,
    },
}

impl EventCtx {
    pub fn take_prev_kv_opeid(&mut self) -> Option<u32> {
        match self {
            EventCtx::KvSet { opeid, .. } => opeid.take(),
            _ => None,
        }
    }
}

struct FnExeCtx {
    pub app: String,
    pub app_type: AppType,
    pub func: String,
    pub _func_meta: FnMeta,
    // pub _req_id: ReqId,
    pub task_id: FnTaskId,
    pub event_ctx: EventCtx,
    pub res: Option<String>,
    /// remote scheduling tasks
    // pub sub_waiters: Vec<JoinHandle<()>>, // pub trigger_node: NodeID,
    _dummy_private: (),
}

pub enum FnExeCtxAsyncAllowedType {
    Jar,
    Wasm,
    Native,
}

impl TryFrom<AppType> for FnExeCtxAsyncAllowedType {
    type Error = WSError;
    fn try_from(v: AppType) -> Result<Self, WSError> {
        match v {
            AppType::Jar => Ok(FnExeCtxAsyncAllowedType::Jar),
            AppType::Wasm => Ok(FnExeCtxAsyncAllowedType::Wasm),
            AppType::Native => Ok(FnExeCtxAsyncAllowedType::Native),
        }
    }
}

impl Into<AppType> for FnExeCtxAsyncAllowedType {
    fn into(self) -> AppType {
        match self {
            FnExeCtxAsyncAllowedType::Jar => AppType::Jar,
            FnExeCtxAsyncAllowedType::Wasm => AppType::Wasm,
            FnExeCtxAsyncAllowedType::Native => AppType::Native,
        }
    }
}

pub struct FnExeCtxAsync {
    inner: FnExeCtx,
}

impl FnExeCtxAsync {
    pub fn new(
        apptype: FnExeCtxAsyncAllowedType,
        app: String,
        func: String,
        func_meta: FnMeta,
        task_id: FnTaskId,
        event_ctx: EventCtx,
    ) -> Self {
        Self {
            inner: FnExeCtx {
                app,
                func,
                task_id,
                event_ctx,
                res: None,
                // sub_waiters: vec![],
                app_type: apptype.into(),
                _func_meta: func_meta,
                _dummy_private: (),
            },
        }
    }

    pub fn task_id(&self) -> &FnTaskId {
        &self.inner.task_id
    }

    pub fn event_ctx(&self) -> &EventCtx {
        &self.inner.event_ctx
    }

    pub fn empty_http(&self) -> bool {
        match &self.inner.event_ctx {
            EventCtx::Http(text) => text.is_empty(),
            _ => false,
        }
    }

    // pub fn http_str_unwrap(&self) -> String {
    //     match &self.inner.event_ctx {
    //         EventCtx::Http(text) => text.clone(),
    //         _ => panic!("not http event ctx"),
    //     }
    // }

    pub fn set_result(&mut self, result: Option<String>) {
        self.inner.res = result;
    }

    pub fn take_result(&mut self) -> Option<String> {
        self.inner.res.take()
    }

    pub fn app_name(&self) -> &str {
        &self.inner.app
    }

    pub fn func_name(&self) -> &str {
        &self.inner.func
    }

    pub fn func_meta(&self) -> &FnMeta {
        &self.inner._func_meta
    }

    pub fn event_ctx_mut(&mut self) -> &mut EventCtx {
        &mut self.inner.event_ctx
    }
}

pub enum FnExeCtxSyncAllowedType {
    Native,
}

impl TryFrom<AppType> for FnExeCtxSyncAllowedType {
    type Error = WSError;
    fn try_from(v: AppType) -> Result<Self, WSError> {
        match v {
            AppType::Native => Ok(FnExeCtxSyncAllowedType::Native),
            AppType::Jar | AppType::Wasm => Err(WSError::from(WsFuncError::UnsupportedAppType)),
        }
    }
}

impl Into<AppType> for FnExeCtxSyncAllowedType {
    fn into(self) -> AppType {
        AppType::Native
    }
}

pub struct FnExeCtxSync {
    inner: FnExeCtx,
}

impl FnExeCtxSync {
    pub fn new(
        apptype: FnExeCtxAsyncAllowedType,
        app: String,
        func: String,
        func_meta: FnMeta,
        // req_id: ReqId,
        task_id: FnTaskId,
        event_ctx: EventCtx,
    ) -> Self {
        Self {
            inner: FnExeCtx {
                app,
                func,
                task_id,
                event_ctx,
                res: None,
                // sub_waiters: vec![],
                app_type: apptype.into(),
                _func_meta: func_meta,
                _dummy_private: (),
            },
        }
    }
}

// impl FnExeCtx {
//     pub fn empty_http(&self) -> bool {
//         match &self.event_ctx {
//             EventCtx::Http(str) => str.len() == 0,
//             _ => false,
//         }
//     }
//     /// call this when you are sure it's a http event
//     pub fn http_str_unwrap(&self) -> String {
//         match &self.event_ctx {
//             EventCtx::Http(str) => str.to_owned(),
//             _ => panic!("not a http event"),
//         }
//     }
// }

logical_module_view_impl!(ExecutorView);
logical_module_view_impl!(ExecutorView, p2p, P2PModule);
logical_module_view_impl!(ExecutorView, appmeta_manager, AppMetaManager);
logical_module_view_impl!(ExecutorView, instance_manager, InstanceManager);
logical_module_view_impl!(ExecutorView, executor, Executor);

#[derive(LogicalModule)]
pub struct Executor {
    sub_task_id: AtomicU32,
    // next_req_id: AtomicUsize,
    view: ExecutorView,

    // src task id -> [(task run node, task id)]
    task_subwait_for: DashMap<u32, Vec<(NodeID, FnTaskId)>>,

    // this runing task id -> src waiting rpc
    task_subwait_by: DashMap<FnTaskId, broadcast::Sender<String>>,

    rpc_handler_distribute_task: RPCHandler<proto::DistributeTaskReq>,
    rpc_caller_listen_for_task_done: RPCCaller<proto::ListenForTaskDoneReq>,
    rpc_handler_listen_for_task_done: RPCHandler<proto::ListenForTaskDoneReq>,
    rpc_handler_add_wait_target: RPCHandler<proto::AddWaitTargetReq>,
}

#[derive(Serialize, Deserialize)]
struct FnDataEventArg {
    src_called_by: NodeID,
    src_taskid: u32,
    trigger_data_key: String,
}

/// Base trait for function execution contexts
pub trait FnExeCtxBase {
    /// Get the application name
    fn app(&self) -> &str;
    /// Get the function name
    fn func(&self) -> &str;
    /// Get the event context
    fn event_ctx(&self) -> &EventCtx;
    /// Get mutable reference to event context
    fn event_ctx_mut(&mut self) -> &mut EventCtx;
    /// Get fn type
    fn app_type(&self) -> AppType;
    /// format arg to pass to function
    fn format_arg_to_pass(&self) -> String {
        match &self.event_ctx() {
            EventCtx::Http(text) => text.clone(),
            EventCtx::KvSet {
                key, src_task_id, ..
            } => {
                let key_str = std::str::from_utf8(&key).unwrap();
                let trigger_data_key = match self.app_type() {
                    AppType::Jar | AppType::Wasm => {
                        // remove prefix fkv
                        key_str
                            .strip_prefix(DATA_UID_PREFIX_FN_KV)
                            .unwrap_or(key_str)
                            .to_string()
                    }
                    AppType::Native => {
                        // keep the key as is
                        key_str.to_string()
                    }
                };

                let arg = FnDataEventArg {
                    trigger_data_key,
                    src_called_by: src_task_id.call_node_id,
                    src_taskid: src_task_id.task_id,
                };
                serde_json::to_string(&arg).unwrap()
            }
        }
    }
}

impl FnExeCtxBase for FnExeCtxAsync {
    fn app(&self) -> &str {
        &self.inner.app
    }
    fn func(&self) -> &str {
        &self.inner.func
    }
    fn event_ctx(&self) -> &EventCtx {
        &self.inner.event_ctx
    }
    fn event_ctx_mut(&mut self) -> &mut EventCtx {
        &mut self.inner.event_ctx
    }
    fn app_type(&self) -> AppType {
        self.inner.app_type
    }
}

impl FnExeCtxBase for FnExeCtxSync {
    fn app(&self) -> &str {
        &self.inner.app
    }
    fn func(&self) -> &str {
        &self.inner.func
    }
    fn event_ctx(&self) -> &EventCtx {
        &self.inner.event_ctx
    }
    fn event_ctx_mut(&mut self) -> &mut EventCtx {
        &mut self.inner.event_ctx
    }
    fn app_type(&self) -> AppType {
        self.inner.app_type
    }
}

impl Executor {
    /// return last task response
    pub async fn wait_for_subtasks(&self, thistask: &u32) -> Option<String> {
        let mut done_tasks = vec![];
        let mut last_res = None;
        loop {
            if !self.task_subwait_for.contains_key(&thistask) {
                tracing::debug!(
                    "src task {} wait_for_subtasks with {:?}",
                    thistask,
                    done_tasks
                );
                break;
            }
            let mut wait_tasks = Vec::new();
            while let Some((_thistask, node_tasks)) = self.task_subwait_for.remove(&thistask) {
                for (node, task) in node_tasks {
                    done_tasks.push(task.clone());
                    let view = self.view.clone();
                    let wait_task = tokio::spawn(async move {
                        let res: Result<proto::ListenForTaskDoneResp, WSError> = view
                            .executor()
                            .rpc_caller_listen_for_task_done
                            .call(
                                view.p2p(),
                                node,
                                proto::ListenForTaskDoneReq {
                                    task_id: Some(task),
                                },
                                Some(Duration::from_secs(180)),
                            )
                            .await;
                        res.map_err(|err| {
                            tracing::error!("listen for task done failed with err: {}", err);
                            err
                        })
                        .unwrap()
                    });
                    wait_tasks.push(wait_task);
                }
            }
            for wait_task in wait_tasks {
                let res = wait_task.await.unwrap();
                if !res.success {
                    tracing::error!(
                        "listen for task done failed with err: {}",
                        res.response_or_errmsg
                    );
                } else {
                    tracing::debug!("listen for task done success: {}", res.response_or_errmsg);
                    last_res = Some(res.response_or_errmsg);
                }
            }
        }
        last_res
    }
    pub fn notify_subwait_done(&self, taskid: &FnTaskId, res: String) {
        loop {
            if let Some((_, sender)) = self.task_subwait_by.remove(&taskid) {
                let _ = sender.send(res);
            }
            return;
        }
    }
    // pub fn take_subwaitings_for_task(&self, taskid: &FnTaskId) -> Option<broadcast::Sender<()>> {
    //     self.task_subwait_by.remove(&taskid).map(|res| res.1)
    // }
    async fn start_rpc(&self) -> WSResult<()> {
        self.rpc_caller_listen_for_task_done.regist(self.view.p2p());
        {
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
        }
        {
            // after some function done, check the waiting list
            // beingg the listen for task done caller
            let view = self.view.clone();
            let _ =
                self.rpc_handler_add_wait_target
                    .regist(self.view.p2p(), move |responsor, req| {
                        let view = view.clone();
                        let _ = tokio::spawn(async move {
                            view.executor()
                                .task_subwait_for
                                .entry(req.src_task_id)
                                .or_insert_with(|| vec![])
                                .push((req.task_run_node, req.sub_task_id.unwrap()));
                            // view.executor().handle_add_wait_target(responsor,req).await;
                            let _ = responsor
                                .send_resp(proto::AddWaitTargetResp {
                                    success: true,
                                    err_msg: "".to_owned(),
                                })
                                .await
                                .todo_handle("add wait target");
                        });
                        Ok(())
                    });
        }
        // {
        //     self.rpc_caller_add_wait_target.regist(self.view.p2p());
        // }
        {
            let view = self.view.clone();
            self.rpc_handler_listen_for_task_done
                .regist(self.view.p2p(), move |responsor, req| {
                    let view = view.clone();
                    let _ = tokio::spawn(async move {
                        tracing::debug!("listen for task done: {:?}", req.task_id);
                        let mut sub = {
                            view.executor()
                                .task_subwait_by
                                .entry(req.task_id.unwrap())
                                .or_insert_with(|| broadcast::channel(16).0)
                                .subscribe()
                        };
                        let res = sub.recv().await;
                        tracing::debug!("task is done: {:?}", res);
                        match res {
                            Ok(res) => {
                                let _ = responsor
                                    .send_resp(proto::ListenForTaskDoneResp {
                                        success: true,
                                        response_or_errmsg: res,
                                    })
                                    .await
                                    .todo_handle("listen task done");
                            }
                            Err(err) => {
                                tracing::warn!("listen task done failed: {:?}", err);
                                let _ = responsor
                                    .send_resp(proto::ListenForTaskDoneResp {
                                        success: false,
                                        response_or_errmsg: format!("err:{:?}", err),
                                    })
                                    .await
                                    .todo_handle("listen task done");
                            }
                        }
                    });
                    Ok(())
                });
        }

        Ok(())
    }
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
            // next_req_id: AtomicUsize::new(0),
            rpc_caller_listen_for_task_done: RPCCaller::new(),
            rpc_handler_listen_for_task_done: RPCHandler::new(),
            rpc_handler_add_wait_target: RPCHandler::new(),

            task_subwait_by: DashMap::new(),
            task_subwait_for: DashMap::new(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        self.start_rpc().await?;
        // self.view
        //     .p2p()
        //     .regist_rpc::<proto::sche::ScheReq, _>();
        Ok(vec![])
    }
}

impl Executor {
    pub fn register_sub_task(&self) -> proto::FnTaskId {
        let taskid = self
            .sub_task_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // (self.view.p2p().nodes_config.this_node(), taskid)
        FnTaskId {
            call_node_id: self.view.p2p().nodes_config.this_node(),
            task_id: taskid,
        }
    }

    pub async fn local_call_execute_async(&self, ctx: FnExeCtxAsync) -> WSResult<Option<String>> {
        self.execute(ctx).await
    }

    pub fn local_call_execute_sync(&self, ctx: FnExeCtxSync) -> WSResult<Option<String>> {
        self.execute_sync(ctx)
    }

    pub fn handle_exec_result(&self, taskid: &FnTaskId, res: WSResult<Option<String>>) {
        let res_str = match res {
            Ok(Some(res)) => res,
            Ok(None) => "".to_owned(),
            Err(err) => {
                tracing::warn!(
                    "handle failed exec result for taskid: {:?} with err: {}",
                    taskid,
                    err
                );
                format!("err:{:?}", err)
            }
        };
        self.notify_subwait_done(taskid, res_str);
    }

    pub async fn handle_distribute_task(
        &self,
        resp: RPCResponsor<proto::DistributeTaskReq>,
        req: proto::DistributeTaskReq,
    ) {
        tracing::debug!("receive distribute task: {:?}", req);
        // alert src to wait for this task

        let app = req.app.to_owned();
        let func = req.func.to_owned();
        // todo
        let (appmeta, _) = match self.view.appmeta_manager().get_app_meta(&app).await {
            Ok(Some(appmeta)) => appmeta,
            Ok(None) => {
                tracing::warn!("app {} not found in data meta", app);
                if let Err(err) = resp
                    .send_resp(DistributeTaskResp {
                        success: false,
                        err_msg: format!("app {} not found in data meta", app),
                    })
                    .await
                {
                    tracing::error!("send distribute task resp failed with err: {}", err);
                }
                return;
            }
            Err(err) => {
                tracing::error!("get appmeta failed with err: {}", err);
                if let Err(err) = resp
                    .send_resp(DistributeTaskResp {
                        success: false,
                        err_msg: format!("get appmeta failed with err: {}", err),
                    })
                    .await
                {
                    tracing::error!("send distribute task resp failed with err: {}", err);
                }
                return;
            }
        };

        let apptype = appmeta.app_type.clone();
        let Some(fnmeta) = appmeta.get_fn_meta(&func) else {
            tracing::warn!("func {} not found, exist:{:?}", func, appmeta.fns());
            if let Err(err) = resp
                .send_resp(DistributeTaskResp {
                    success: false,
                    err_msg: format!("func {} not found, exist:{:?}", func, appmeta.fns()),
                })
                .await
            {
                tracing::error!("send distribute task resp failed with err: {}", err);
            }
            return;
        };

        //费新文
        // distribute task requires sync support
        // if fnmeta.sync_async.asyncable() {
        //     let warn = format!(
        //         "func {} not support sync, meta:{:?}",
        //         func, fnmeta.sync_async
        //     );
        //     tracing::warn!("{}", warn);
        //     if let Err(err) = resp
        //         .send_resp(DistributeTaskResp {
        //             success: false,
        //             err_msg: warn,
        //         })
        //         .await
        //     {
        //         tracing::error!("send distribute task resp failed with err: {}", err);
        //     }
        //     return;
        // }

        // // construct sync fn exe ctx
        // let ctx = FnExeCtxSync::new(
        //     match FnExeCtxAsyncAllowedType::try_from(apptype) {  // 这里修正为 FnExeCtxAsyncAllowedType
        //         Ok(v) => v,
        //         Err(err) => {
        //             let warn = format!("app type {:?} not supported, err: {}", apptype, err);
        //             tracing::warn!("{}", warn);
        //             if let Err(err) = resp
        //                 .send_resp(DistributeTaskResp {
        //                     success: false,
        //                     err_msg: warn,
        //                 })
        //                 .await
        //             {
        //                 tracing::error!("send distribute task resp failed with err: {}", err);
        //             }
        //             return;
        //         }
        //     },
        //     req.app,
        //     req.func,
        //     fnmeta.clone(),
        //     req.task_id as usize,
        //     match req.trigger.unwrap() {
        //         distribute_task_req::Trigger::EventNew(new) => EventCtx::KvSet {
        //             key: new.key,
        //             opeid: Some(new.opeid),
        //         },
        //         distribute_task_req::Trigger::EventWrite(write) => EventCtx::KvSet {
        //             key: write.key,
        //             opeid: Some(write.opeid),
        //         },
        //     },
        // );

        // if let Err(err) = resp
        //     .send_resp(DistributeTaskResp {
        //         success: true,
        //         err_msg: "".to_owned(),
        //     })
        //     .await
        // {
        //     tracing::error!("send sche resp for app:{app} fn:{func} failed with err: {err}");
        // }
        // let _ = self.execute_sync(ctx);

        //判断函数是否支持异步或者同步
        // distribute task requires async support
        if !fnmeta.sync_async.asyncable() {
            //如果函数支持同步
            // construct sync fn exe ctx
            let ctx = FnExeCtxSync::new(
                match FnExeCtxAsyncAllowedType::try_from(apptype) {
                    // 这里修正为 FnExeCtxAsyncAllowedType
                    Ok(v) => v,
                    Err(err) => {
                        let warn = format!("app type {:?} not supported, err: {}", apptype, err);
                        tracing::warn!("{}", warn);
                        if let Err(err) = resp
                            .send_resp(DistributeTaskResp {
                                success: false,
                                err_msg: warn,
                            })
                            .await
                        {
                            tracing::error!("send distribute task resp failed with err: {}", err);
                        }
                        return;
                    }
                },
                req.app,
                req.func,
                fnmeta.clone(),
                req.task_id.unwrap(), // as TaskId,
                match req.trigger.unwrap() {
                    distribute_task_req::Trigger::EventNew(new) => EventCtx::KvSet {
                        key: new.key,
                        opeid: Some(new.opeid),
                        src_task_id: req.trigger_src_task_id.unwrap(),
                    },
                    distribute_task_req::Trigger::EventWrite(write) => EventCtx::KvSet {
                        key: write.key,
                        opeid: Some(write.opeid),
                        src_task_id: req.trigger_src_task_id.unwrap(),
                    },
                },
            );

            if let Err(err) = resp
                .send_resp(DistributeTaskResp {
                    success: true,
                    err_msg: "".to_owned(),
                })
                .await
            {
                tracing::error!("send sche resp for app:{app} fn:{func} failed with err: {err}");
            }
            let taskid = ctx.inner.task_id.clone();
            let res = self.execute_sync(ctx);
            self.handle_exec_result(&taskid, res);
            // let res_str = match res {
            //     Ok(Some(res)) => res,
            //     Ok(None) => "".to_owned(),
            //     Err(err) => format!("err:{:?}", err),
            // };
            // self.notify_subwait_done(&taskid, res_str);
        } else {
            //如果函数支持异步
            // construct async fn exe ctx
            let ctx = FnExeCtxAsync::new(
                match FnExeCtxAsyncAllowedType::try_from(apptype) {
                    Ok(v) => v,
                    Err(err) => {
                        let warn = format!("app type {:?} not supported, err: {}", apptype, err);
                        tracing::warn!("{}", warn);
                        if let Err(err) = resp
                            .send_resp(DistributeTaskResp {
                                success: false,
                                err_msg: warn,
                            })
                            .await
                        {
                            tracing::error!("send distribute task resp failed with err: {}", err);
                        }
                        return;
                    }
                },
                req.app,
                req.func,
                fnmeta.clone(),
                req.task_id.unwrap(),
                match req.trigger.unwrap() {
                    distribute_task_req::Trigger::EventNew(new) => EventCtx::KvSet {
                        key: new.key,
                        opeid: Some(new.opeid),
                        src_task_id: req.trigger_src_task_id.unwrap(),
                    },
                    distribute_task_req::Trigger::EventWrite(write) => EventCtx::KvSet {
                        key: write.key,
                        opeid: Some(write.opeid),
                        src_task_id: req.trigger_src_task_id.unwrap(),
                    },
                },
            );

            if let Err(err) = resp
                .send_resp(DistributeTaskResp {
                    success: true,
                    err_msg: "".to_owned(),
                })
                .await
            {
                tracing::error!("send sche resp for app:{app} fn:{func} failed with err: {err}");
            }

            let taskid = ctx.task_id().clone();
            let res = self.execute(ctx).await;

            self.handle_exec_result(&taskid, res);

            // notify src task
            // self.notify_subwait_done(&taskid);
            // self.take_subwaitings_for_task(&ctx.task_id())
        }
    }

    /// before call this, verify app and func exist
    pub async fn handle_http_task(
        &self,
        appname: &str,
        funcname: &str,
        text: String,
    ) -> WSResult<Option<String>> {
        // let req_id: ReqId = self
        //     .next_req_id
        //     .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // check app exist
        tracing::debug!("calling get_app_meta to check app exist, app: {}", appname);
        let Some((appmeta, datameta_opt)) =
            self.view.appmeta_manager().get_app_meta(appname).await?
        else {
            tracing::warn!("app {} not found", appname);
            return Err(WsFuncError::AppNotFound {
                app: appname.to_owned(),
            }
            .into());
        };
        // check func exist
        let Some(func) = appmeta.get_fn_meta(funcname) else {
            tracing::warn!("func {} not found, exist:{:?}", funcname, appmeta.fns());
            return Err(WsFuncError::FuncNotFound {
                app: appname.to_owned(),
                func: funcname.to_owned(),
            }
            .into());
        };

        // get app file and extract to execute dir
        if let Some(datameta) = datameta_opt {
            self.view
                .appmeta_manager()
                .load_app_file(appname, datameta)
                .await
                .map_err(|e| {
                    tracing::error!("load app file failed with err: {}", e);
                    e
                })?;
        }

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
        // prepare ctx and run //////////////////////////
        let task_id = self.register_sub_task();

        let res = if func.sync_async.asyncable() {
            let ctx = FnExeCtxAsync::new(
                FnExeCtxAsyncAllowedType::try_from(appmeta.app_type.clone()).unwrap(),
                appname.to_owned(),
                funcname.to_owned(),
                func.clone(),
                task_id.clone(),
                EventCtx::Http(text),
            );
            self.execute(ctx).await
        } else {
            let ctx = FnExeCtxSync::new(
                FnExeCtxAsyncAllowedType::try_from(appmeta.app_type.clone()).unwrap(),
                appname.to_owned(),
                funcname.to_owned(),
                func.clone(),
                task_id.clone(),
                EventCtx::Http(text),
            );

            self.execute_sync(ctx)
        };

        // wait for sub tasks done
        let _ = self.wait_for_subtasks(&task_id.task_id).await;

        res
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

    fn execute_sync(&self, mut ctx: FnExeCtxSync) -> WSResult<Option<String>> {
        let instance = self
            .view
            .instance_manager()
            .load_instance_sync(&ctx.inner.app_type, &ctx.inner.app)?;

        let _ = self
            .view
            .instance_manager()
            .instance_running_function
            .insert(
                instance.instance_name().to_owned(),
                UnsafeFunctionCtx::Sync(
                    NonNull::new(&ctx as *const FnExeCtxSync as *mut FnExeCtxSync).unwrap(),
                ),
            );

        tracing::debug!(
            "start run sync instance {} app {} fn {}",
            instance.instance_name(),
            ctx.inner.app,
            ctx.inner.func
        );

        let bf_exec_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        tracing::debug!("start execute sync");
        let res = instance.execute_sync(self.view.instance_manager(), &mut ctx)?;

        let res = res.map(|v| {
            let mut res: serde_json::Value = serde_json::from_str(&*v).unwrap();
            let _ = res.as_object_mut().unwrap().insert(
                "bf_exec_time".to_owned(),
                serde_json::Value::from(bf_exec_time),
            );
            serde_json::to_string(&res).unwrap()
        });

        let _ = self
            .view
            .instance_manager()
            .instance_running_function
            .remove(&instance.instance_name());

        tracing::debug!(
            "finish run sync instance {} fn {}, res:{:?}",
            instance.instance_name(),
            ctx.inner.func,
            res
        );

        self.view
            .instance_manager()
            .finish_using(&ctx.inner.app, instance);

        Ok(res)
    }

    /// prepare app and func before call execute
    async fn execute(&self, mut fn_ctx: FnExeCtxAsync) -> WSResult<Option<String>> {
        let instance = self
            .view
            .instance_manager()
            .load_instance(&fn_ctx.inner.app_type, &fn_ctx.inner.app)
            .await;

        let _ = self
            .view
            .instance_manager()
            .instance_running_function
            .insert(
                instance.instance_name().to_owned(),
                UnsafeFunctionCtx::Async(
                    NonNull::new(&fn_ctx as *const FnExeCtxAsync as *mut FnExeCtxAsync).unwrap(),
                ),
            );

        tracing::debug!(
            "start run instance {} app {} fn {}",
            instance.instance_name(),
            fn_ctx.inner.app,
            fn_ctx.inner.func
        );

        let bf_exec_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        tracing::debug!("start execute");
        let res = instance
            .execute(self.view.instance_manager(), &mut fn_ctx)
            .await;

        let res = res.map(|v| {
            v.map(|v| {
                let mut res: serde_json::Value = serde_json::from_str(&*v).unwrap();
                let _ = res.as_object_mut().unwrap().insert(
                    "bf_exec_time".to_owned(),
                    serde_json::Value::from(bf_exec_time),
                );
                serde_json::to_string(&res).unwrap()
            })
        });

        let _ = self
            .view
            .instance_manager()
            .instance_running_function
            .remove(&instance.instance_name());

        tracing::debug!(
            "finish run instance {} fn {}, res:{:?}",
            instance.instance_name(),
            fn_ctx.inner.func,
            res
        );

        // while let Some(t) = fn_ctx.inner.sub_waiters.pop() {
        //     let _ = t.await.unwrap();
        // }

        self.view
            .instance_manager()
            .finish_using(&fn_ctx.inner.app, instance);

        res
    }
}
