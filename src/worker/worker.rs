use crate::{sys::WorkerView, util::JoinHandleWrapper};
use async_trait::async_trait;
use ws_derive::LogicalModule;

use crate::{
    general::network::{p2p::RPCCaller, proto},
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs},
};

#[derive(LogicalModule)]
pub struct WorkerCore {
    pub rpc_caller_make_sche: RPCCaller<proto::sche::MakeSchePlanReq>,
    pub rpc_caller_distribute_task: RPCCaller<proto::sche::DistributeTaskReq>,
    pub view: WorkerView,
}

#[async_trait]
impl LogicalModule for WorkerCore {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            rpc_caller_make_sche: RPCCaller::default(),
            rpc_caller_distribute_task: RPCCaller::default(),
            view: WorkerView::new(args.logical_modules_ref.clone()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let all = vec![];
        self.rpc_caller_distribute_task.regist(self.view.p2p());
        self.rpc_caller_make_sche.regist(self.view.p2p());
        Ok(all)
    }
}
