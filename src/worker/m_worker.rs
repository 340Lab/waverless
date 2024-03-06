use crate::{
    general::network::m_p2p::P2PModule, logical_module_view_impl, sys::LogicalModulesRef,
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use ws_derive::LogicalModule;

use crate::{
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs},
};

logical_module_view_impl!(WorkerView);
logical_module_view_impl!(WorkerView, p2p, P2PModule);

#[derive(LogicalModule)]
pub struct WorkerCore {
    pub view: WorkerView,
}

#[async_trait]
impl LogicalModule for WorkerCore {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: WorkerView::new(args.logical_modules_ref.clone()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let all = vec![];

        Ok(all)
    }
}
