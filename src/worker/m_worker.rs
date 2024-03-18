use crate::util::JoinHandleWrapper;
use async_trait::async_trait;
use ws_derive::LogicalModule;

use crate::{
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs},
};

#[derive(LogicalModule)]
pub struct WorkerCore {}

#[async_trait]
impl LogicalModule for WorkerCore {
    fn inner_new(_args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {}
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let all = vec![];

        Ok(all)
    }
}
