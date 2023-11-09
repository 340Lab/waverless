// Manage the key routing node information

use std::sync::Arc;

use crate::{
    module_iter::*,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModules},
    util::JoinHandleWrapper,
};

use async_trait::async_trait;
use tokio::task::JoinHandle;

use super::dist_kv_raft::RaftDistKV;

#[derive(LogicalModuleParent, LogicalModule)]
pub struct DataRouter {
    #[parent]
    pub raft_kv: RaftDistKV,
    name: String,
}

#[async_trait]
impl LogicalModule for DataRouter {
    fn inner_new(mut args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        args.expand_parent_name(Self::self_name());
        Self {
            name: args.parent_name.clone(),
            raft_kv: RaftDistKV::new(args),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        // 核心任务，
        //  1. keyrange路径查询，分配
        //  2. 与其他data router通信，维护路由表
        let mut tasks = vec![];
        tasks.append(&mut self.raft_kv.start().await?);

        let main_task = tokio::spawn(async move {});
        tasks.push(main_task.into());
        Ok(tasks)
    }
    fn name(&self) -> &str {
        &self.name
    }
}
