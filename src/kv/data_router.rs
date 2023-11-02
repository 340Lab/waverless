// Manage the key routing node information

use std::sync::Arc;

use crate::{
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModules},
};

use tokio::task::JoinHandle;

use super::dist_kv_raft::RaftDistKV;

pub struct DataRouter {
    pub raft_kv: RaftDistKV,
}

impl DataRouter {}

impl LogicalModule for DataRouter {
    fn new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            raft_kv: RaftDistKV::new(args),
        }
    }
    fn start(&self) -> WSResult<Vec<JoinHandle<()>>> {
        // 核心任务，
        //  1. keyrange路径查询，分配
        //  2. 与其他data router通信，维护路由表
        let mut tasks = vec![];
        tasks.append(&mut self.raft_kv.start()?);

        let main_task = tokio::spawn(async move {});
        tasks.push(main_task);
        Ok(tasks)
    }
}
