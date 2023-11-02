// mod openraft_adapter;
pub mod tikvraft_proxy;

use self::tikvraft_proxy::RaftModule;

use super::{
    dist_kv::{DistKV, SetOptions},
    KeyRange,
};
use crate::{
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModules, Sys},
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub struct RaftDistKV {
    pub raft_module: tikvraft_proxy::RaftModule,
}

impl LogicalModule for RaftDistKV {
    fn new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            raft_module: RaftModule::new(args),
        }
    }
    fn start(&self) -> WSResult<Vec<JoinHandle<()>>> {
        let mut all = vec![];
        all.append(&mut self.raft_module.start()?);
        Ok(all)
    }
}

#[async_trait]
impl DistKV for RaftDistKV {
    async fn get<'a>(&'a self, sys: &Sys, key_range: KeyRange<'a>) -> WSResult<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn set(
        &self,
        sys: &Sys,
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
        opts: SetOptions,
    ) -> WSResult<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
        Ok(None)
    }
}
