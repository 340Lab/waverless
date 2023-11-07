// mod openraft_adapter;
pub mod tikvraft_proxy;

use self::tikvraft_proxy::TiKVRaftModule;

use super::{
    dist_kv::{DistKV, SetOptions},
    KeyRange,
};
use crate::{
    module_iter::*,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModules, Sys},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(LogicalModuleParent, LogicalModule)]
pub struct RaftDistKV {
    #[sub]
    pub raft_module: TiKVRaftModule,
    pub name: String,
}

impl LogicalModule for RaftDistKV {
    fn inner_new(mut args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        args.expand_parent_name(Self::self_name());
        Self {
            raft_module: TiKVRaftModule::new(args.clone()),
            name: args.parent_name.clone(),
        }
    }
    fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let mut all = vec![];
        all.append(&mut self.raft_module.start()?);
        Ok(all)
    }
    fn name(&self) -> &str {
        &self.name
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
