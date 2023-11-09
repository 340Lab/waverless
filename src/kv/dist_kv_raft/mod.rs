// mod openraft_adapter;
// pub mod tikvraft_proxy;
mod async_raft_proxy;
// use self::tikvraft_proxy::TiKVRaftModule;

use super::{
    dist_kv::{DistKV, SetOptions},
    KeyRange,
};
use crate::{
    module_iter::*,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, Sys},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;



pub type RaftModule = async_raft_proxy::AsyncRaftModule;

#[derive(LogicalModuleParent, LogicalModule)]
pub struct RaftDistKV {
    #[sub]
    pub raft_module: RaftModule,
    pub name: String,
}

#[async_trait]
impl LogicalModule for RaftDistKV {
    fn inner_new(mut args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        args.expand_parent_name(Self::self_name());
        Self {
            raft_module: RaftModule::new(args.clone()),
            // raft_module: TiKVRaftModule::new(args.clone()),
            name: args.parent_name.clone(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let mut all = vec![];
        all.append(&mut self.raft_module.start().await?);
        Ok(all)
    }
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl DistKV for RaftDistKV {
    async fn get<'a>(&'a self, _sys: &Sys, _key_range: KeyRange<'a>) -> WSResult<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn set(
        &self,
        _sys: &Sys,
        _kvs: Vec<(Vec<u8>, Vec<u8>)>,
        _opts: SetOptions,
    ) -> WSResult<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
        Ok(None)
    }
}
