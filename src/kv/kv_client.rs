

use async_trait::async_trait;


use crate::{
    module_iter::*,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, NodeID, Sys},
    util::JoinHandleWrapper,
};

use super::{
    dist_kv::{DistKV, SetOptions},
    KeyRange,
};

#[derive(LogicalModuleParent, LogicalModule)]
pub struct KVClient {
    name: String,
}

impl KVClient {
    pub fn get(
        &self,
        _sys: &Sys,
        _node_id: NodeID,
        _key_range: KeyRange,
    ) -> WSResult<Option<Vec<u8>>> {
        Ok(None)
    }
    pub fn set(
        &self,
        _sys: &Sys,
        _node_id: NodeID,
        _kvs: &[(&[u8], &[u8])],
        _opts: SetOptions,
    ) -> WSResult<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
        Ok(None)
    }
}

#[async_trait]
impl LogicalModule for KVClient {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        KVClient {
            name: format!("{}::{}", args.parent_name, Self::self_name()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        Ok(vec![])
    }
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl DistKV for KVClient {
    async fn get<'a>(&'a self, _sys: &Sys, _key_range: KeyRange<'a>) -> WSResult<Option<Vec<u8>>> {
        // 1. get the node id of the key by DataRouterClientNode
        // let routemap: BTreeMap<KeyRange, NodeID> = sys
        //     .logical_nodes
        //     .data_router_client
        //     .get_route_of_key_range(key_range)?;
        // // 2. get data from the nodes
        // for (r, n) in routemap {}

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
