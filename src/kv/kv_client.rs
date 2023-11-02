use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::{
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModules, NodeID, Sys},
};

use super::{
    dist_kv::{DistKV, SetOptions},
    KeyRange,
};

pub struct KVClient {}

impl KVClient {
    pub fn get(
        &self,
        sys: &Sys,
        node_id: NodeID,
        key_range: KeyRange,
    ) -> WSResult<Option<Vec<u8>>> {
        Ok(None)
    }
    pub fn set(
        &self,
        sys: &Sys,
        node_id: NodeID,
        kvs: &[(&[u8], &[u8])],
        opts: SetOptions,
    ) -> WSResult<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
        Ok(None)
    }
}

impl LogicalModule for KVClient {
    fn new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        KVClient {}
    }
    fn start(&self) -> WSResult<Vec<JoinHandle<()>>> {
        Ok(vec![])
    }
}

#[async_trait]
impl DistKV for KVClient {
    async fn get<'a>(&'a self, sys: &Sys, key_range: KeyRange<'a>) -> WSResult<Option<Vec<u8>>> {
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
        sys: &Sys,
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
        opts: SetOptions,
    ) -> WSResult<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
        Ok(None)
    }
}
