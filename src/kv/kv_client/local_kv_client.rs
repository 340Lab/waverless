use std::vec;

use async_trait::async_trait;
use ws_derive::LogicalModule;

use crate::{
    kv::dist_kv::SetOptions,
    network::proto::kv::{kv_response::KvPairOpt, KeyRange, KvPair},
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs},
    util::JoinHandleWrapper,
};

use super::KVClient;

#[derive(LogicalModule)]
pub struct LocalKVClient {}

#[async_trait]
impl KVClient for LocalKVClient {
    async fn get(&self, _key_range: KeyRange) -> WSResult<Vec<KvPair>> {
        Ok(vec![])
    }
    async fn set(&self, _kvs: Vec<KvPair>, _opts: SetOptions) -> WSResult<Vec<KvPairOpt>> {
        Ok(vec![])
    }
}

#[async_trait]
impl LogicalModule for LocalKVClient {
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
